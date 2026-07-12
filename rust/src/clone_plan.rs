//! Exact clone-plan policy for files, head, and full requests.
//!
//! Inputs are verifier-produced receipts, never caller-labelled hashes. Target
//! resolution, artifact verification, and bundle generation happen before this
//! pure planner; it only chooses among receipts whose identity it rechecks.

use crate::artifact_scheduler::{ArtifactKind, ArtifactRecord, ArtifactState};
use crate::topup::{
    PinnedBundleRequest, TopUpMode, VerifiedPinnedBundle, pinned_bundle_semantic_digest,
};

fn new_transport_session() -> String {
    hex::encode(rand::random::<[u8; 32]>())
}
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    Files,
    Head,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryAvailability {
    Initializing,
    Active,
    Failed,
}

/// A scheduler publication whose ready state and manifest were checked at the
/// verifier boundary. Fields stay private so API callers cannot relabel a hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArtifactReceipt {
    workspace: String,
    repo: String,
    commit: String,
    kind: ArtifactKind,
    format_version: u32,
    manifest: String,
}

impl VerifiedArtifactReceipt {
    #[allow(dead_code)] // production scheduler wiring lands in the cutover wave
    pub(crate) fn from_published(record: &ArtifactRecord) -> Result<Self> {
        if record.state != ArtifactState::Ready
            || record.owner.is_some()
            || record.lease_expires_at.is_some()
            || record.error.is_some()
            || record.failure_class.is_some()
        {
            bail!("artifact receipt source is not a clean ready publication")
        }
        let manifest = record
            .manifest
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("ready artifact has no manifest"))?;
        validate_manifest(manifest, "artifact")?;
        validate_oid(&record.key.commit, "artifact commit")?;
        if record.key.workspace.is_empty() || record.key.repo.is_empty() {
            bail!("artifact receipt identity is empty")
        }
        Ok(Self {
            workspace: record.key.workspace.clone(),
            repo: record.key.repo.clone(),
            commit: record.key.commit.clone(),
            kind: record.key.kind,
            format_version: record.key.format_version,
            manifest: manifest.to_owned(),
        })
    }

    pub fn manifest(&self) -> &str {
        &self.manifest
    }
}

/// Authenticated pinned-bundle semantics. The private constructor accepts only
/// the output of the bundle verifier and rechecks its semantic digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedTopUpReceipt {
    request: PinnedBundleRequest,
    workspace: String,
    repo: String,
    branch: String,
    base_commit: String,
    target_commit: String,
    mode: TopUpMode,
}

impl VerifiedTopUpReceipt {
    pub(crate) fn transport_request(&self) -> &PinnedBundleRequest {
        &self.request
    }
    #[allow(dead_code)] // production bundle-plan wiring lands in the cutover wave
    pub(crate) fn from_verified(bundle: &VerifiedPinnedBundle) -> Result<Self> {
        validate_manifest(&bundle.manifest_hash, "pinned bundle")?;
        if bundle.semantic_digest
            != pinned_bundle_semantic_digest(&bundle.bundle, &bundle.artifacts)
        {
            bail!("pinned bundle semantic receipt is invalid")
        }
        validate_oid(&bundle.bundle.base_commit, "bundle base")?;
        validate_oid(&bundle.bundle.target_commit, "bundle target")?;
        if bundle.bundle.workspace_id.is_empty()
            || bundle.bundle.repo_path.is_empty()
            || bundle.bundle.branch.is_empty()
            || bundle.artifacts.is_empty()
        {
            bail!("pinned bundle semantic identity is incomplete")
        }
        Ok(Self {
            request: PinnedBundleRequest {
                manifest_hash: bundle.manifest_hash.clone(),
                transport_session: new_transport_session(),
                format_version: bundle.bundle.format_version,
                workspace_id: bundle.bundle.workspace_id.clone(),
                repo_path: bundle.bundle.repo_path.clone(),
                base_commit: bundle.bundle.base_commit.clone(),
                target_commit: bundle.bundle.target_commit.clone(),
                branch: bundle.bundle.branch.clone(),
                mode: bundle.bundle.mode,
            },
            workspace: bundle.bundle.workspace_id.clone(),
            repo: bundle.bundle.repo_path.clone(),
            branch: bundle.bundle.branch.clone(),
            base_commit: bundle.bundle.base_commit.clone(),
            target_commit: bundle.bundle.target_commit.clone(),
            mode: bundle.bundle.mode,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExactArtifacts {
    pub head: Option<VerifiedArtifactReceipt>,
    pub history: Option<VerifiedArtifactReceipt>,
    pub files: Option<VerifiedArtifactReceipt>,
}

#[derive(Debug, Clone)]
pub struct ClonePlanningInput<'a> {
    pub availability: RepositoryAvailability,
    pub workspace: &'a str,
    pub repo: &'a str,
    pub branch: &'a str,
    pub artifact_format_version: u32,
    pub mode: SyncMode,
    pub target_commit: &'a str,
    pub exact: &'a ExactArtifacts,
    pub top_up: Option<&'a VerifiedTopUpReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClonePayload {
    FilesArchive {
        manifest: String,
        transport_session: String,
    },
    HeadArtifact {
        manifest: String,
        discard_git: bool,
        transport_session: String,
    },
    FullArtifacts {
        head_manifest: String,
        history_manifest: String,
        transport_session: String,
    },
    PinnedBundle {
        request: PinnedBundleRequest,
        base_commit: String,
        mode: TopUpMode,
        discard_git: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClonePlan {
    Ready {
        target_commit: String,
        payload: ClonePayload,
    },
    Pending {
        target_commit: String,
        required: Vec<ArtifactKind>,
    },
    RepositoryInitializing,
    RepositoryFailed,
}

pub fn plan_clone(input: ClonePlanningInput<'_>) -> Result<ClonePlan> {
    // Lifecycle wins before resolution validation: an unadmitted repo must not
    // leak target/progress details and a failed repo is terminal regardless of
    // whether an upstream target can currently be resolved.
    match input.availability {
        RepositoryAvailability::Initializing => return Ok(ClonePlan::RepositoryInitializing),
        RepositoryAvailability::Failed => return Ok(ClonePlan::RepositoryFailed),
        RepositoryAvailability::Active => {}
    }
    validate_request_identity(&input)?;
    let ready = |payload| ClonePlan::Ready {
        target_commit: input.target_commit.to_owned(),
        payload,
    };

    Ok(match input.mode {
        SyncMode::Files => {
            if let Some(files) = &input.exact.files {
                validate_exact(files, &input, ArtifactKind::Files)?;
                ready(ClonePayload::FilesArchive {
                    manifest: files.manifest.clone(),
                    transport_session: new_transport_session(),
                })
            } else if let Some(head) = &input.exact.head {
                validate_exact(head, &input, ArtifactKind::Head)?;
                ready(ClonePayload::HeadArtifact {
                    manifest: head.manifest.clone(),
                    discard_git: true,
                    transport_session: new_transport_session(),
                })
            } else if let Some(bundle) = input.top_up {
                validate_top_up(bundle, &input, TopUpMode::Head)?;
                ready(ClonePayload::PinnedBundle {
                    request: bundle.request.clone(),
                    base_commit: bundle.base_commit.clone(),
                    mode: TopUpMode::Head,
                    discard_git: true,
                })
            } else {
                ClonePlan::Pending {
                    target_commit: input.target_commit.to_owned(),
                    required: vec![ArtifactKind::Head],
                }
            }
        }
        SyncMode::Head => {
            if let Some(head) = &input.exact.head {
                validate_exact(head, &input, ArtifactKind::Head)?;
                ready(ClonePayload::HeadArtifact {
                    manifest: head.manifest.clone(),
                    discard_git: false,
                    transport_session: new_transport_session(),
                })
            } else if let Some(bundle) = input.top_up {
                validate_top_up(bundle, &input, TopUpMode::Head)?;
                ready(ClonePayload::PinnedBundle {
                    request: bundle.request.clone(),
                    base_commit: bundle.base_commit.clone(),
                    mode: TopUpMode::Head,
                    discard_git: false,
                })
            } else {
                ClonePlan::Pending {
                    target_commit: input.target_commit.to_owned(),
                    required: vec![ArtifactKind::Head],
                }
            }
        }
        SyncMode::Full => {
            let head = input
                .exact
                .head
                .as_ref()
                .map(|receipt| validate_exact(receipt, &input, ArtifactKind::Head).map(|_| receipt))
                .transpose()?;
            let history = input
                .exact
                .history
                .as_ref()
                .map(|receipt| {
                    validate_exact(receipt, &input, ArtifactKind::FullHistory).map(|_| receipt)
                })
                .transpose()?;
            if let (Some(head), Some(history)) = (head, history) {
                ready(ClonePayload::FullArtifacts {
                    head_manifest: head.manifest.clone(),
                    history_manifest: history.manifest.clone(),
                    transport_session: new_transport_session(),
                })
            } else if let Some(bundle) = input.top_up {
                validate_top_up(bundle, &input, TopUpMode::Full)?;
                ready(ClonePayload::PinnedBundle {
                    request: bundle.request.clone(),
                    base_commit: bundle.base_commit.clone(),
                    mode: TopUpMode::Full,
                    discard_git: false,
                })
            } else {
                let mut required = Vec::with_capacity(2);
                if head.is_none() {
                    required.push(ArtifactKind::Head);
                }
                if history.is_none() {
                    required.push(ArtifactKind::FullHistory);
                }
                ClonePlan::Pending {
                    target_commit: input.target_commit.to_owned(),
                    required,
                }
            }
        }
    })
}

/// Admission requires verified exact Head + History receipts for one identity;
/// Files is deliberately irrelevant.
pub fn admission_base_ready(
    workspace: &str,
    repo: &str,
    target_commit: &str,
    format_version: u32,
    exact: &ExactArtifacts,
) -> Result<bool> {
    validate_oid(target_commit, "admission target")?;
    if format_version == 0 {
        bail!("admission artifact format version is zero")
    }
    let Some(head) = &exact.head else {
        return Ok(false);
    };
    let Some(history) = &exact.history else {
        return Ok(false);
    };
    let input = ClonePlanningInput {
        availability: RepositoryAvailability::Active,
        workspace,
        repo,
        branch: "admission",
        artifact_format_version: format_version,
        mode: SyncMode::Full,
        target_commit,
        exact,
        top_up: None,
    };
    validate_exact(head, &input, ArtifactKind::Head)?;
    validate_exact(history, &input, ArtifactKind::FullHistory)?;
    Ok(true)
}

fn validate_request_identity(input: &ClonePlanningInput<'_>) -> Result<()> {
    if input.workspace.is_empty() || input.repo.is_empty() || input.branch.is_empty() {
        bail!("clone-plan identity is empty")
    }
    if input.artifact_format_version == 0 {
        bail!("clone-plan artifact format version is zero")
    }
    validate_oid(input.target_commit, "target")
}

fn validate_exact(
    receipt: &VerifiedArtifactReceipt,
    input: &ClonePlanningInput<'_>,
    expected_kind: ArtifactKind,
) -> Result<()> {
    if receipt.workspace != input.workspace
        || receipt.repo != input.repo
        || receipt.commit != input.target_commit
        || receipt.kind != expected_kind
        || receipt.format_version != input.artifact_format_version
    {
        bail!("exact artifact receipt identity does not match clone request")
    }
    validate_manifest(&receipt.manifest, "exact artifact")
}

fn validate_top_up(
    receipt: &VerifiedTopUpReceipt,
    input: &ClonePlanningInput<'_>,
    expected_mode: TopUpMode,
) -> Result<()> {
    if receipt.workspace != input.workspace
        || receipt.repo != input.repo
        || receipt.branch != input.branch
        || receipt.target_commit != input.target_commit
        || receipt.mode != expected_mode
    {
        bail!("pinned bundle receipt identity does not match clone request")
    }
    validate_manifest(&receipt.request.manifest_hash, "pinned bundle")?;
    validate_oid(&receipt.base_commit, "bundle base")
}

fn validate_oid(value: &str, label: &str) -> Result<()> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        bail!("{label} must be a canonical lowercase SHA-1 commit")
    }
    Ok(())
}

fn validate_manifest(value: &str, label: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        bail!("{label} is not a canonical lowercase SHA-256 manifest hash")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{ArtifactKey, FailureClass};
    use crate::cas::Cas;
    use crate::pack::PackBuilder;
    use crate::pinned_bundle::{
        PinnedBundleBuild, StoredPack, VerifiedBaseArtifact, generate_pinned_bundle,
        verify_pinned_bundle_ready,
    };
    use crate::topup::{PinnedArtifactDescriptor, PinnedArtifactKind, PinnedTopUpBundle};
    use std::process::Command;

    const W: &str = "workspace-test";
    const R: &str = "acme/repo";
    const BRANCH: &str = "main";
    const T: &str = "2222222222222222222222222222222222222222";
    const B: &str = "1111111111111111111111111111111111111111";
    const HEAD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HISTORY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FILES: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn published(kind: ArtifactKind, commit: &str, manifest: &str) -> ArtifactRecord {
        ArtifactRecord {
            id: 1,
            key: ArtifactKey {
                workspace: W.into(),
                repo: R.into(),
                commit: commit.into(),
                kind,
                format_version: 1,
            },
            state: ArtifactState::Ready,
            owner: None,
            lease_expires_at: None,
            lease_generation: 1,
            claim_attempts: 1,
            retry_count: 0,
            manifest: Some(manifest.into()),
            error: None,
            failure_class: None,
        }
    }

    fn receipt(kind: ArtifactKind, commit: &str, manifest: &str) -> VerifiedArtifactReceipt {
        VerifiedArtifactReceipt::from_published(&published(kind, commit, manifest)).unwrap()
    }

    fn bundle(mode: TopUpMode) -> VerifiedTopUpReceipt {
        let semantic = PinnedTopUpBundle {
            format_version: 1,
            workspace_id: W.into(),
            repo_path: R.into(),
            base_commit: B.into(),
            target_commit: T.into(),
            branch: BRANCH.into(),
            mode,
            canonical_origin: "https://github.com/acme/repo.git".into(),
        };
        let artifacts = vec![PinnedArtifactDescriptor {
            kind: PinnedArtifactKind::BasePack,
            hash: HEAD.into(),
            len: 1,
        }];
        let verified = VerifiedPinnedBundle {
            manifest_hash: "d".repeat(64),
            semantic_digest: pinned_bundle_semantic_digest(&semantic, &artifacts),
            bundle: semantic,
            artifacts,
        };
        VerifiedTopUpReceipt::from_verified(&verified).unwrap()
    }

    #[test]
    fn concurrent_plans_for_same_pinned_root_get_distinct_transport_sessions() {
        let first = bundle(TopUpMode::Head);
        let second = bundle(TopUpMode::Head);
        assert_eq!(first.request.manifest_hash, second.request.manifest_hash);
        assert_ne!(
            first.request.transport_session,
            second.request.transport_session
        );
        for session in [
            &first.request.transport_session,
            &second.request.transport_session,
        ] {
            assert_eq!(session.len(), 64);
            assert!(session.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }

    fn plan(
        mode: SyncMode,
        exact: &ExactArtifacts,
        top_up: Option<&VerifiedTopUpReceipt>,
    ) -> Result<ClonePlan> {
        plan_clone(ClonePlanningInput {
            availability: RepositoryAvailability::Active,
            workspace: W,
            repo: R,
            branch: BRANCH,
            artifact_format_version: 1,
            mode,
            target_commit: T,
            exact,
            top_up,
        })
    }

    #[test]
    fn lifecycle_precedes_target_validation_and_never_leaks_partial_artifacts() {
        for availability in [
            RepositoryAvailability::Initializing,
            RepositoryAvailability::Failed,
        ] {
            let result = plan_clone(ClonePlanningInput {
                availability,
                workspace: W,
                repo: R,
                branch: BRANCH,
                artifact_format_version: 1,
                mode: SyncMode::Full,
                target_commit: "unresolved",
                exact: &ExactArtifacts::default(),
                top_up: None,
            })
            .unwrap();
            assert_eq!(
                result,
                if availability == RepositoryAvailability::Initializing {
                    ClonePlan::RepositoryInitializing
                } else {
                    ClonePlan::RepositoryFailed
                }
            );
        }
    }

    #[test]
    fn admission_requires_same_identity_exact_head_and_history_not_files() {
        let exact = ExactArtifacts {
            head: Some(receipt(ArtifactKind::Head, T, HEAD)),
            history: Some(receipt(ArtifactKind::FullHistory, T, HISTORY)),
            files: None,
        };
        assert!(admission_base_ready(W, R, T, 1, &exact).unwrap());
        let wrong_commit = ExactArtifacts {
            history: Some(receipt(ArtifactKind::FullHistory, B, HISTORY)),
            ..exact.clone()
        };
        assert!(admission_base_ready(W, R, T, 1, &wrong_commit).is_err());
        assert!(admission_base_ready(W, R, T, 0, &exact).is_err());
        let missing = ExactArtifacts {
            history: None,
            ..exact
        };
        assert!(!admission_base_ready(W, R, T, 1, &missing).unwrap());
    }

    #[test]
    fn files_precedence_is_archive_then_exact_head_then_verified_head_bundle() {
        let all = ExactArtifacts {
            files: Some(receipt(ArtifactKind::Files, T, FILES)),
            head: Some(receipt(ArtifactKind::Head, T, HEAD)),
            history: None,
        };
        assert!(matches!(
            plan(SyncMode::Files, &all, None).unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::FilesArchive { .. },
                ..
            }
        ));
        let head = ExactArtifacts { files: None, ..all };
        assert!(matches!(
            plan(SyncMode::Files, &head, None).unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::HeadArtifact {
                    discard_git: true,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            plan(
                SyncMode::Files,
                &ExactArtifacts::default(),
                Some(&bundle(TopUpMode::Head))
            )
            .unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::PinnedBundle {
                    discard_git: true,
                    mode: TopUpMode::Head,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn full_requests_only_missing_sibling_and_exact_target_is_never_substituted() {
        let head = ExactArtifacts {
            head: Some(receipt(ArtifactKind::Head, T, HEAD)),
            ..Default::default()
        };
        assert_eq!(
            plan(SyncMode::Full, &head, None).unwrap(),
            ClonePlan::Pending {
                target_commit: T.into(),
                required: vec![ArtifactKind::FullHistory]
            }
        );
        let history = ExactArtifacts {
            history: Some(receipt(ArtifactKind::FullHistory, T, HISTORY)),
            ..Default::default()
        };
        assert_eq!(
            plan(SyncMode::Full, &history, None).unwrap(),
            ClonePlan::Pending {
                target_commit: T.into(),
                required: vec![ArtifactKind::Head]
            }
        );
        let empty = plan(SyncMode::Head, &ExactArtifacts::default(), None).unwrap();
        assert_eq!(
            empty,
            ClonePlan::Pending {
                target_commit: T.into(),
                required: vec![ArtifactKind::Head]
            }
        );
        assert!(
            matches!(plan(SyncMode::Full, &ExactArtifacts::default(), Some(&bundle(TopUpMode::Full))).unwrap(), ClonePlan::Ready { target_commit, .. } if target_commit == T)
        );
    }

    #[test]
    fn mislabeled_commit_repo_kind_format_and_dirty_publication_fail_closed() {
        let wrong_commit = ExactArtifacts {
            head: Some(receipt(ArtifactKind::Head, B, HEAD)),
            ..Default::default()
        };
        assert!(plan(SyncMode::Head, &wrong_commit, None).is_err());

        let mut wrong_repo_record = published(ArtifactKind::Head, T, HEAD);
        wrong_repo_record.key.repo = "other/repo".into();
        let wrong_repo = ExactArtifacts {
            head: Some(VerifiedArtifactReceipt::from_published(&wrong_repo_record).unwrap()),
            ..Default::default()
        };
        assert!(plan(SyncMode::Head, &wrong_repo, None).is_err());

        let wrong_kind = ExactArtifacts {
            head: Some(receipt(ArtifactKind::Files, T, FILES)),
            ..Default::default()
        };
        assert!(plan(SyncMode::Head, &wrong_kind, None).is_err());

        let mut wrong_format_record = published(ArtifactKind::Head, T, HEAD);
        wrong_format_record.key.format_version = 2;
        let wrong_format = ExactArtifacts {
            head: Some(VerifiedArtifactReceipt::from_published(&wrong_format_record).unwrap()),
            ..Default::default()
        };
        assert!(plan(SyncMode::Head, &wrong_format, None).is_err());

        for mutate in ["running", "owner", "lease", "error", "failure"] {
            let mut record = published(ArtifactKind::Head, T, HEAD);
            match mutate {
                "running" => record.state = ArtifactState::Running,
                "owner" => record.owner = Some("worker".into()),
                "lease" => record.lease_expires_at = Some(123),
                "error" => record.error = Some("bad".into()),
                "failure" => record.failure_class = Some(FailureClass::Permanent),
                _ => unreachable!(),
            }
            assert!(VerifiedArtifactReceipt::from_published(&record).is_err());
        }
    }

    #[test]
    fn bundle_semantics_are_authenticated_and_bound_to_request_identity() {
        let valid = bundle(TopUpMode::Head);
        assert!(plan(SyncMode::Head, &ExactArtifacts::default(), Some(&valid)).is_ok());

        let mut wrong_target = valid.clone();
        wrong_target.target_commit = B.into();
        assert!(
            plan(
                SyncMode::Head,
                &ExactArtifacts::default(),
                Some(&wrong_target)
            )
            .is_err()
        );

        let mut wrong_repo = valid.clone();
        wrong_repo.repo = "other/repo".into();
        assert!(
            plan(
                SyncMode::Head,
                &ExactArtifacts::default(),
                Some(&wrong_repo)
            )
            .is_err()
        );

        let semantic = PinnedTopUpBundle {
            format_version: 1,
            workspace_id: W.into(),
            repo_path: R.into(),
            base_commit: B.into(),
            target_commit: T.into(),
            branch: BRANCH.into(),
            mode: TopUpMode::Head,
            canonical_origin: "https://github.com/acme/repo.git".into(),
        };
        let forged = VerifiedPinnedBundle {
            manifest_hash: "d".repeat(64),
            semantic_digest: "e".repeat(64),
            bundle: semantic,
            artifacts: vec![PinnedArtifactDescriptor {
                kind: PinnedArtifactKind::BasePack,
                hash: HEAD.into(),
                len: 1,
            }],
        };
        assert!(VerifiedTopUpReceipt::from_verified(&forged).is_err());
    }

    #[test]
    fn exact_ready_artifact_ignores_irrelevant_stale_bundle() {
        let exact = ExactArtifacts {
            head: Some(receipt(ArtifactKind::Head, T, HEAD)),
            ..Default::default()
        };
        let mut irrelevant = bundle(TopUpMode::Full);
        irrelevant.repo = "stale/repo".into();
        assert!(matches!(
            plan(SyncMode::Head, &exact, Some(&irrelevant)).unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::HeadArtifact { .. },
                ..
            }
        ));
    }

    #[test]
    fn generated_and_cas_verified_bundle_flows_into_exact_target_plan() {
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        crate::git::init(&repo).unwrap();
        for (key, value) in [
            ("user.name", "test"),
            ("user.email", "test@example.invalid"),
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(&repo)
                    .args(["config", key, value])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let commit = |name: &str, body: &str| {
            std::fs::write(repo.join(name), body).unwrap();
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(&repo)
                    .args(["add", "-A"])
                    .status()
                    .unwrap()
                    .success()
            );
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(&repo)
                    .args(["commit", "-m", name])
                    .status()
                    .unwrap()
                    .success()
            );
            let output = Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        let base_commit = commit("base.txt", "base");
        let cas = Cas::new(root.path().join("cas")).unwrap();
        let packs = PackBuilder::new(&repo, &cas)
            .build_depth_packs(&base_commit, Some(1), 1024 * 1024)
            .unwrap()
            .into_iter()
            .map(|(pack, pack_len, index, index_len)| StoredPack {
                pack: PinnedArtifactDescriptor {
                    kind: PinnedArtifactKind::BasePack,
                    hash: pack,
                    len: pack_len,
                },
                index: PinnedArtifactDescriptor {
                    kind: PinnedArtifactKind::BasePackIndex,
                    hash: index,
                    len: index_len,
                },
            })
            .collect();
        let base = VerifiedBaseArtifact {
            commit: base_commit.clone(),
            mode: TopUpMode::Head,
            packs,
        };
        let target = commit("target.txt", "target");
        let request = generate_pinned_bundle(PinnedBundleBuild {
            mirror: &repo,
            cas: &cas,
            workspace_id: W,
            repo_path: R,
            base_commit: &base_commit,
            base_artifact: &base,
            target_commit: &target,
            mode: TopUpMode::Head,
            branch: BRANCH,
            canonical_origin: "https://github.com/acme/repo.git",
        })
        .unwrap();
        let verified = verify_pinned_bundle_ready(&cas, &request).unwrap();
        let receipt = VerifiedTopUpReceipt::from_verified(&verified).unwrap();
        let plan = plan_clone(ClonePlanningInput {
            availability: RepositoryAvailability::Active,
            workspace: W,
            repo: R,
            branch: BRANCH,
            artifact_format_version: 1,
            mode: SyncMode::Head,
            target_commit: &target,
            exact: &ExactArtifacts::default(),
            top_up: Some(&receipt),
        })
        .unwrap();
        let ClonePlan::Ready {
            target_commit,
            payload: ClonePayload::PinnedBundle {
                request: planned, ..
            },
        } = plan
        else {
            panic!("generated verified bundle did not produce a pinned plan")
        };
        assert_eq!(target_commit, target);
        assert_eq!(planned.manifest_hash, request.manifest_hash);
        assert_eq!(planned.workspace_id, request.workspace_id);
        assert_eq!(planned.repo_path, request.repo_path);
        assert_eq!(planned.base_commit, request.base_commit);
        assert_eq!(planned.target_commit, request.target_commit);
        assert_eq!(planned.branch, request.branch);
        assert_eq!(planned.mode, request.mode);
        assert_eq!(planned.transport_session.len(), 64);
        assert!(
            planned
                .transport_session
                .bytes()
                .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) })
        );
    }
}
