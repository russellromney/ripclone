//! Pure clone-plan policy for exact files, head, and full requests.
//!
//! Target resolution, artifact verification, and pinned-bundle generation happen
//! outside this module. The planner accepts only their verified receipts and
//! never substitutes a different target commit.

use crate::artifact_scheduler::ArtifactKind;
use crate::topup::{PinnedBundleRequest, TopUpMode};
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExactArtifacts {
    pub head_manifest: Option<String>,
    pub history_manifest: Option<String>,
    pub files_manifest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibleBase {
    pub commit: String,
    pub mode: TopUpMode,
    pub head_manifest: String,
    pub history_manifest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedTopUp {
    pub request: PinnedBundleRequest,
    pub base_commit: String,
    pub target_commit: String,
    pub mode: TopUpMode,
}

#[derive(Debug, Clone)]
pub struct ClonePlanningInput<'a> {
    pub availability: RepositoryAvailability,
    pub mode: SyncMode,
    pub target_commit: &'a str,
    pub exact: &'a ExactArtifacts,
    pub base: Option<&'a CompatibleBase>,
    pub top_up: Option<&'a VerifiedTopUp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClonePayload {
    FilesArchive {
        manifest: String,
    },
    HeadArtifact {
        manifest: String,
        discard_git: bool,
    },
    FullArtifacts {
        head_manifest: String,
        history_manifest: String,
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
    validate_oid(input.target_commit, "target")?;
    match input.availability {
        RepositoryAvailability::Initializing => return Ok(ClonePlan::RepositoryInitializing),
        RepositoryAvailability::Failed => return Ok(ClonePlan::RepositoryFailed),
        RepositoryAvailability::Active => {}
    }
    let ready = |payload| ClonePlan::Ready {
        target_commit: input.target_commit.to_owned(),
        payload,
    };
    Ok(match input.mode {
        SyncMode::Files => {
            if let Some(manifest) = &input.exact.files_manifest {
                validate_manifest(manifest, "exact files")?;
                ready(ClonePayload::FilesArchive {
                    manifest: manifest.clone(),
                })
            } else if let Some(manifest) = &input.exact.head_manifest {
                validate_manifest(manifest, "exact head")?;
                ready(ClonePayload::HeadArtifact {
                    manifest: manifest.clone(),
                    discard_git: true,
                })
            } else {
                validate_fallback(input.base, input.top_up, input.target_commit)?;
                if let Some((base, top_up)) =
                    compatible_top_up(input.base, input.top_up, TopUpMode::Head)
                {
                    ensure_top_up_base(base, top_up)?;
                    ready(ClonePayload::PinnedBundle {
                        request: top_up.request.clone(),
                        base_commit: base.commit.clone(),
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
        }
        SyncMode::Head => {
            if let Some(manifest) = &input.exact.head_manifest {
                validate_manifest(manifest, "exact head")?;
                ready(ClonePayload::HeadArtifact {
                    manifest: manifest.clone(),
                    discard_git: false,
                })
            } else {
                validate_fallback(input.base, input.top_up, input.target_commit)?;
                if let Some((base, top_up)) =
                    compatible_top_up(input.base, input.top_up, TopUpMode::Head)
                {
                    ensure_top_up_base(base, top_up)?;
                    ready(ClonePayload::PinnedBundle {
                        request: top_up.request.clone(),
                        base_commit: base.commit.clone(),
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
        }
        SyncMode::Full => {
            if let (Some(head), Some(history)) =
                (&input.exact.head_manifest, &input.exact.history_manifest)
            {
                validate_manifest(head, "exact head")?;
                validate_manifest(history, "exact history")?;
                ready(ClonePayload::FullArtifacts {
                    head_manifest: head.clone(),
                    history_manifest: history.clone(),
                })
            } else {
                validate_fallback(input.base, input.top_up, input.target_commit)?;
                if let Some((base, top_up)) =
                    compatible_top_up(input.base, input.top_up, TopUpMode::Full)
                {
                    ensure_top_up_base(base, top_up)?;
                    ready(ClonePayload::PinnedBundle {
                        request: top_up.request.clone(),
                        base_commit: base.commit.clone(),
                        mode: TopUpMode::Full,
                        discard_git: false,
                    })
                } else {
                    ClonePlan::Pending {
                        target_commit: input.target_commit.to_owned(),
                        required: vec![ArtifactKind::Head, ArtifactKind::FullHistory],
                    }
                }
            }
        }
    })
}

/// Repository admission is a separate monotonic decision: files never gate it.
pub fn admission_base_ready(exact: &ExactArtifacts) -> bool {
    exact
        .head_manifest
        .as_deref()
        .is_some_and(|value| validate_manifest(value, "head").is_ok())
        && exact
            .history_manifest
            .as_deref()
            .is_some_and(|value| validate_manifest(value, "history").is_ok())
}

fn compatible_top_up<'a>(
    base: Option<&'a CompatibleBase>,
    top_up: Option<&'a VerifiedTopUp>,
    mode: TopUpMode,
) -> Option<(&'a CompatibleBase, &'a VerifiedTopUp)> {
    match (base, top_up) {
        (Some(base), Some(top_up)) if base.mode == mode && top_up.mode == mode => {
            Some((base, top_up))
        }
        _ => None,
    }
}

fn ensure_top_up_base(base: &CompatibleBase, top_up: &VerifiedTopUp) -> Result<()> {
    if base.commit != top_up.base_commit {
        bail!("pinned bundle does not extend the selected verified base")
    }
    if base.mode == TopUpMode::Full && base.history_manifest.is_none() {
        bail!("full base is missing its verified history artifact")
    }
    Ok(())
}

fn validate_fallback(
    base: Option<&CompatibleBase>,
    top_up: Option<&VerifiedTopUp>,
    target_commit: &str,
) -> Result<()> {
    if let Some(base) = base {
        validate_base(base)?;
    }
    if let Some(top_up) = top_up {
        validate_top_up(top_up, target_commit)?;
    }
    match (base, top_up) {
        (None, Some(_)) => bail!("pinned bundle has no selected verified base"),
        (Some(base), Some(top_up)) if base.mode != top_up.mode => {
            bail!("pinned bundle mode does not match the selected verified base")
        }
        _ => Ok(()),
    }
}

fn validate_base(base: &CompatibleBase) -> Result<()> {
    validate_oid(&base.commit, "base")?;
    validate_manifest(&base.head_manifest, "verified base head")?;
    if let Some(history) = &base.history_manifest {
        validate_manifest(history, "verified base history")?;
    }
    if base.mode == TopUpMode::Full && base.history_manifest.is_none() {
        bail!("full base is missing its history artifact")
    }
    Ok(())
}

fn validate_top_up(top_up: &VerifiedTopUp, target: &str) -> Result<()> {
    validate_oid(&top_up.base_commit, "bundle base")?;
    validate_oid(&top_up.target_commit, "bundle target")?;
    if top_up.target_commit != target {
        bail!("pinned bundle targets a different commit")
    }
    validate_manifest(&top_up.request.manifest_hash, "pinned bundle receipt")?;
    Ok(())
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

    const T: &str = "2222222222222222222222222222222222222222";
    const B: &str = "1111111111111111111111111111111111111111";
    const HEAD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HISTORY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FILES: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn top_up(mode: TopUpMode) -> VerifiedTopUp {
        VerifiedTopUp {
            request: PinnedBundleRequest {
                manifest_hash: "a".repeat(64),
            },
            base_commit: B.into(),
            target_commit: T.into(),
            mode,
        }
    }

    fn base(mode: TopUpMode) -> CompatibleBase {
        CompatibleBase {
            commit: B.into(),
            mode,
            head_manifest: HEAD.into(),
            history_manifest: (mode == TopUpMode::Full).then(|| HISTORY.into()),
        }
    }

    fn plan(
        mode: SyncMode,
        exact: &ExactArtifacts,
        base: Option<&CompatibleBase>,
        top_up: Option<&VerifiedTopUp>,
    ) -> Result<ClonePlan> {
        plan_clone(ClonePlanningInput {
            availability: RepositoryAvailability::Active,
            mode,
            target_commit: T,
            exact,
            base,
            top_up,
        })
    }

    #[test]
    fn initializing_or_failed_repo_never_leaks_partial_artifacts() {
        let exact = ExactArtifacts {
            head_manifest: Some(HEAD.into()),
            history_manifest: Some(HISTORY.into()),
            files_manifest: Some(FILES.into()),
        };
        for availability in [
            RepositoryAvailability::Initializing,
            RepositoryAvailability::Failed,
        ] {
            let result = plan_clone(ClonePlanningInput {
                availability,
                mode: SyncMode::Full,
                target_commit: T,
                exact: &exact,
                base: None,
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
    fn admission_requires_exact_head_and_history_but_never_files() {
        assert!(!admission_base_ready(&ExactArtifacts::default()));
        assert!(!admission_base_ready(&ExactArtifacts {
            head_manifest: Some(HEAD.into()),
            ..Default::default()
        }));
        assert!(admission_base_ready(&ExactArtifacts {
            head_manifest: Some(HEAD.into()),
            history_manifest: Some(HISTORY.into()),
            files_manifest: None,
        }));
    }

    #[test]
    fn files_prefers_archive_then_exact_head_then_older_head_bundle() {
        let all = ExactArtifacts {
            files_manifest: Some(FILES.into()),
            head_manifest: Some(HEAD.into()),
            history_manifest: None,
        };
        assert!(matches!(
            plan(SyncMode::Files, &all, None, None).unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::FilesArchive { .. },
                ..
            }
        ));
        let head = ExactArtifacts {
            head_manifest: Some(HEAD.into()),
            ..Default::default()
        };
        assert!(matches!(
            plan(SyncMode::Files, &head, None, None).unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::HeadArtifact {
                    discard_git: true,
                    ..
                },
                ..
            }
        ));
        let base = base(TopUpMode::Head);
        let bundle = top_up(TopUpMode::Head);
        assert!(matches!(
            plan(
                SyncMode::Files,
                &ExactArtifacts::default(),
                Some(&base),
                Some(&bundle)
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
    fn head_and_full_use_only_mode_compatible_exact_or_bundle_inputs() {
        let head = ExactArtifacts {
            head_manifest: Some(HEAD.into()),
            ..Default::default()
        };
        assert!(matches!(
            plan(SyncMode::Head, &head, None, None).unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::HeadArtifact {
                    discard_git: false,
                    ..
                },
                ..
            }
        ));
        assert_eq!(
            plan(SyncMode::Full, &head, None, None).unwrap(),
            ClonePlan::Pending {
                target_commit: T.into(),
                required: vec![ArtifactKind::Head, ArtifactKind::FullHistory],
            }
        );
        let full = ExactArtifacts {
            head_manifest: Some(HEAD.into()),
            history_manifest: Some(HISTORY.into()),
            files_manifest: None,
        };
        assert!(matches!(
            plan(SyncMode::Full, &full, None, None).unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::FullArtifacts { .. },
                ..
            }
        ));
    }

    #[test]
    fn mismatched_target_base_mode_receipt_and_empty_manifests_fail_closed() {
        let exact = ExactArtifacts::default();
        let head_base = base(TopUpMode::Head);
        let mut bundle = top_up(TopUpMode::Head);
        bundle.target_commit = "3".repeat(40);
        assert!(plan(SyncMode::Head, &exact, Some(&head_base), Some(&bundle)).is_err());

        let mut bundle = top_up(TopUpMode::Full);
        bundle.base_commit = "4".repeat(40);
        let full_base = base(TopUpMode::Full);
        assert!(plan(SyncMode::Full, &exact, Some(&full_base), Some(&bundle)).is_err());

        let head_bundle = top_up(TopUpMode::Head);
        assert!(plan(SyncMode::Full, &exact, Some(&full_base), Some(&head_bundle)).is_err());
        assert!(
            plan(
                SyncMode::Head,
                &ExactArtifacts {
                    head_manifest: Some(String::new()),
                    ..Default::default()
                },
                None,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn target_is_never_substituted_even_when_only_an_old_base_exists() {
        let base = base(TopUpMode::Head);
        let no_bundle = plan(
            SyncMode::Head,
            &ExactArtifacts::default(),
            Some(&base),
            None,
        )
        .unwrap();
        assert_eq!(
            no_bundle,
            ClonePlan::Pending {
                target_commit: T.into(),
                required: vec![ArtifactKind::Head],
            }
        );
        let bundle = top_up(TopUpMode::Head);
        let ready = plan(
            SyncMode::Head,
            &ExactArtifacts::default(),
            Some(&base),
            Some(&bundle),
        )
        .unwrap();
        assert!(matches!(ready, ClonePlan::Ready { target_commit, .. } if target_commit == T));
    }

    #[test]
    fn exact_ready_artifact_does_not_depend_on_stale_fallback_metadata() {
        let exact = ExactArtifacts {
            head_manifest: Some(HEAD.into()),
            ..Default::default()
        };
        let invalid_base = CompatibleBase {
            commit: "not-an-oid".into(),
            mode: TopUpMode::Full,
            head_manifest: String::new(),
            history_manifest: None,
        };
        let invalid_bundle = VerifiedTopUp {
            request: PinnedBundleRequest {
                manifest_hash: "bad".into(),
            },
            base_commit: "bad".into(),
            target_commit: "bad".into(),
            mode: TopUpMode::Full,
        };
        assert!(matches!(
            plan(
                SyncMode::Head,
                &exact,
                Some(&invalid_base),
                Some(&invalid_bundle)
            )
            .unwrap(),
            ClonePlan::Ready {
                payload: ClonePayload::HeadArtifact { .. },
                ..
            }
        ));
    }

    #[test]
    fn exhaustive_exact_readiness_matrix_preserves_target_and_mode_contracts() {
        for mode in [SyncMode::Files, SyncMode::Head, SyncMode::Full] {
            for head in [false, true] {
                for history in [false, true] {
                    for files in [false, true] {
                        let exact = ExactArtifacts {
                            head_manifest: head.then(|| HEAD.into()),
                            history_manifest: history.then(|| HISTORY.into()),
                            files_manifest: files.then(|| FILES.into()),
                        };
                        let result = plan(mode, &exact, None, None).unwrap();
                        match result {
                            ClonePlan::Ready {
                                target_commit,
                                payload,
                            } => {
                                assert_eq!(target_commit, T);
                                match mode {
                                    SyncMode::Files => {
                                        assert!(files || head);
                                        assert!(matches!(
                                            payload,
                                            ClonePayload::FilesArchive { .. }
                                                | ClonePayload::HeadArtifact {
                                                    discard_git: true,
                                                    ..
                                                }
                                        ));
                                    }
                                    SyncMode::Head => {
                                        assert!(head);
                                        assert!(matches!(
                                            payload,
                                            ClonePayload::HeadArtifact {
                                                discard_git: false,
                                                ..
                                            }
                                        ));
                                    }
                                    SyncMode::Full => {
                                        assert!(head && history);
                                        assert!(matches!(
                                            payload,
                                            ClonePayload::FullArtifacts { .. }
                                        ));
                                    }
                                }
                            }
                            ClonePlan::Pending {
                                target_commit,
                                required,
                            } => {
                                assert_eq!(target_commit, T);
                                match mode {
                                    SyncMode::Files | SyncMode::Head => {
                                        assert!(!head && (mode == SyncMode::Head || !files));
                                        assert_eq!(required, vec![ArtifactKind::Head]);
                                    }
                                    SyncMode::Full => {
                                        assert!(!(head && history));
                                        assert_eq!(
                                            required,
                                            vec![ArtifactKind::Head, ArtifactKind::FullHistory]
                                        );
                                    }
                                }
                            }
                            other => panic!("active exact matrix returned {other:?}"),
                        }
                    }
                }
            }
        }
    }
}
