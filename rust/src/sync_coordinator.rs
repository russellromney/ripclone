//! Policy boundary for normalized branch synchronization.
//!
//! Every trigger is only a wake-up. The coordinator resolves the provider's
//! current branch tip, durably acquires an immutable source snapshot for that
//! exact commit, and only then makes independently-buildable artifact jobs
//! visible. Poll/webhook traffic never repairs an unchanged commit; explicit
//! install/API/button requests may idempotently ensure it.

use crate::artifact_scheduler::{ArtifactKind, FailureClass, ObservationSnapshot};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;

const MAX_OBSERVATION_RACES: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncIntent {
    /// Polls and webhooks detect movement only. They must not turn a slow or
    /// failed same-commit build into an unbounded repair loop.
    ObserveMovement,
    /// User-directed operations ensure the exact current tip, including an
    /// idempotent repair attempt when the branch itself did not move.
    EnsureCurrent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncModes {
    pub head: bool,
    pub full: bool,
    pub files: bool,
}

impl Default for SyncModes {
    fn default() -> Self {
        Self {
            head: true,
            full: true,
            files: true,
        }
    }
}

impl SyncModes {
    fn kinds(self) -> Result<Vec<ArtifactKind>> {
        let mut kinds = Vec::with_capacity(3);
        if self.head {
            kinds.push(ArtifactKind::Head);
        }
        if self.full {
            kinds.push(ArtifactKind::FullHistory);
        }
        if self.files {
            kinds.push(ArtifactKind::Files);
        }
        if kinds.is_empty() {
            bail!("normalized sync must request at least one artifact mode")
        }
        Ok(kinds)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchSyncRequest {
    pub workspace: String,
    pub repo: String,
    pub branch: String,
    pub intent: SyncIntent,
    pub modes: SyncModes,
    pub format_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableSourceSnapshot {
    pub workspace: String,
    pub repo: String,
    pub commit: String,
    /// CAS identity of the authenticated immutable source root.
    pub manifest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchSyncOutcome {
    Unchanged {
        commit: String,
        generation: u64,
    },
    Ensured {
        commit: String,
        generation: u64,
        artifacts: Vec<(ArtifactKind, ArtifactIntentOutcome)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactIntentOutcome {
    Runnable(i64),
    Subscribed(i64),
    Ready(i64),
    /// Durable desired work that is waiting for runnable-lane capacity.
    Deferred(i64),
    Failed(i64, FailureClass),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactObservationOutcome {
    /// Branch target, exact source root, and every requested mode are durable
    /// in one transaction. `advanced` is false for an idempotent same-tip
    /// ensure or when another observer committed the same target first.
    Recorded {
        generation: u64,
        advanced: bool,
        artifacts: Vec<(ArtifactKind, ArtifactIntentOutcome)>,
    },
    Stale {
        current_generation: u64,
    },
}

#[async_trait]
pub trait BranchTipResolver: Send + Sync {
    /// Resolve the provider's current branch tip. Webhook payload SHAs are not
    /// accepted here: an old/replayed delivery must never regress a branch.
    async fn resolve_current_tip(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<String>;
}

#[async_trait]
pub trait DurableSourceAcquirer: Send + Sync {
    /// Idempotently publish an immutable, fleet-readable snapshot before any
    /// artifact consumer for `commit` can be scheduled.
    async fn acquire_exact(
        &self,
        workspace: &str,
        repo: &str,
        commit: &str,
    ) -> Result<DurableSourceSnapshot>;
}

#[async_trait]
pub trait ArtifactObservation: Send + Sync {
    async fn snapshot(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<ObservationSnapshot>;

    /// Atomically record the exact branch target, source-root retention, and a
    /// durable intent for every requested mode under the snapshot CAS. Lane
    /// pressure may make an intent Deferred, but may not roll back the target,
    /// source root, or another mode. Runnable admission/wakeup occurs after this
    /// transaction and is never the first durable memory of desired work.
    async fn record_tip_and_intents(
        &self,
        snapshot: &ObservationSnapshot,
        source: &DurableSourceSnapshot,
        kinds: &[ArtifactKind],
        format_version: u32,
    ) -> Result<ArtifactObservationOutcome>;
}

pub struct NormalizedSyncCoordinator<R, S, O> {
    resolver: R,
    sources: S,
    observations: O,
}

impl<R, S, O> NormalizedSyncCoordinator<R, S, O>
where
    R: BranchTipResolver,
    S: DurableSourceAcquirer,
    O: ArtifactObservation,
{
    pub fn new(resolver: R, sources: S, observations: O) -> Self {
        Self {
            resolver,
            sources,
            observations,
        }
    }

    pub async fn sync_branch(&self, request: &BranchSyncRequest) -> Result<BranchSyncOutcome> {
        validate_request(request)?;
        let kinds = request.modes.kinds()?;

        for _ in 0..MAX_OBSERVATION_RACES {
            let before = self
                .observations
                .snapshot(&request.workspace, &request.repo, &request.branch)
                .await
                .context("snapshot normalized branch observation")?;
            let commit = self
                .resolver
                .resolve_current_tip(&request.workspace, &request.repo, &request.branch)
                .await
                .context("resolve current upstream branch tip")?;
            crate::artifact_scheduler::validate_canonical_commit_oid(&commit)
                .context("provider returned a non-canonical commit")?;

            if before.commit() == Some(commit.as_str())
                && request.intent == SyncIntent::ObserveMovement
            {
                let generation = before.generation().unwrap_or(0);
                return Ok(BranchSyncOutcome::Unchanged { commit, generation });
            }

            let source = self
                .sources
                .acquire_exact(&request.workspace, &request.repo, &commit)
                .await
                .context("publish durable source snapshot")?;
            validate_source_identity(request, &commit, &source)?;
            match self
                .observations
                .record_tip_and_intents(&before, &source, &kinds, request.format_version)
                .await
                .context("atomically publish branch target, source root, and artifact intents")?
            {
                ArtifactObservationOutcome::Recorded {
                    generation,
                    artifacts,
                    ..
                } => {
                    return Ok(BranchSyncOutcome::Ensured {
                        commit,
                        generation,
                        artifacts,
                    });
                }
                ArtifactObservationOutcome::Stale { .. } => {
                    // Re-resolve the provider. Reusing `commit` here could let a
                    // delayed webhook or force-push loser regress the branch.
                }
            }
        }
        bail!("branch tip kept changing during normalized sync")
    }
}

fn validate_request(request: &BranchSyncRequest) -> Result<()> {
    if request.workspace.trim().is_empty()
        || request.repo.trim().is_empty()
        || request.branch.trim().is_empty()
    {
        bail!("workspace, repo, and branch must be non-empty")
    }
    if request.format_version == 0 {
        bail!("artifact format version must be non-zero")
    }
    Ok(())
}

fn validate_source_identity(
    request: &BranchSyncRequest,
    commit: &str,
    source: &DurableSourceSnapshot,
) -> Result<()> {
    if source.workspace != request.workspace
        || source.repo != request.repo
        || source.commit != commit
        || source.manifest.trim().is_empty()
    {
        bail!("durable source snapshot identity does not match resolved target")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    const C1: &str = "1111111111111111111111111111111111111111";
    const C2: &str = "2222222222222222222222222222222222222222";
    const C3: &str = "3333333333333333333333333333333333333333";

    #[derive(Clone)]
    struct Resolver {
        commits: Arc<Mutex<VecDeque<Result<String, String>>>>,
        calls: Arc<Mutex<usize>>,
    }
    #[async_trait]
    impl BranchTipResolver for Resolver {
        async fn resolve_current_tip(&self, _: &str, _: &str, _: &str) -> Result<String> {
            *self.calls.lock().unwrap() += 1;
            self.commits
                .lock()
                .unwrap()
                .pop_front()
                .expect("resolver result")
                .map_err(anyhow::Error::msg)
        }
    }

    #[derive(Clone)]
    struct Sources {
        calls: Arc<Mutex<Vec<String>>>,
        fail: Arc<Mutex<bool>>,
        wrong_identity: Arc<Mutex<bool>>,
    }
    #[async_trait]
    impl DurableSourceAcquirer for Sources {
        async fn acquire_exact(
            &self,
            workspace: &str,
            repo: &str,
            commit: &str,
        ) -> Result<DurableSourceSnapshot> {
            self.calls.lock().unwrap().push(commit.to_owned());
            if *self.fail.lock().unwrap() {
                bail!("source unavailable")
            }
            let wrong = *self.wrong_identity.lock().unwrap();
            Ok(DurableSourceSnapshot {
                workspace: if wrong { "other" } else { workspace }.to_owned(),
                repo: repo.to_owned(),
                commit: commit.to_owned(),
                manifest: format!("source-{commit}"),
            })
        }
    }

    #[derive(Clone)]
    struct Observations {
        snapshots: Arc<Mutex<VecDeque<ObservationSnapshot>>>,
        outcomes: Arc<Mutex<VecDeque<ArtifactObservationOutcome>>>,
        observed: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl ArtifactObservation for Observations {
        async fn snapshot(&self, _: &str, _: &str, _: &str) -> Result<ObservationSnapshot> {
            Ok(self
                .snapshots
                .lock()
                .unwrap()
                .pop_front()
                .expect("observation snapshot"))
        }

        async fn record_tip_and_intents(
            &self,
            _: &ObservationSnapshot,
            source: &DurableSourceSnapshot,
            _: &[ArtifactKind],
            _: u32,
        ) -> Result<ArtifactObservationOutcome> {
            self.observed.lock().unwrap().push(source.commit.to_owned());
            Ok(self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("observation outcome"))
        }
    }

    fn snapshot(generation: Option<u64>, commit: Option<&str>) -> ObservationSnapshot {
        ObservationSnapshot::new(
            "acme",
            "owner/repo",
            "main",
            generation,
            commit.map(str::to_owned),
        )
    }

    fn fixture(
        commits: Vec<Result<&str, &str>>,
        snapshots: Vec<ObservationSnapshot>,
        outcomes: Vec<ArtifactObservationOutcome>,
    ) -> (
        NormalizedSyncCoordinator<Resolver, Sources, Observations>,
        Resolver,
        Sources,
        Observations,
    ) {
        let resolver = Resolver {
            commits: Arc::new(Mutex::new(
                commits
                    .into_iter()
                    .map(|r| r.map(str::to_owned).map_err(str::to_owned))
                    .collect(),
            )),
            calls: Arc::new(Mutex::new(0)),
        };
        let sources = Sources {
            calls: Arc::new(Mutex::new(Vec::new())),
            fail: Arc::new(Mutex::new(false)),
            wrong_identity: Arc::new(Mutex::new(false)),
        };
        let observations = Observations {
            snapshots: Arc::new(Mutex::new(snapshots.into())),
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            observed: Arc::new(Mutex::new(Vec::new())),
        };
        (
            NormalizedSyncCoordinator::new(resolver.clone(), sources.clone(), observations.clone()),
            resolver,
            sources,
            observations,
        )
    }

    fn request(intent: SyncIntent) -> BranchSyncRequest {
        BranchSyncRequest {
            workspace: "acme".into(),
            repo: "owner/repo".into(),
            branch: "main".into(),
            intent,
            modes: SyncModes::default(),
            format_version: 2,
        }
    }

    fn scheduled(generation: u64) -> ArtifactObservationOutcome {
        ArtifactObservationOutcome::Recorded {
            generation,
            advanced: true,
            artifacts: vec![
                (ArtifactKind::Head, ArtifactIntentOutcome::Runnable(1)),
                (
                    ArtifactKind::FullHistory,
                    ArtifactIntentOutcome::Deferred(2),
                ),
                (ArtifactKind::Files, ArtifactIntentOutcome::Runnable(3)),
            ],
        }
    }

    fn already_recorded(generation: u64) -> ArtifactObservationOutcome {
        ArtifactObservationOutcome::Recorded {
            generation,
            advanced: false,
            artifacts: vec![
                (ArtifactKind::Head, ArtifactIntentOutcome::Ready(1)),
                (
                    ArtifactKind::FullHistory,
                    ArtifactIntentOutcome::Subscribed(2),
                ),
                (ArtifactKind::Files, ArtifactIntentOutcome::Deferred(3)),
            ],
        }
    }

    #[tokio::test]
    async fn poll_at_unchanged_tip_does_not_acquire_or_schedule() {
        let (coordinator, resolver, sources, observations) =
            fixture(vec![Ok(C1)], vec![snapshot(Some(7), Some(C1))], vec![]);
        let outcome = coordinator
            .sync_branch(&request(SyncIntent::ObserveMovement))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            BranchSyncOutcome::Unchanged {
                commit: C1.into(),
                generation: 7
            }
        );
        assert_eq!(*resolver.calls.lock().unwrap(), 1);
        assert!(sources.calls.lock().unwrap().is_empty());
        assert!(observations.observed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn moved_tip_is_durable_before_all_three_jobs_are_observed() {
        let (coordinator, _, sources, observations) = fixture(
            vec![Ok(C2)],
            vec![snapshot(Some(3), Some(C1))],
            vec![scheduled(4)],
        );
        let outcome = coordinator
            .sync_branch(&request(SyncIntent::ObserveMovement))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            BranchSyncOutcome::Ensured {
                generation: 4,
                ref artifacts,
                ..
            } if artifacts.len() == 3
                && artifacts.iter().any(|(kind, state)|
                    *kind == ArtifactKind::FullHistory
                        && matches!(state, ArtifactIntentOutcome::Deferred(2)))
        ));
        assert_eq!(*sources.calls.lock().unwrap(), vec![C2]);
        assert_eq!(*observations.observed.lock().unwrap(), vec![C2]);
    }

    #[tokio::test]
    async fn source_failure_exposes_no_artifact_work() {
        let (coordinator, _, sources, observations) = fixture(
            vec![Ok(C2)],
            vec![snapshot(Some(1), Some(C1))],
            vec![scheduled(2)],
        );
        *sources.fail.lock().unwrap() = true;
        let error = coordinator
            .sync_branch(&request(SyncIntent::ObserveMovement))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("source unavailable"));
        assert!(observations.observed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stale_observation_reresolves_instead_of_replaying_old_webhook_sha() {
        let (coordinator, resolver, sources, observations) = fixture(
            vec![Ok(C2), Ok(C3)],
            vec![snapshot(Some(1), Some(C1)), snapshot(Some(2), Some(C2))],
            vec![
                ArtifactObservationOutcome::Stale {
                    current_generation: 2,
                },
                scheduled(3),
            ],
        );
        let outcome = coordinator
            .sync_branch(&request(SyncIntent::ObserveMovement))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            BranchSyncOutcome::Ensured { commit, generation: 3, .. } if commit == C3
        ));
        assert_eq!(*resolver.calls.lock().unwrap(), 2);
        assert_eq!(*sources.calls.lock().unwrap(), vec![C2, C3]);
        assert_eq!(*observations.observed.lock().unwrap(), vec![C2, C3]);
    }

    #[tokio::test]
    async fn explicit_same_commit_request_idempotently_ensures_source_and_jobs() {
        let (coordinator, _, sources, observations) = fixture(
            vec![Ok(C1)],
            vec![snapshot(Some(9), Some(C1))],
            vec![already_recorded(9)],
        );
        let outcome = coordinator
            .sync_branch(&request(SyncIntent::EnsureCurrent))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            BranchSyncOutcome::Ensured { generation: 9, ref artifacts, .. }
                if artifacts.len() == 3
        ));
        assert_eq!(*sources.calls.lock().unwrap(), vec![C1]);
        assert_eq!(*observations.observed.lock().unwrap(), vec![C1]);
    }

    #[tokio::test]
    async fn explicit_same_commit_ensure_is_generation_fenced_and_reresolves() {
        let (coordinator, resolver, sources, observations) = fixture(
            vec![Ok(C1), Ok(C2)],
            vec![snapshot(Some(1), Some(C1)), snapshot(Some(2), Some(C2))],
            vec![
                ArtifactObservationOutcome::Stale {
                    current_generation: 2,
                },
                already_recorded(2),
            ],
        );
        let outcome = coordinator
            .sync_branch(&request(SyncIntent::EnsureCurrent))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            BranchSyncOutcome::Ensured { commit, generation: 2, .. } if commit == C2
        ));
        assert_eq!(*resolver.calls.lock().unwrap(), 2);
        assert_eq!(*sources.calls.lock().unwrap(), vec![C1, C2]);
        assert_eq!(*observations.observed.lock().unwrap(), vec![C1, C2]);
    }

    #[tokio::test]
    async fn explicit_request_repairs_when_another_observer_wins_the_race() {
        let (coordinator, _, sources, observations) = fixture(
            vec![Ok(C2)],
            vec![snapshot(Some(1), Some(C1))],
            vec![already_recorded(2)],
        );
        let outcome = coordinator
            .sync_branch(&request(SyncIntent::EnsureCurrent))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            BranchSyncOutcome::Ensured { commit, generation: 2, .. } if commit == C2
        ));
        assert_eq!(*sources.calls.lock().unwrap(), vec![C2]);
        assert_eq!(*observations.observed.lock().unwrap(), vec![C2]);
    }

    #[tokio::test]
    async fn mismatched_source_identity_fails_closed_before_observation() {
        let (coordinator, _, sources, observations) = fixture(
            vec![Ok(C2)],
            vec![snapshot(Some(1), Some(C1))],
            vec![scheduled(2)],
        );
        *sources.wrong_identity.lock().unwrap() = true;
        assert!(
            coordinator
                .sync_branch(&request(SyncIntent::EnsureCurrent))
                .await
                .is_err()
        );
        assert!(observations.observed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_target_empty_modes_and_permanent_races_fail_bounded() {
        let (invalid, _, sources, observations) =
            fixture(vec![Ok("not-an-oid")], vec![snapshot(None, None)], vec![]);
        assert!(
            invalid
                .sync_branch(&request(SyncIntent::ObserveMovement))
                .await
                .is_err()
        );
        assert!(sources.calls.lock().unwrap().is_empty());
        assert!(observations.observed.lock().unwrap().is_empty());

        let (empty, _, _, _) = fixture(vec![], vec![], vec![]);
        let mut empty_request = request(SyncIntent::EnsureCurrent);
        empty_request.modes = SyncModes {
            head: false,
            full: false,
            files: false,
        };
        assert!(empty.sync_branch(&empty_request).await.is_err());

        let (racing, resolver, _, _) = fixture(
            vec![Ok(C1), Ok(C2), Ok(C3), Ok(C1)],
            vec![
                snapshot(None, None),
                snapshot(Some(1), Some(C1)),
                snapshot(Some(2), Some(C2)),
                snapshot(Some(3), Some(C3)),
            ],
            vec![
                ArtifactObservationOutcome::Stale {
                    current_generation: 1,
                },
                ArtifactObservationOutcome::Stale {
                    current_generation: 2,
                },
                ArtifactObservationOutcome::Stale {
                    current_generation: 3,
                },
                ArtifactObservationOutcome::Stale {
                    current_generation: 4,
                },
            ],
        );
        assert!(
            format!(
                "{:#}",
                racing
                    .sync_branch(&request(SyncIntent::ObserveMovement))
                    .await
                    .unwrap_err()
            )
            .contains("kept changing")
        );
        assert_eq!(*resolver.calls.lock().unwrap(), MAX_OBSERVATION_RACES);
    }
}
