//! Admission of a repository after its exact immutable clone base is durable.
//!
//! Admission is deliberately independent from branch publication.  An added
//! repository remains `Initializing` until the exact pinned target has both a
//! verified Head and FullHistory artifact.  Files is useful acceleration, but
//! never gates admission.

use crate::artifact_scheduler::{
    ArtifactKey, ArtifactKind, ArtifactRecord, ArtifactState, FailureClass, ScheduleOutcome,
};
use crate::artifact_scheduler_backend::ArtifactSchedulerPersistence;
use crate::provider::RepoId;
use crate::ref_store::{AddedRepo, RefStore, RepoLifecycleState};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const DEFAULT_FORMAT_VERSION: u32 = 1;
const DEFAULT_CONSUMER_TTL_SECS: i64 = 60 * 60;
const DEFAULT_RECONCILE_CONCURRENCY: usize = 8;
const DEFAULT_VERIFICATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const DEFAULT_VERIFICATION_CANCEL_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionVerification {
    Verified,
    Retryable(String),
    Corrupt(String),
}

#[async_trait]
pub trait AdmissionPublicationVerifier: Send + Sync {
    /// Verify one immutable publication. Implementations must observe
    /// `cancelled` and drain any blocking/process children before returning.
    async fn verify(
        &self,
        record: &ArtifactRecord,
        cancelled: CancellationToken,
    ) -> AdmissionVerification;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionOutcome {
    NotFound,
    AlreadyActive,
    Failed(String),
    WaitingForTarget,
    WaitingForArtifacts,
    StaleAttempt,
    Activated,
}

pub struct ArtifactAdmissionCoordinator {
    scheduler: Arc<dyn ArtifactSchedulerPersistence>,
    ref_store: Arc<dyn RefStore>,
    verifier: Arc<dyn AdmissionPublicationVerifier>,
    format_version: u32,
    consumer_ttl_secs: i64,
    reconcile_concurrency: usize,
    verification_limit: Arc<Semaphore>,
    verification_timeout: Duration,
    verification_cancel_grace: Duration,
}

impl ArtifactAdmissionCoordinator {
    pub fn new(
        scheduler: Arc<dyn ArtifactSchedulerPersistence>,
        ref_store: Arc<dyn RefStore>,
        verifier: Arc<dyn AdmissionPublicationVerifier>,
    ) -> Self {
        Self {
            scheduler,
            ref_store,
            verifier,
            format_version: DEFAULT_FORMAT_VERSION,
            consumer_ttl_secs: DEFAULT_CONSUMER_TTL_SECS,
            reconcile_concurrency: DEFAULT_RECONCILE_CONCURRENCY,
            verification_limit: Arc::new(Semaphore::new(DEFAULT_RECONCILE_CONCURRENCY)),
            verification_timeout: DEFAULT_VERIFICATION_TIMEOUT,
            verification_cancel_grace: DEFAULT_VERIFICATION_CANCEL_GRACE,
        }
    }

    #[cfg(test)]
    fn with_consumer_ttl(mut self, ttl_secs: i64) -> Self {
        self.consumer_ttl_secs = ttl_secs;
        self
    }

    #[cfg(test)]
    fn with_verification_policy(
        mut self,
        concurrency: usize,
        timeout: Duration,
        cancel_grace: Duration,
    ) -> Self {
        self.reconcile_concurrency = concurrency;
        self.verification_limit = Arc::new(Semaphore::new(concurrency));
        self.verification_timeout = timeout;
        self.verification_cancel_grace = cancel_grace;
        self
    }

    /// Subscribe the immutable artifacts required by one pinned admission.
    /// Every call reloads durable lifecycle state, so a stale caller cannot
    /// pin work for a replacement attempt.
    pub async fn subscribe_pinned_attempt(
        &self,
        repo_id: &RepoId,
        expected_attempt_id: &str,
    ) -> Result<AdmissionOutcome> {
        let Some(repo) = self.ref_store.load_added_repo(repo_id).await? else {
            return Ok(AdmissionOutcome::NotFound);
        };
        match repo.state {
            RepoLifecycleState::Active => {
                self.release_attempt_consumers(&repo).await?;
                return Ok(AdmissionOutcome::AlreadyActive);
            }
            RepoLifecycleState::Failed => {
                let outcome = AdmissionOutcome::Failed(
                    repo.failure
                        .clone()
                        .unwrap_or_else(|| "repository admission failed".into()),
                );
                self.release_attempt_consumers(&repo).await?;
                return Ok(outcome);
            }
            RepoLifecycleState::Initializing => {}
        }
        if repo.initialization_attempt_id.as_deref() != Some(expected_attempt_id) {
            return Ok(AdmissionOutcome::StaleAttempt);
        }
        let Some((branch, target)) = pinned_identity(&repo)? else {
            return Ok(AdmissionOutcome::WaitingForTarget);
        };
        let consumer_id = admission_consumer_id(repo_id, branch, target, expected_attempt_id);
        for kind in [ArtifactKind::Head, ArtifactKind::FullHistory] {
            let key = self.key(repo_id, target, kind);
            let outcome = self
                .scheduler
                .subscribe_consumer(&key, &consumer_id, self.consumer_ttl_secs)
                .await
                .with_context(|| format!("subscribe admission {kind:?} artifact"))?;
            if let ScheduleOutcome::Failed(id, class) = outcome {
                let detail = self.artifact_failure_detail(id).await?;
                if terminal(class) {
                    self.fail_if_current(
                        &repo,
                        target,
                        format!("{kind:?} artifact is {class:?}: {detail}"),
                    )
                    .await?;
                    return self.reload_outcome(repo_id).await;
                }
                match self.scheduler.retry_failed(&key).await? {
                    crate::artifact_scheduler::RetryOutcome::Requeued(_)
                    | crate::artifact_scheduler::RetryOutcome::NotFailed => {}
                    crate::artifact_scheduler::RetryOutcome::NotRetryable(class) => {
                        self.fail_if_current(
                            &repo,
                            target,
                            format!("{kind:?} artifact is {class:?}: {detail}"),
                        )
                        .await?;
                        return self.reload_outcome(repo_id).await;
                    }
                    crate::artifact_scheduler::RetryOutcome::Exhausted => {
                        self.fail_if_current(
                            &repo,
                            target,
                            format!("{kind:?} artifact exhausted retries: {detail}"),
                        )
                        .await?;
                        return self.reload_outcome(repo_id).await;
                    }
                }
            }
        }
        Ok(AdmissionOutcome::WaitingForArtifacts)
    }

    /// Reconcile one admission from durable state.  Readiness is not trusted:
    /// both exact publications are reverified before the lifecycle CAS.
    pub async fn reconcile_repo(&self, repo_id: &RepoId) -> Result<AdmissionOutcome> {
        let Some(repo) = self.ref_store.load_added_repo(repo_id).await? else {
            return Ok(AdmissionOutcome::NotFound);
        };
        match repo.state {
            RepoLifecycleState::Active => {
                self.release_attempt_consumers(&repo).await?;
                return Ok(AdmissionOutcome::AlreadyActive);
            }
            RepoLifecycleState::Failed => {
                let outcome = AdmissionOutcome::Failed(
                    repo.failure
                        .clone()
                        .unwrap_or_else(|| "repository admission failed".into()),
                );
                self.release_attempt_consumers(&repo).await?;
                return Ok(outcome);
            }
            RepoLifecycleState::Initializing => {}
        }
        let Some(attempt_id) = repo.initialization_attempt_id.as_deref() else {
            return Ok(AdmissionOutcome::StaleAttempt);
        };
        let Some((branch, target)) = pinned_identity(&repo)? else {
            return Ok(AdmissionOutcome::WaitingForTarget);
        };

        // Renewal happens before inspection.  Reconciliation can therefore be
        // safely restarted and long builds remain protected from collection.
        let subscribed = self.subscribe_pinned_attempt(repo_id, attempt_id).await?;
        if !matches!(subscribed, AdmissionOutcome::WaitingForArtifacts) {
            return Ok(subscribed);
        }

        let head = self
            .scheduler
            .get_by_key(&self.key(repo_id, target, ArtifactKind::Head))
            .await?;
        let history = self
            .scheduler
            .get_by_key(&self.key(repo_id, target, ArtifactKind::FullHistory))
            .await?;
        for record in [&head, &history].into_iter().flatten() {
            if record.state == ArtifactState::Failed && record.failure_class.is_some_and(terminal) {
                let reason = format!(
                    "{:?} artifact failed permanently: {}",
                    record.key.kind,
                    record.error.as_deref().unwrap_or("unspecified failure")
                );
                self.fail_if_current(&repo, target, reason).await?;
                return self.reload_outcome(repo_id).await;
            }
        }
        let (Some(head), Some(history)) = (head, history) else {
            return Ok(AdmissionOutcome::WaitingForArtifacts);
        };
        if head.state != ArtifactState::Ready || history.state != ArtifactState::Ready {
            return Ok(AdmissionOutcome::WaitingForArtifacts);
        }

        match self.verify_publication(&head).await {
            AdmissionVerification::Verified => {}
            AdmissionVerification::Retryable(_) => {
                return Ok(AdmissionOutcome::WaitingForArtifacts);
            }
            AdmissionVerification::Corrupt(error) => {
                self.fail_if_current(
                    &repo,
                    target,
                    format!("Head publication is corrupt: {error}"),
                )
                .await?;
                return self.reload_outcome(repo_id).await;
            }
        }
        if !self.attempt_is_current(repo_id, target, attempt_id).await? {
            let _ = self.release_attempt_consumers(&repo).await;
            return self.reload_outcome(repo_id).await;
        }
        match self.verify_publication(&history).await {
            AdmissionVerification::Verified => {}
            AdmissionVerification::Retryable(_) => {
                return Ok(AdmissionOutcome::WaitingForArtifacts);
            }
            AdmissionVerification::Corrupt(error) => {
                self.fail_if_current(
                    &repo,
                    target,
                    format!("FullHistory publication is corrupt: {error}"),
                )
                .await?;
                return self.reload_outcome(repo_id).await;
            }
        }

        let activated = self
            .ref_store
            .activate_repo(repo_id, branch, target, Some(attempt_id))
            .await?;
        if !activated {
            let _ = self.release_attempt_consumers(&repo).await;
            return self.reload_outcome(repo_id).await;
        }
        // Activation is already durable. A transient cleanup failure must not
        // turn a successfully admitted repository back into an error.
        let _ = self.release_attempt_consumers(&repo).await;
        Ok(AdmissionOutcome::Activated)
    }

    /// Reconcile every initializing repository without allowing one malformed
    /// or temporarily unavailable repository to starve all later entries.
    /// Only failure to enumerate durable lifecycle state aborts the pass.
    pub async fn reconcile_all(
        &self,
    ) -> Result<Vec<(RepoId, Result<AdmissionOutcome, anyhow::Error>)>> {
        let repos = self.ref_store.list_added_repos().await?;
        let outcomes = stream::iter(
            repos
                .into_iter()
                .filter(|repo| repo.state == RepoLifecycleState::Initializing),
        )
        .map(|repo| async move {
            let outcome = self.reconcile_repo(&repo.repo_id).await;
            (repo.repo_id, outcome)
        })
        .buffer_unordered(self.reconcile_concurrency)
        .collect()
        .await;
        Ok(outcomes)
    }

    /// Release the exact attempt's durable consumers. Removal handlers call
    /// this before deleting lifecycle state; reconciliation also calls it when
    /// an attempt terminates or loses its CAS race.
    pub async fn release_attempt_consumers(&self, repo: &AddedRepo) -> Result<()> {
        let Some(attempt_id) = repo.initialization_attempt_id.as_deref() else {
            return Ok(());
        };
        let Some((branch, target)) = pinned_identity(repo)? else {
            return Ok(());
        };
        let consumer_id = admission_consumer_id(&repo.repo_id, branch, target, attempt_id);
        for kind in [ArtifactKind::Head, ArtifactKind::FullHistory] {
            if let Some(record) = self
                .scheduler
                .get_by_key(&self.key(&repo.repo_id, target, kind))
                .await?
            {
                self.scheduler
                    .release_consumer(record.id, &consumer_id)
                    .await?;
            }
        }
        Ok(())
    }

    fn key(&self, repo_id: &RepoId, commit: &str, kind: ArtifactKind) -> ArtifactKey {
        ArtifactKey {
            workspace: repo_id.workspace.as_str().to_owned(),
            repo: repo_id.path.clone(),
            commit: commit.to_owned(),
            kind,
            format_version: self.format_version,
        }
    }

    async fn fail_if_current(&self, repo: &AddedRepo, target: &str, failure: String) -> Result<()> {
        let branch = repo
            .initialization_branch
            .as_deref()
            .context("pinned admission has no branch")?;
        self.ref_store
            .fail_repo_initialization(
                &repo.repo_id,
                branch,
                Some(target),
                &failure,
                repo.initialization_attempt_id.as_deref(),
            )
            .await?;
        self.release_attempt_consumers(repo).await?;
        Ok(())
    }

    async fn artifact_failure_detail(&self, id: i64) -> Result<String> {
        Ok(self
            .scheduler
            .get(id)
            .await?
            .and_then(|record| record.error)
            .unwrap_or_else(|| "unspecified failure".into()))
    }

    async fn verify_publication(&self, record: &ArtifactRecord) -> AdmissionVerification {
        let permit = match tokio::time::timeout(
            self.verification_timeout,
            self.verification_limit.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return AdmissionVerification::Retryable(
                    "admission verification concurrency gate is closed".into(),
                );
            }
            Err(_) => {
                return AdmissionVerification::Retryable(
                    "admission verification concurrency gate is saturated".into(),
                );
            }
        };
        let cancelled = CancellationToken::new();
        let verifier = self.verifier.clone();
        let record = record.clone();
        let verify_cancel = cancelled.clone();
        let mut verification =
            tokio::spawn(async move { verifier.verify(&record, verify_cancel).await });
        let mut did_not_drain = false;
        let outcome = match tokio::time::timeout(self.verification_timeout, &mut verification).await
        {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => AdmissionVerification::Retryable(if error.is_panic() {
                "admission verifier panicked".into()
            } else {
                format!("admission verifier task failed: {error}")
            }),
            Err(_) => {
                cancelled.cancel();
                match tokio::time::timeout(self.verification_cancel_grace, &mut verification).await
                {
                    Ok(Ok(AdmissionVerification::Corrupt(error))) => {
                        // Timeout dominates a late semantic result: the verifier
                        // did not complete within policy and must be retried.
                        AdmissionVerification::Retryable(format!(
                            "admission verification timed out before reporting: {error}"
                        ))
                    }
                    Ok(Ok(AdmissionVerification::Retryable(error))) => {
                        AdmissionVerification::Retryable(error)
                    }
                    Ok(Ok(AdmissionVerification::Verified)) => AdmissionVerification::Retryable(
                        "admission verification completed only after timeout".into(),
                    ),
                    Ok(Err(error)) => AdmissionVerification::Retryable(if error.is_panic() {
                        "admission verifier panicked while cancelling".into()
                    } else {
                        format!("admission verifier cancellation task failed: {error}")
                    }),
                    Err(_) => {
                        did_not_drain = true;
                        verification.abort();
                        let _ = verification.await;
                        AdmissionVerification::Retryable(
                            "admission verifier did not drain after cancellation".into(),
                        )
                    }
                }
            }
        };
        if did_not_drain {
            // Quarantine this capacity slot. Releasing it would allow repeated
            // hostile publications to accumulate detached blocking children.
            permit.forget();
        }
        outcome
    }

    async fn reload_outcome(&self, repo_id: &RepoId) -> Result<AdmissionOutcome> {
        let Some(repo) = self.ref_store.load_added_repo(repo_id).await? else {
            return Ok(AdmissionOutcome::NotFound);
        };
        Ok(match repo.state {
            RepoLifecycleState::Active => AdmissionOutcome::AlreadyActive,
            RepoLifecycleState::Failed => AdmissionOutcome::Failed(
                repo.failure
                    .unwrap_or_else(|| "repository admission failed".into()),
            ),
            RepoLifecycleState::Initializing => AdmissionOutcome::StaleAttempt,
        })
    }

    async fn attempt_is_current(
        &self,
        repo_id: &RepoId,
        target: &str,
        attempt_id: &str,
    ) -> Result<bool> {
        Ok(self
            .ref_store
            .load_added_repo(repo_id)
            .await?
            .is_some_and(|repo| {
                repo.state == RepoLifecycleState::Initializing
                    && repo.initialization_target.as_deref() == Some(target)
                    && repo.initialization_attempt_id.as_deref() == Some(attempt_id)
            }))
    }
}

fn pinned_identity(repo: &AddedRepo) -> Result<Option<(&str, &str)>> {
    let Some(branch) = repo.initialization_branch.as_deref() else {
        return Ok(None);
    };
    let Some(target) = repo.initialization_target.as_deref() else {
        return Ok(None);
    };
    crate::artifact_scheduler::validate_canonical_commit_oid(target)
        .context("invalid pinned admission target")?;
    if branch.is_empty() {
        bail!("pinned admission branch is empty")
    }
    Ok(Some((branch, target)))
}

fn admission_consumer_id(repo_id: &RepoId, branch: &str, target: &str, attempt_id: &str) -> String {
    let mut digest = Sha256::new();
    for component in [
        repo_id.workspace.as_str(),
        repo_id.path.as_str(),
        branch,
        target,
        attempt_id,
    ] {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component.as_bytes());
    }
    format!("admission-{}", hex::encode(digest.finalize()))
}

fn terminal(class: FailureClass) -> bool {
    matches!(class, FailureClass::Permanent | FailureClass::DeadLetter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{ArtifactScheduler, CompletionEvidence, SchedulerLimits};
    use crate::provider::WorkspaceId;
    use crate::ref_store::{AddedRepoSource, FileRefStore};
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Mutex, Notify};

    const TARGET: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[derive(Default)]
    struct TestVerifier {
        corrupt: Mutex<HashSet<String>>,
        transient: Mutex<HashSet<String>>,
        hang: Mutex<HashSet<String>>,
        panic_on: Mutex<HashSet<String>>,
        pause: AtomicBool,
        calls: AtomicUsize,
        entered: Notify,
        resume: Notify,
    }

    #[async_trait]
    impl AdmissionPublicationVerifier for TestVerifier {
        async fn verify(
            &self,
            record: &ArtifactRecord,
            cancelled: CancellationToken,
        ) -> AdmissionVerification {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                tokio::select! {
                    _ = self.resume.notified() => {}
                    _ = cancelled.cancelled() => {
                        return AdmissionVerification::Retryable("test verification cancelled".into());
                    }
                }
            }
            let Some(manifest) = record.manifest.as_deref() else {
                return AdmissionVerification::Corrupt("missing manifest".into());
            };
            if self.hang.lock().await.contains(manifest) {
                cancelled.cancelled().await;
                return AdmissionVerification::Retryable("test verification timed out".into());
            }
            assert!(
                !self.panic_on.lock().await.contains(manifest),
                "test verifier panic"
            );
            if self.transient.lock().await.contains(manifest) {
                return AdmissionVerification::Retryable("test storage outage".into());
            }
            if self.corrupt.lock().await.contains(manifest) {
                return AdmissionVerification::Corrupt("test corruption".into());
            }
            AdmissionVerification::Verified
        }
    }

    struct Fixture {
        _root: tempfile::TempDir,
        scheduler: Arc<ArtifactScheduler>,
        refs: Arc<FileRefStore>,
        verifier: Arc<TestVerifier>,
        coordinator: Arc<ArtifactAdmissionCoordinator>,
        repo_id: RepoId,
        db_url: String,
    }

    impl Fixture {
        async fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let db_url = format!("sqlite://{}", root.path().join("scheduler.db").display());
            let scheduler = Arc::new(
                ArtifactScheduler::open(&db_url, SchedulerLimits::default())
                    .await
                    .unwrap(),
            );
            let refs = Arc::new(FileRefStore::new(root.path()));
            let verifier = Arc::new(TestVerifier::default());
            let coordinator = Arc::new(
                ArtifactAdmissionCoordinator::new(
                    scheduler.clone(),
                    refs.clone(),
                    verifier.clone(),
                )
                .with_consumer_ttl(60),
            );
            Self {
                _root: root,
                scheduler,
                refs,
                verifier,
                coordinator,
                repo_id: RepoId {
                    workspace: WorkspaceId::new("workspace-a"),
                    path: "owner/repo".into(),
                },
                db_url,
            }
        }

        fn repo(&self, attempt: &str) -> AddedRepo {
            self.repo_for(&self.repo_id, attempt)
        }

        fn repo_for(&self, repo_id: &RepoId, attempt: &str) -> AddedRepo {
            AddedRepo {
                repo_id: repo_id.clone(),
                added_at: 1,
                history_enabled: true,
                source: AddedRepoSource::Api,
                repo_size_bytes: None,
                state: RepoLifecycleState::Initializing,
                initialization_branch: Some("main".into()),
                initialization_target: Some(TARGET.into()),
                activated_at: None,
                failure: None,
                initialization_attempt_id: Some(attempt.into()),
            }
        }

        async fn add(&self, attempt: &str) {
            self.refs.add_repo(&self.repo(attempt)).await.unwrap();
        }

        fn key(&self, kind: ArtifactKind) -> ArtifactKey {
            ArtifactKey {
                workspace: self.repo_id.workspace.as_str().into(),
                repo: self.repo_id.path.clone(),
                commit: TARGET.into(),
                kind,
                format_version: 1,
            }
        }

        async fn make_required_ready(&self, head_manifest: &str, history_manifest: &str) {
            self.make_ready_for(&self.repo_id, "attempt-1", head_manifest, history_manifest)
                .await;
        }

        async fn make_ready_for(
            &self,
            repo_id: &RepoId,
            attempt: &str,
            head_manifest: &str,
            history_manifest: &str,
        ) {
            self.coordinator
                .subscribe_pinned_attempt(repo_id, attempt)
                .await
                .unwrap();
            for _ in 0..2 {
                let claim = self
                    .scheduler
                    .claim("test-worker", 30)
                    .await
                    .unwrap()
                    .unwrap();
                let manifest = match claim.record.key.kind {
                    ArtifactKind::Head => head_manifest,
                    ArtifactKind::FullHistory => history_manifest,
                    ArtifactKind::Files => panic!("Files must not gate admission"),
                };
                let evidence = CompletionEvidence::new(claim.record.key.clone(), manifest).unwrap();
                assert!(
                    self.scheduler
                        .complete(&claim, "test-worker", &evidence)
                        .await
                        .unwrap()
                );
            }
        }

        async fn consumer_count(&self) -> i64 {
            let pool = sqlx::SqlitePool::connect(&self.db_url).await.unwrap();
            sqlx::query_scalar("SELECT count(*) FROM artifact_consumers")
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    async fn exact_head_and_history_admit_without_files() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;

        assert_eq!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Activated
        );
        assert_eq!(
            f.refs
                .load_added_repo(&f.repo_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RepoLifecycleState::Active
        );
        assert!(
            f.scheduler
                .get_by_key(&f.key(ArtifactKind::Files))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn retryable_or_incomplete_work_remains_initializing() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.coordinator
            .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
            .await
            .unwrap();
        let claim = f.scheduler.claim("worker", 30).await.unwrap().unwrap();
        f.scheduler
            .fail(&claim, "worker", FailureClass::Retryable, "temporary")
            .await
            .unwrap();

        assert_eq!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::WaitingForArtifacts
        );
        assert_eq!(
            f.refs
                .load_added_repo(&f.repo_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RepoLifecycleState::Initializing
        );
        let requeued = f.scheduler.get(claim.record.id).await.unwrap().unwrap();
        assert_eq!(requeued.state, ArtifactState::Queued);
    }

    #[tokio::test]
    async fn transient_verification_does_not_poison_admission() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        f.verifier.transient.lock().await.insert("head".into());

        assert_eq!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::WaitingForArtifacts
        );
        assert_eq!(
            f.refs
                .load_added_repo(&f.repo_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RepoLifecycleState::Initializing
        );
        f.verifier.transient.lock().await.clear();
        assert_eq!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Activated
        );
    }

    #[tokio::test]
    async fn verifier_panic_is_retryable_not_process_or_lifecycle_failure() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        f.verifier.panic_on.lock().await.insert("head".into());

        assert_eq!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::WaitingForArtifacts
        );
        assert_eq!(
            f.refs
                .load_added_repo(&f.repo_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RepoLifecycleState::Initializing
        );
    }

    #[tokio::test]
    async fn permanent_required_failure_fails_only_current_attempt() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.coordinator
            .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
            .await
            .unwrap();
        let claim = f.scheduler.claim("worker", 30).await.unwrap().unwrap();
        f.scheduler
            .fail(
                &claim,
                "worker",
                FailureClass::Permanent,
                "invalid repository",
            )
            .await
            .unwrap();

        assert!(matches!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Failed(message) if message.contains("invalid repository")
        ));
        let failed = f.refs.load_added_repo(&f.repo_id).await.unwrap().unwrap();
        assert_eq!(failed.state, RepoLifecycleState::Failed);
        assert_eq!(
            failed.initialization_attempt_id.as_deref(),
            Some("attempt-1")
        );
        assert_eq!(
            f.consumer_count().await,
            0,
            "terminal attempts must release consumers"
        );
    }

    #[tokio::test]
    async fn exhausted_retry_budget_fails_instead_of_waiting_forever() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.coordinator
            .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
            .await
            .unwrap();
        let mut final_outcome = AdmissionOutcome::WaitingForArtifacts;
        for _ in 0..16 {
            if let Some(claim) = f.scheduler.claim("worker", 30).await.unwrap() {
                f.scheduler
                    .fail(&claim, "worker", FailureClass::Retryable, "temporary")
                    .await
                    .unwrap();
            }
            final_outcome = f.coordinator.reconcile_repo(&f.repo_id).await.unwrap();
            if matches!(final_outcome, AdmissionOutcome::Failed(_)) {
                break;
            }
        }
        assert!(matches!(
            final_outcome,
            AdmissionOutcome::Failed(message) if message.contains("exhausted retries")
        ));
    }

    #[tokio::test]
    async fn ready_but_corrupt_publication_fails_closed() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "corrupt-history").await;
        f.verifier
            .corrupt
            .lock()
            .await
            .insert("corrupt-history".into());

        assert!(matches!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Failed(message) if message.contains("corrupt")
        ));
        assert_eq!(
            f.refs
                .load_added_repo(&f.repo_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RepoLifecycleState::Failed
        );
    }

    #[tokio::test]
    async fn stale_attempt_cannot_subscribe() {
        let f = Fixture::new().await;
        f.add("attempt-2").await;
        assert_eq!(
            f.coordinator
                .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
                .await
                .unwrap(),
            AdmissionOutcome::StaleAttempt
        );
        assert!(
            f.scheduler
                .get_by_key(&f.key(ArtifactKind::Head))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            f.scheduler
                .get_by_key(&f.key(ArtifactKind::FullHistory))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn removal_during_verification_cannot_activate() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        f.verifier.pause.store(true, Ordering::SeqCst);
        let coordinator = f.coordinator.clone();
        let repo_id = f.repo_id.clone();
        let task = tokio::spawn(async move { coordinator.reconcile_repo(&repo_id).await.unwrap() });
        f.verifier.entered.notified().await;
        f.refs.remove_added_repo(&f.repo_id).await.unwrap();
        f.verifier.resume.notify_one();

        assert_eq!(task.await.unwrap(), AdmissionOutcome::NotFound);
        assert!(f.refs.load_added_repo(&f.repo_id).await.unwrap().is_none());
        assert_eq!(f.verifier.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.consumer_count().await, 0);
    }

    #[tokio::test]
    async fn replacement_attempt_wins_race_with_old_verification() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        f.verifier.pause.store(true, Ordering::SeqCst);
        let coordinator = f.coordinator.clone();
        let repo_id = f.repo_id.clone();
        let task = tokio::spawn(async move { coordinator.reconcile_repo(&repo_id).await.unwrap() });
        f.verifier.entered.notified().await;
        f.refs.remove_added_repo(&f.repo_id).await.unwrap();
        f.refs.add_repo(&f.repo("attempt-2")).await.unwrap();
        f.verifier.resume.notify_one();

        assert_eq!(task.await.unwrap(), AdmissionOutcome::StaleAttempt);
        let replacement = f.refs.load_added_repo(&f.repo_id).await.unwrap().unwrap();
        assert_eq!(replacement.state, RepoLifecycleState::Initializing);
        assert_eq!(
            replacement.initialization_attempt_id.as_deref(),
            Some("attempt-2")
        );
        assert_eq!(f.verifier.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.consumer_count().await, 0);
    }

    #[tokio::test]
    async fn invalid_or_unpinned_target_never_schedules() {
        let f = Fixture::new().await;
        let mut repo = f.repo("attempt-1");
        repo.initialization_target = None;
        f.refs.add_repo(&repo).await.unwrap();
        assert_eq!(
            f.coordinator
                .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
                .await
                .unwrap(),
            AdmissionOutcome::WaitingForTarget
        );
        f.refs.remove_added_repo(&f.repo_id).await.unwrap();
        let mut malformed = f.repo("attempt-1");
        malformed.initialization_target = Some("not-an-oid".into());
        f.refs.add_repo(&malformed).await.unwrap();
        assert!(
            f.coordinator
                .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid pinned admission target")
        );
    }

    #[tokio::test]
    async fn malformed_repo_does_not_starve_reconcile_pass() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        let malformed_id = RepoId {
            workspace: WorkspaceId::new("workspace-a"),
            path: "owner/malformed".into(),
        };
        let mut malformed = f.repo("bad-attempt");
        malformed.repo_id = malformed_id.clone();
        malformed.initialization_target = Some("malformed".into());
        f.refs.add_repo(&malformed).await.unwrap();

        let outcomes = f.coordinator.reconcile_all().await.unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().any(|(id, result)| {
            id == &f.repo_id && matches!(result, Ok(AdmissionOutcome::WaitingForArtifacts))
        }));
        assert!(outcomes.iter().any(|(id, result)| {
            id == &malformed_id
                && result
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("invalid pinned admission target")
        }));
    }

    #[tokio::test]
    async fn timed_out_verifier_cannot_starve_other_repositories() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("hang-head", "history").await;
        let second = RepoId {
            workspace: WorkspaceId::new("workspace-a"),
            path: "owner/second".into(),
        };
        f.refs
            .add_repo(&f.repo_for(&second, "attempt-2"))
            .await
            .unwrap();
        f.make_ready_for(&second, "attempt-2", "second-head", "second-history")
            .await;
        f.verifier.hang.lock().await.insert("hang-head".into());
        let coordinator = ArtifactAdmissionCoordinator::new(
            f.scheduler.clone(),
            f.refs.clone(),
            f.verifier.clone(),
        )
        .with_consumer_ttl(60)
        .with_verification_policy(2, Duration::from_millis(25), Duration::from_millis(25));

        let started = std::time::Instant::now();
        let outcomes = coordinator.reconcile_all().await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(outcomes.iter().any(|(id, result)| {
            id == &f.repo_id && matches!(result, Ok(AdmissionOutcome::WaitingForArtifacts))
        }));
        assert!(
            outcomes
                .iter()
                .any(|(id, result)| id == &second
                    && matches!(result, Ok(AdmissionOutcome::Activated)))
        );
    }
}
