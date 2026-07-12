//! Admission of a repository after its exact immutable clone base is durable.
//!
//! Admission is deliberately independent from branch publication.  An added
//! repository remains `Initializing` until the exact pinned target has both a
//! verified Head and FullHistory artifact.  Files is useful acceleration, but
//! never gates admission.

use crate::artifact_scheduler::{
    ActivationFenceProvenance, ArtifactKey, ArtifactKind, ArtifactRecord, ArtifactState,
    FailureClass, QuarantineOutcome, ScheduleOutcome,
};
use crate::artifact_scheduler_backend::ArtifactSchedulerPersistence;
use crate::provider::RepoId;
use crate::ref_store::{AddedRepo, RefStore, RepoLifecycleState};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const DEFAULT_CONSUMER_TTL_SECS: i64 = 60 * 60;
const ACTIVATION_FENCE_TTL_SECS: i64 = 60;
const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RECONCILE_CONCURRENCY: usize = 8;
const DEFAULT_VERIFICATION_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const DEFAULT_VERIFICATION_CANCEL_GRACE: Duration = Duration::from_secs(5);
const DEFAULT_VERIFICATION_QUEUE_TIMEOUT: Duration = Duration::from_secs(1);

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
    /// Verification isolation has lost every capacity slot. Readiness should
    /// report degraded until the coordinator/verifier process is replaced.
    Degraded(String),
    StaleAttempt,
    Activated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionReadiness {
    pub healthy: bool,
    pub poisoned_verifier_slots: usize,
    pub verifier_slots: usize,
    pub busy_verifier_slots: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownAdmissionRecoveryPage {
    pub processed: usize,
    pub settled: usize,
    pub retained: usize,
    pub next_generation: Option<u64>,
}

pub struct ArtifactAdmissionCoordinator {
    scheduler: Arc<dyn ArtifactSchedulerPersistence>,
    ref_store: Arc<dyn RefStore>,
    verifier: Arc<dyn AdmissionPublicationVerifier>,
    format_version: u32,
    consumer_ttl_secs: i64,
    reconcile_concurrency: usize,
    verification_limit: Arc<Semaphore>,
    verification_slots: usize,
    poisoned_verifier_slots: Arc<AtomicUsize>,
    verification_timeout: Duration,
    verification_cancel_grace: Duration,
    verification_queue_timeout: Duration,
    activation_timeout: Duration,
    unknown_recovery_cursor: AtomicU64,
    #[cfg(test)]
    after_subscribe: Option<Arc<dyn TestSubscriptionHook>>,
    #[cfg(test)]
    after_activation_fence: Option<Arc<dyn TestActivationFenceHook>>,
}

struct VerificationTaskGuard {
    cancelled: CancellationToken,
    task: Option<JoinHandle<AdmissionVerification>>,
    permit: Option<OwnedSemaphorePermit>,
    poisoned_slots: Arc<AtomicUsize>,
    armed: bool,
}

impl VerificationTaskGuard {
    fn poison(&mut self) {
        self.cancelled.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        if let Some(permit) = self.permit.take() {
            permit.forget();
            self.poisoned_slots.fetch_add(1, Ordering::AcqRel);
        }
        self.armed = false;
    }

    fn finish(&mut self) {
        self.task.take();
        self.permit.take();
        self.armed = false;
    }
}

impl Drop for VerificationTaskGuard {
    fn drop(&mut self) {
        if self.armed {
            // Dropping the outer reconcile future must never detach verifier
            // work. Cancellation plus task abortion is immediate; capacity is
            // conservatively poisoned because blocking children cannot be
            // proven drained from synchronous Drop.
            self.poison();
        }
    }
}

#[cfg(test)]
#[async_trait]
trait TestSubscriptionHook: Send + Sync {
    async fn subscribed(&self, kind: ArtifactKind);
}

#[cfg(test)]
#[async_trait]
trait TestActivationFenceHook: Send + Sync {
    async fn fenced(&self);
}

impl ArtifactAdmissionCoordinator {
    pub fn new(
        scheduler: Arc<dyn ArtifactSchedulerPersistence>,
        ref_store: Arc<dyn RefStore>,
        verifier: Arc<dyn AdmissionPublicationVerifier>,
        format_version: u32,
    ) -> Result<Self> {
        if format_version == 0 {
            bail!("admission artifact format version must be nonzero")
        }
        if !scheduler.full_admission_recovery_protocol_supported() {
            bail!("admission scheduler lacks the full typed recovery protocol")
        }
        Ok(Self {
            scheduler,
            ref_store,
            verifier,
            format_version,
            consumer_ttl_secs: DEFAULT_CONSUMER_TTL_SECS,
            reconcile_concurrency: DEFAULT_RECONCILE_CONCURRENCY,
            verification_limit: Arc::new(Semaphore::new(DEFAULT_RECONCILE_CONCURRENCY)),
            verification_slots: DEFAULT_RECONCILE_CONCURRENCY,
            poisoned_verifier_slots: Arc::new(AtomicUsize::new(0)),
            verification_timeout: DEFAULT_VERIFICATION_TIMEOUT,
            verification_cancel_grace: DEFAULT_VERIFICATION_CANCEL_GRACE,
            verification_queue_timeout: DEFAULT_VERIFICATION_QUEUE_TIMEOUT,
            activation_timeout: ACTIVATION_TIMEOUT,
            unknown_recovery_cursor: AtomicU64::new(0),
            #[cfg(test)]
            after_subscribe: None,
            #[cfg(test)]
            after_activation_fence: None,
        })
    }

    #[cfg(test)]
    fn with_subscription_hook(mut self, hook: Arc<dyn TestSubscriptionHook>) -> Self {
        self.after_subscribe = Some(hook);
        self
    }

    #[cfg(test)]
    fn with_activation_fence_hook(mut self, hook: Arc<dyn TestActivationFenceHook>) -> Self {
        self.after_activation_fence = Some(hook);
        self
    }

    #[cfg(test)]
    fn with_activation_timeout(mut self, timeout: Duration) -> Self {
        self.activation_timeout = timeout;
        self
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
        self.verification_slots = concurrency;
        self.poisoned_verifier_slots = Arc::new(AtomicUsize::new(0));
        self.verification_timeout = timeout;
        self.verification_cancel_grace = cancel_grace;
        self.verification_queue_timeout = timeout.min(DEFAULT_VERIFICATION_QUEUE_TIMEOUT);
        self
    }

    /// A non-draining verifier may still own blocking children after task
    /// abortion. Such slots are permanently poisoned in this coordinator;
    /// recovery is intentionally process/coordinator replacement, never an
    /// unsafe in-place permit reset.
    pub fn readiness(&self) -> AdmissionReadiness {
        let poisoned = self.poisoned_verifier_slots.load(Ordering::Acquire);
        AdmissionReadiness {
            healthy: poisoned < self.verification_slots,
            poisoned_verifier_slots: poisoned,
            verifier_slots: self.verification_slots,
            busy_verifier_slots: self
                .verification_slots
                .saturating_sub(poisoned)
                .saturating_sub(self.verification_limit.available_permits()),
        }
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
                self.settle_activation_operation_for(&repo).await?;
                self.release_attempt_consumers(&repo).await?;
                return Ok(AdmissionOutcome::AlreadyActive);
            }
            RepoLifecycleState::Failed => {
                let outcome = AdmissionOutcome::Failed(
                    repo.failure
                        .clone()
                        .unwrap_or_else(|| "repository admission failed".into()),
                );
                self.settle_activation_operation_for(&repo).await?;
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
            #[cfg(test)]
            if let Some(hook) = &self.after_subscribe {
                hook.subscribed(kind).await;
            }
            // Subscription is durable. Recheck immediately so replacement or
            // removal racing either subscription cannot leak the old attempt's
            // consumer or cause the second artifact to be subscribed.
            if !self
                .attempt_is_current(repo_id, target, expected_attempt_id)
                .await?
            {
                self.release_attempt_consumers(&repo).await?;
                return self.reload_outcome(repo_id).await;
            }
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
                self.settle_activation_operation_for(&repo).await?;
                self.release_attempt_consumers(&repo).await?;
                return Ok(AdmissionOutcome::AlreadyActive);
            }
            RepoLifecycleState::Failed => {
                let outcome = AdmissionOutcome::Failed(
                    repo.failure
                        .clone()
                        .unwrap_or_else(|| "repository admission failed".into()),
                );
                self.settle_activation_operation_for(&repo).await?;
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
                return self
                    .retryable_outcome_if_current(&repo, target, attempt_id)
                    .await;
            }
            AdmissionVerification::Corrupt(error) => {
                return self.repair_corrupt(&repo, target, &head, error).await;
            }
        }
        if !self.attempt_is_current(repo_id, target, attempt_id).await? {
            let _ = self.release_attempt_consumers(&repo).await;
            return self.reload_outcome(repo_id).await;
        }
        match self.verify_publication(&history).await {
            AdmissionVerification::Verified => {}
            AdmissionVerification::Retryable(_) => {
                return self
                    .retryable_outcome_if_current(&repo, target, attempt_id)
                    .await;
            }
            AdmissionVerification::Corrupt(error) => {
                return self.repair_corrupt(&repo, target, &history, error).await;
            }
        }

        if !self.attempt_is_current(repo_id, target, attempt_id).await? {
            self.release_attempt_consumers(&repo).await?;
            return self.reload_outcome(repo_id).await;
        }
        // Acquire one pairwise manifest-CAS guard. Quarantine cannot withdraw
        // either publication while this expiring guard is live, closing the
        // cross-store check/activate window without permanent crash leaks.
        let activation_provenance = ActivationFenceProvenance {
            workspace: repo_id.workspace.as_str().to_owned(),
            repo: repo_id.path.clone(),
            branch: branch.to_owned(),
            target: target.to_owned(),
            attempt_id: attempt_id.to_owned(),
        };
        let expected = [
            (head.id, head.manifest.clone()),
            (history.id, history.manifest.clone()),
        ];
        let Some(fence) = self
            .scheduler
            .fence_ready_publications(&expected, &activation_provenance, ACTIVATION_FENCE_TTL_SECS)
            .await?
        else {
            return Ok(AdmissionOutcome::WaitingForArtifacts);
        };
        if !self
            .scheduler
            .mark_activation_unknown(&fence, ACTIVATION_FENCE_TTL_SECS)
            .await?
        {
            return Ok(AdmissionOutcome::WaitingForArtifacts);
        }
        #[cfg(test)]
        if let Some(hook) = &self.after_activation_fence {
            hook.fenced().await;
        }

        let activation = tokio::time::timeout(
            self.activation_timeout,
            self.ref_store
                .activate_repo(repo_id, branch, target, Some(attempt_id)),
        )
        .await;
        let activated = match activation {
            Ok(Ok(activated)) => activated,
            Ok(Err(error)) => {
                // The store may have committed before surfacing an error. Keep
                // the durable unknown fence for idempotent recovery.
                return Err(error).context("repository activation outcome is unknown");
            }
            Err(_) => {
                // Cancellation/timeout is ambiguous. A late commit is allowed;
                // the next pass observes lifecycle state or renews/takes over
                // this exact operation before retrying the idempotent CAS.
                return Ok(AdmissionOutcome::WaitingForArtifacts);
            }
        };
        if !activated {
            let current = self.ref_store.load_added_repo(repo_id).await?;
            let definitely_lost = !current.as_ref().is_some_and(|current| {
                current.state == RepoLifecycleState::Initializing
                    && current.initialization_attempt_id.as_deref() == Some(attempt_id)
                    && current.initialization_target.as_deref() == Some(target)
            });
            if definitely_lost {
                self.scheduler
                    .release_ready_publication_fence(fence)
                    .await?;
                let _ = self.release_attempt_consumers(&repo).await;
                return self.reload_outcome(repo_id).await;
            }
            return Ok(AdmissionOutcome::WaitingForArtifacts);
        }
        let _ = self.scheduler.release_ready_publication_fence(fence).await;
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
        // One bounded durable recovery page per recurring pass prevents a
        // crashed activation worker from leaving permanent safety roots.
        let cursor = self.unknown_recovery_cursor.load(Ordering::Acquire);
        let recovery = self
            .reconcile_unknown_fences_page((cursor != 0).then_some(cursor), 128)
            .await?;
        self.unknown_recovery_cursor
            .store(recovery.next_generation.unwrap_or(0), Ordering::Release);
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

    /// Bounded fleet recovery for activation calls whose outcome was unknown
    /// when their original worker stopped. The durable generation capability
    /// is the only authority that can settle a root.
    pub async fn reconcile_unknown_fences_page(
        &self,
        after_generation: Option<u64>,
        limit: usize,
    ) -> Result<UnknownAdmissionRecoveryPage> {
        let page = self
            .scheduler
            .unknown_activation_fences_page(after_generation, limit)
            .await?;
        let mut settled = 0;
        let mut retained = 0;
        let processed = page.fences.len();
        for fence in page.fences {
            let provenance = fence.provenance().clone();
            let repo_id = RepoId {
                workspace: crate::provider::WorkspaceId::new(provenance.workspace.clone()),
                path: provenance.repo.clone(),
            };
            let current = self.ref_store.load_added_repo(&repo_id).await?;
            let is_current = current.as_ref().is_some_and(|repo| {
                repo.state == RepoLifecycleState::Initializing
                    && repo.initialization_branch.as_deref() == Some(&provenance.branch)
                    && repo.initialization_target.as_deref() == Some(&provenance.target)
                    && repo.initialization_attempt_id.as_deref() == Some(&provenance.attempt_id)
            });
            if !is_current {
                self.scheduler
                    .release_ready_publication_fence(fence)
                    .await?;
                settled += 1;
                continue;
            }
            if !self
                .scheduler
                .mark_activation_unknown(&fence, ACTIVATION_FENCE_TTL_SECS)
                .await?
            {
                retained += 1;
                continue;
            }
            let activation = tokio::time::timeout(
                self.activation_timeout,
                self.ref_store.activate_repo(
                    &repo_id,
                    &provenance.branch,
                    &provenance.target,
                    Some(&provenance.attempt_id),
                ),
            )
            .await;
            let durable_terminal = match activation {
                Ok(Ok(true)) => true,
                Ok(Ok(false)) | Ok(Err(_)) | Err(_) => !self
                    .ref_store
                    .load_added_repo(&repo_id)
                    .await?
                    .is_some_and(|repo| {
                        repo.state == RepoLifecycleState::Initializing
                            && repo.initialization_attempt_id.as_deref()
                                == Some(&provenance.attempt_id)
                            && repo.initialization_target.as_deref() == Some(&provenance.target)
                    }),
            };
            if durable_terminal {
                self.scheduler
                    .release_ready_publication_fence(fence)
                    .await?;
                settled += 1;
            } else {
                retained += 1;
            }
        }
        Ok(UnknownAdmissionRecoveryPage {
            processed,
            settled,
            retained,
            next_generation: page.next_generation,
        })
    }

    /// Release the exact attempt's durable consumers. Removal handlers call
    /// this before deleting lifecycle state; reconciliation also calls it when
    /// an attempt terminates or loses its CAS race.
    pub async fn release_attempt_consumers(&self, repo: &AddedRepo) -> Result<()> {
        self.settle_activation_operation_for(repo).await?;
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

    async fn settle_activation_operation_for(&self, repo: &AddedRepo) -> Result<()> {
        let Some(attempt_id) = repo.initialization_attempt_id.as_deref() else {
            return Ok(());
        };
        let Some((_, target)) = pinned_identity(repo)? else {
            return Ok(());
        };
        let provenance = ActivationFenceProvenance {
            workspace: repo.repo_id.workspace.as_str().to_owned(),
            repo: repo.repo_id.path.clone(),
            branch: repo.initialization_branch.clone().unwrap_or_default(),
            target: target.to_owned(),
            attempt_id: attempt_id.to_owned(),
        };
        if let Some(fence) = self.scheduler.recover_activation_fence(&provenance).await? {
            self.scheduler
                .release_ready_publication_fence(fence)
                .await?;
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
        if !self.readiness().healthy {
            return AdmissionVerification::Retryable(
                "all admission verifier capacity is poisoned; replace the isolated verifier coordinator"
                    .into(),
            );
        }
        let permit = match tokio::time::timeout(
            self.verification_queue_timeout,
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
        let verification =
            tokio::spawn(async move { verifier.verify(&record, verify_cancel).await });
        let mut guard = VerificationTaskGuard {
            cancelled: cancelled.clone(),
            task: Some(verification),
            permit: Some(permit),
            poisoned_slots: self.poisoned_verifier_slots.clone(),
            armed: true,
        };
        let mut did_not_drain = false;
        let outcome = match tokio::time::timeout(
            self.verification_timeout,
            guard.task.as_mut().expect("verification task is owned"),
        )
        .await
        {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => AdmissionVerification::Retryable(if error.is_panic() {
                "admission verifier panicked".into()
            } else {
                format!("admission verifier task failed: {error}")
            }),
            Err(_) => {
                cancelled.cancel();
                match tokio::time::timeout(
                    self.verification_cancel_grace,
                    guard.task.as_mut().expect("verification task is owned"),
                )
                .await
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
            guard.poison();
        } else {
            guard.finish();
        }
        outcome
    }

    async fn retryable_outcome_if_current(
        &self,
        repo: &AddedRepo,
        target: &str,
        attempt_id: &str,
    ) -> Result<AdmissionOutcome> {
        if !self
            .attempt_is_current(&repo.repo_id, target, attempt_id)
            .await?
        {
            self.release_attempt_consumers(repo).await?;
            return self.reload_outcome(&repo.repo_id).await;
        }
        let readiness = self.readiness();
        Ok(if readiness.healthy {
            AdmissionOutcome::WaitingForArtifacts
        } else {
            AdmissionOutcome::Degraded(format!(
                "all {}/{} admission verifier slots are poisoned; replace the isolated verifier coordinator",
                readiness.poisoned_verifier_slots, readiness.verifier_slots
            ))
        })
    }

    async fn repair_corrupt(
        &self,
        repo: &AddedRepo,
        target: &str,
        record: &ArtifactRecord,
        error: String,
    ) -> Result<AdmissionOutcome> {
        if !self
            .attempt_is_current(
                &repo.repo_id,
                target,
                repo.initialization_attempt_id
                    .as_deref()
                    .unwrap_or_default(),
            )
            .await?
        {
            self.release_attempt_consumers(repo).await?;
            return self.reload_outcome(&repo.repo_id).await;
        }
        let detail = format!("{:?} publication is corrupt: {error}", record.key.kind);
        match self
            .scheduler
            .quarantine_ready(record.id, record.manifest.as_deref(), &detail)
            .await?
        {
            QuarantineOutcome::Requeued(_) | QuarantineOutcome::LostRace => {
                Ok(AdmissionOutcome::WaitingForArtifacts)
            }
            QuarantineOutcome::Exhausted => {
                self.fail_if_current(repo, target, format!("{detail}; repair retries exhausted"))
                    .await?;
                self.reload_outcome(&repo.repo_id).await
            }
        }
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
    const RELEASED_FENCE_SCHEMA_V2: &str = r#"
CREATE TABLE ready_publication_fence_sequence(id INTEGER PRIMARY KEY CHECK(id=1),generation INTEGER NOT NULL CHECK(generation>=0));
INSERT INTO ready_publication_fence_sequence(id,generation) VALUES(1,42);
CREATE TABLE ready_publication_fences(
 token TEXT PRIMARY KEY,generation INTEGER NOT NULL UNIQUE CHECK(generation>0),operation_id TEXT NOT NULL UNIQUE,
 expires_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('held','activation_unknown')),
 UNIQUE(token,generation));
CREATE TABLE ready_publication_fence_members(
 token TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),artifact_id INTEGER NOT NULL,manifest TEXT,
 PRIMARY KEY(token,artifact_id),
 FOREIGN KEY(token,generation) REFERENCES ready_publication_fences(token,generation) ON DELETE CASCADE,
 FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE);
"#;

    #[derive(Default)]
    struct TestVerifier {
        corrupt: Mutex<HashSet<String>>,
        transient: Mutex<HashSet<String>>,
        hang: Mutex<HashSet<String>>,
        wedged: Mutex<HashSet<String>>,
        outer_abort: Mutex<HashSet<String>>,
        panic_on: Mutex<HashSet<String>>,
        pause_before: Mutex<HashSet<String>>,
        pause_after: Mutex<HashSet<String>>,
        pause: AtomicBool,
        calls: AtomicUsize,
        active: AtomicUsize,
        entered: Notify,
        resume: Notify,
    }

    struct ActiveVerification<'a>(&'a AtomicUsize);
    impl Drop for ActiveVerification<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl AdmissionPublicationVerifier for TestVerifier {
        async fn verify(
            &self,
            record: &ArtifactRecord,
            cancelled: CancellationToken,
        ) -> AdmissionVerification {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveVerification(&self.active);
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
            if self.pause_before.lock().await.remove(manifest) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if self.hang.lock().await.contains(manifest) {
                cancelled.cancelled().await;
                return AdmissionVerification::Retryable("test verification timed out".into());
            }
            if self.wedged.lock().await.contains(manifest) {
                std::future::pending::<()>().await;
                unreachable!();
            }
            if self.outer_abort.lock().await.contains(manifest) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
                unreachable!();
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
            if self.pause_after.lock().await.remove(manifest) {
                self.entered.notify_one();
                self.resume.notified().await;
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

    struct ReplaceAfterSubscribe {
        refs: Arc<FileRefStore>,
        replacement: AddedRepo,
        replace_after: usize,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl TestSubscriptionHook for ReplaceAfterSubscribe {
        async fn subscribed(&self, _kind: ArtifactKind) {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.replace_after {
                self.refs
                    .remove_added_repo(&self.replacement.repo_id)
                    .await
                    .unwrap();
                self.refs.add_repo(&self.replacement).await.unwrap();
            }
        }
    }

    struct QuarantineAfterFence {
        scheduler: Arc<ArtifactScheduler>,
        record: ArtifactRecord,
        outcome: Mutex<Option<QuarantineOutcome>>,
    }

    struct LateCommitRefStore {
        inner: Arc<FileRefStore>,
        first: AtomicBool,
        activation_calls: AtomicUsize,
        allow_commit: Arc<Notify>,
        committed: Arc<Notify>,
    }

    #[async_trait]
    impl RefStore for LateCommitRefStore {
        async fn load(&self, id: &RepoId) -> Result<Option<crate::RefInfo>> {
            self.inner.load(id).await
        }
        async fn save(&self, id: &RepoId, info: &crate::RefInfo) -> Result<()> {
            self.inner.save(id, info).await
        }
        async fn list(&self) -> Result<Vec<RepoId>> {
            self.inner.list().await
        }
        async fn load_branch(&self, id: &RepoId, branch: &str) -> Result<Option<crate::RefInfo>> {
            self.inner.load_branch(id, branch).await
        }
        async fn save_branch(
            &self,
            id: &RepoId,
            branch: &str,
            info: &crate::RefInfo,
        ) -> Result<()> {
            self.inner.save_branch(id, branch, info).await
        }
        async fn update_build_status(
            &self,
            id: &RepoId,
            branch: &str,
            commit: &str,
            status: &str,
        ) -> Result<bool> {
            self.inner
                .update_build_status(id, branch, commit, status)
                .await
        }
        async fn touch_last_accessed_at(
            &self,
            id: &RepoId,
            branch: &str,
            commit: &str,
        ) -> Result<bool> {
            self.inner.touch_last_accessed_at(id, branch, commit).await
        }
        async fn list_branches(&self, id: &RepoId) -> Result<Vec<String>> {
            self.inner.list_branches(id).await
        }
        async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
            self.inner.add_repo(repo).await
        }
        async fn load_added_repo(&self, id: &RepoId) -> Result<Option<AddedRepo>> {
            self.inner.load_added_repo(id).await
        }
        async fn remove_added_repo(&self, id: &RepoId) -> Result<()> {
            self.inner.remove_added_repo(id).await
        }
        async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
            self.inner.list_added_repos().await
        }
        async fn activate_repo(
            &self,
            id: &RepoId,
            branch: &str,
            commit: &str,
            attempt: Option<&str>,
        ) -> Result<bool> {
            self.activation_calls.fetch_add(1, Ordering::SeqCst);
            if !self.first.swap(false, Ordering::SeqCst) {
                return self.inner.activate_repo(id, branch, commit, attempt).await;
            }
            let inner = self.inner.clone();
            let id = id.clone();
            let branch = branch.to_owned();
            let commit = commit.to_owned();
            let attempt = attempt.map(str::to_owned);
            let allow_commit = self.allow_commit.clone();
            let committed = self.committed.clone();
            tokio::spawn(async move {
                allow_commit.notified().await;
                inner
                    .activate_repo(&id, &branch, &commit, attempt.as_deref())
                    .await
                    .unwrap();
                committed.notify_one();
            });
            std::future::pending::<Result<bool>>().await
        }
    }

    #[async_trait]
    impl TestActivationFenceHook for QuarantineAfterFence {
        async fn fenced(&self) {
            let outcome = self
                .scheduler
                .quarantine_ready(
                    self.record.id,
                    self.record.manifest.as_deref(),
                    "concurrent scrub corruption",
                )
                .await
                .unwrap();
            *self.outcome.lock().await = Some(outcome);
        }
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
                    1,
                )
                .unwrap()
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

        fn provenance(&self, attempt: &str) -> ActivationFenceProvenance {
            ActivationFenceProvenance {
                workspace: self.repo_id.workspace.as_str().to_owned(),
                repo: self.repo_id.path.clone(),
                branch: "main".into(),
                target: TARGET.into(),
                attempt_id: attempt.into(),
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

        async fn make_unknown_fence(&self, attempt: &str) {
            let head = self
                .scheduler
                .get_by_key(&self.key(ArtifactKind::Head))
                .await
                .unwrap()
                .unwrap();
            let history = self
                .scheduler
                .get_by_key(&self.key(ArtifactKind::FullHistory))
                .await
                .unwrap()
                .unwrap();
            let fence = self
                .scheduler
                .fence_ready_publications(
                    &[
                        (head.id, head.manifest.clone()),
                        (history.id, history.manifest.clone()),
                    ],
                    &self.provenance(attempt),
                    60,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(
                self.scheduler
                    .mark_activation_unknown(&fence, 60)
                    .await
                    .unwrap()
            );
            drop(fence);
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
    async fn ready_but_corrupt_publication_is_quarantined_and_rebuilt() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "corrupt-history").await;
        f.verifier
            .corrupt
            .lock()
            .await
            .insert("corrupt-history".into());

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
        let quarantined = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(quarantined.state, ArtifactState::Queued);
        assert_eq!(quarantined.retry_count, 1);
        assert!(quarantined.manifest.is_none());

        f.verifier.corrupt.lock().await.clear();
        let claim = f
            .scheduler
            .claim("repair-worker", 30)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.record.key.kind, ArtifactKind::FullHistory);
        let evidence =
            CompletionEvidence::new(claim.record.key.clone(), "repaired-history").unwrap();
        assert!(
            f.scheduler
                .complete(&claim, "repair-worker", &evidence)
                .await
                .unwrap()
        );
        assert_eq!(
            f.coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Activated
        );
    }

    #[tokio::test]
    async fn repeated_corrupt_rebuilds_fail_only_after_repair_budget_exhausts() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "bad-history-0").await;
        let mut outcome = AdmissionOutcome::WaitingForArtifacts;
        for generation in 0..8 {
            let manifest = format!("bad-history-{generation}");
            f.verifier.corrupt.lock().await.insert(manifest);
            outcome = f.coordinator.reconcile_repo(&f.repo_id).await.unwrap();
            if matches!(outcome, AdmissionOutcome::Failed(_)) {
                break;
            }
            assert_eq!(outcome, AdmissionOutcome::WaitingForArtifacts);
            assert_eq!(
                f.refs
                    .load_added_repo(&f.repo_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                RepoLifecycleState::Initializing
            );
            let claim = f
                .scheduler
                .claim("repair-worker", 30)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(claim.record.key.kind, ArtifactKind::FullHistory);
            let next = format!("bad-history-{}", generation + 1);
            let evidence = CompletionEvidence::new(claim.record.key.clone(), next).unwrap();
            assert!(
                f.scheduler
                    .complete(&claim, "repair-worker", &evidence)
                    .await
                    .unwrap()
            );
        }
        assert!(matches!(
            outcome,
            AdmissionOutcome::Failed(message) if message.contains("repair retries exhausted")
        ));
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

    async fn replacement_after_subscription_cleans_exact_old_attempt(replace_after: usize) {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        let hook = Arc::new(ReplaceAfterSubscribe {
            refs: f.refs.clone(),
            replacement: f.repo("attempt-2"),
            replace_after,
            calls: AtomicUsize::new(0),
        });
        let coordinator = ArtifactAdmissionCoordinator::new(
            f.scheduler.clone(),
            f.refs.clone(),
            f.verifier.clone(),
            1,
        )
        .unwrap()
        .with_subscription_hook(hook.clone());
        assert_eq!(
            coordinator
                .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
                .await
                .unwrap(),
            AdmissionOutcome::StaleAttempt
        );
        assert_eq!(f.consumer_count().await, 0);
        assert_eq!(
            f.refs
                .load_added_repo(&f.repo_id)
                .await
                .unwrap()
                .unwrap()
                .initialization_attempt_id
                .as_deref(),
            Some("attempt-2")
        );
        assert_eq!(hook.calls.load(Ordering::SeqCst), replace_after);
        assert!(
            f.scheduler
                .get_by_key(&f.key(ArtifactKind::FullHistory))
                .await
                .unwrap()
                .is_none(),
            "released unobserved admission work must not survive replacement"
        );
    }

    #[tokio::test]
    async fn replacement_after_first_subscription_cleans_old_consumer() {
        replacement_after_subscription_cleans_exact_old_attempt(1).await;
    }

    #[tokio::test]
    async fn replacement_after_second_subscription_cleans_both_old_consumers() {
        replacement_after_subscription_cleans_exact_old_attempt(2).await;
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

    async fn retryable_verification_rechecks_replaced_attempt(paused_manifest: &str) {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        f.verifier
            .transient
            .lock()
            .await
            .insert(paused_manifest.into());
        f.verifier
            .pause_before
            .lock()
            .await
            .insert(paused_manifest.into());
        let coordinator = f.coordinator.clone();
        let repo_id = f.repo_id.clone();
        let task = tokio::spawn(async move { coordinator.reconcile_repo(&repo_id).await.unwrap() });
        f.verifier.entered.notified().await;
        f.refs.remove_added_repo(&f.repo_id).await.unwrap();
        f.refs.add_repo(&f.repo("attempt-2")).await.unwrap();
        f.verifier.resume.notify_one();

        assert_eq!(task.await.unwrap(), AdmissionOutcome::StaleAttempt);
        assert_eq!(f.consumer_count().await, 0);
        assert_eq!(
            f.refs
                .load_added_repo(&f.repo_id)
                .await
                .unwrap()
                .unwrap()
                .initialization_attempt_id
                .as_deref(),
            Some("attempt-2")
        );
    }

    #[tokio::test]
    async fn head_retryable_return_cleans_replaced_attempt() {
        retryable_verification_rechecks_replaced_attempt("head").await;
    }

    #[tokio::test]
    async fn history_retryable_return_cleans_replaced_attempt() {
        retryable_verification_rechecks_replaced_attempt("history").await;
    }

    #[tokio::test]
    async fn activation_fence_blocks_quarantine_until_lifecycle_cas_finishes() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let hook = Arc::new(QuarantineAfterFence {
            scheduler: f.scheduler.clone(),
            record: head.clone(),
            outcome: Mutex::new(None),
        });
        let coordinator = ArtifactAdmissionCoordinator::new(
            f.scheduler.clone(),
            f.refs.clone(),
            f.verifier.clone(),
            1,
        )
        .unwrap()
        .with_activation_fence_hook(hook.clone());
        assert_eq!(
            coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Activated
        );
        assert_eq!(
            *hook.outcome.lock().await,
            Some(QuarantineOutcome::LostRace)
        );
        // The fence is released only after activation is durable, so a later
        // scrub can withdraw the publication normally.
        assert!(matches!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "late scrub corruption")
                .await
                .unwrap(),
            QuarantineOutcome::Requeued(_)
        ));
    }

    #[tokio::test]
    async fn timed_out_activation_retains_fence_until_late_commit_is_observed() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let late = Arc::new(LateCommitRefStore {
            inner: f.refs.clone(),
            first: AtomicBool::new(true),
            activation_calls: AtomicUsize::new(0),
            allow_commit: Arc::new(Notify::new()),
            committed: Arc::new(Notify::new()),
        });
        let coordinator = ArtifactAdmissionCoordinator::new(
            f.scheduler.clone(),
            late.clone(),
            f.verifier.clone(),
            1,
        )
        .unwrap()
        .with_activation_timeout(Duration::from_millis(100));

        assert_eq!(
            coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::WaitingForArtifacts
        );
        assert_eq!(late.activation_calls.load(Ordering::SeqCst), 1);
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        let state: String = sqlx::query_scalar("SELECT state FROM ready_publication_fences")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(state, "activation_unknown");

        // Even beyond its liveness deadline, an unknown fence remains a safety
        // root and cannot be quarantined. Recovery renews the same exact
        // operation and retries its idempotent lifecycle CAS.
        sqlx::query("UPDATE ready_publication_fences SET expires_at=0")
            .execute(&pool)
            .await
            .unwrap();
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "past-deadline scrub")
                .await
                .unwrap(),
            QuarantineOutcome::LostRace
        );
        assert_eq!(
            coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Activated
        );
        assert_eq!(late.activation_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ready_publication_fences")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );

        // The original timed-out call commits after recovery settled the
        // fence. It is the same idempotent attempt and cannot demote/rewrite
        // the already Active lifecycle row.
        late.allow_commit.notify_one();
        late.committed.notified().await;
        assert_eq!(
            coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::AlreadyActive
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ready_publication_fences")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn expired_activation_fence_cannot_block_quarantine_after_crash() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let history = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        let fence = f
            .scheduler
            .fence_ready_publications(
                &[
                    (head.id, head.manifest.clone()),
                    (history.id, history.manifest.clone()),
                ],
                &f.provenance("crash-test"),
                60,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "scrub")
                .await
                .unwrap(),
            QuarantineOutcome::LostRace
        );
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        sqlx::query("UPDATE ready_publication_fences SET expires_at=0")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "scrub after crash")
                .await
                .unwrap(),
            QuarantineOutcome::Requeued(_)
        ));
        drop(fence);
    }

    #[tokio::test]
    async fn stale_fence_capability_cannot_delete_reacquired_generation() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let history = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        let expected = [
            (head.id, head.manifest.clone()),
            (history.id, history.manifest.clone()),
        ];
        let old = f
            .scheduler
            .fence_ready_publications(&expected, &f.provenance("aba-attempt"), 60)
            .await
            .unwrap()
            .unwrap();
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        sqlx::query("UPDATE ready_publication_fences SET expires_at=0")
            .execute(&pool)
            .await
            .unwrap();
        let renewed = f
            .scheduler
            .fence_ready_publications(&expected, &f.provenance("aba-attempt"), 60)
            .await
            .unwrap()
            .unwrap();
        f.scheduler
            .release_ready_publication_fence(old)
            .await
            .unwrap();
        assert_eq!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "scrub")
                .await
                .unwrap(),
            QuarantineOutcome::LostRace,
            "an old capability must not release a renewed generation"
        );
        f.scheduler
            .release_ready_publication_fence(renewed)
            .await
            .unwrap();
        assert!(matches!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "scrub")
                .await
                .unwrap(),
            QuarantineOutcome::Requeued(_)
        ));
    }

    #[tokio::test]
    async fn stale_recovery_capability_cannot_settle_reacquired_generation() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let history = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        let expected = [
            (head.id, head.manifest.clone()),
            (history.id, history.manifest.clone()),
        ];
        let provenance = f.provenance("recovery-aba");
        let acquired = f
            .scheduler
            .fence_ready_publications(&expected, &provenance, 60)
            .await
            .unwrap()
            .unwrap();
        assert!(
            f.scheduler
                .mark_activation_unknown(&acquired, 60)
                .await
                .unwrap()
        );
        drop(acquired);
        let stale = f
            .scheduler
            .recover_activation_fence(&provenance)
            .await
            .unwrap()
            .unwrap();
        let current = f
            .scheduler
            .recover_activation_fence(&provenance)
            .await
            .unwrap()
            .unwrap();
        f.scheduler
            .release_ready_publication_fence(current)
            .await
            .unwrap();
        let renewed = f
            .scheduler
            .fence_ready_publications(&expected, &provenance, 60)
            .await
            .unwrap()
            .unwrap();
        f.scheduler
            .release_ready_publication_fence(stale)
            .await
            .unwrap();
        assert_eq!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "scrub")
                .await
                .unwrap(),
            QuarantineOutcome::LostRace
        );
        f.scheduler
            .release_ready_publication_fence(renewed)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn forged_ordinary_consumer_names_cannot_block_quarantine() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        for forged in [
            "admission-activation-forged",
            "ADMISSION-ACTIVATION-FORGED",
            "Admission-Activation-Forged",
        ] {
            f.scheduler
                .subscribe_consumer(&head.key, forged, 60)
                .await
                .unwrap();
        }
        assert!(matches!(
            f.scheduler
                .quarantine_ready(head.id, head.manifest.as_deref(), "real scrub")
                .await
                .unwrap(),
            QuarantineOutcome::Requeued(_)
        ));
    }

    #[tokio::test]
    async fn startup_rejects_corrupt_fence_pair_membership() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let history = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        let _fence = f
            .scheduler
            .fence_ready_publications(
                &[
                    (head.id, head.manifest.clone()),
                    (history.id, history.manifest.clone()),
                ],
                &f.provenance("corrupt-startup"),
                60,
            )
            .await
            .unwrap()
            .unwrap();
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        sqlx::query("DELETE FROM ready_publication_fence_members WHERE artifact_id=?")
            .bind(history.id)
            .execute(&pool)
            .await
            .unwrap();
        let error = match ArtifactScheduler::open(&f.db_url, SchedulerLimits::default()).await {
            Ok(_) => panic!("corrupt fence membership was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("fence integrity"));
    }

    #[tokio::test]
    async fn startup_rejects_orphan_fence_membership() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.coordinator
            .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
            .await
            .unwrap();
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("PRAGMA foreign_keys=OFF")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO ready_publication_fence_members(token,generation,artifact_id,manifest)
             VALUES('orphan',99,?,'orphan-manifest')",
        )
        .bind(head.id)
        .execute(&mut *connection)
        .await
        .unwrap();
        drop(connection);
        let error = match ArtifactScheduler::open(&f.db_url, SchedulerLimits::default()).await {
            Ok(_) => panic!("orphan fence membership was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("fence integrity"));
    }

    #[tokio::test]
    async fn fence_acquisition_rejects_wrong_typed_pair_and_provenance() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let history = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        assert!(
            f.scheduler
                .fence_ready_publications(
                    &[
                        (head.id, head.manifest.clone()),
                        (head.id, head.manifest.clone())
                    ],
                    &f.provenance("attempt-1"),
                    60,
                )
                .await
                .is_err()
        );
        let mut wrong = f.provenance("attempt-1");
        wrong.workspace = "other-workspace".into();
        assert!(
            f.scheduler
                .fence_ready_publications(
                    &[
                        (head.id, head.manifest.clone()),
                        (history.id, history.manifest.clone())
                    ],
                    &wrong,
                    60,
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unknown_fence_recovery_settles_removed_and_replaced_attempts() {
        for replacement in [false, true] {
            let f = Fixture::new().await;
            f.add("attempt-1").await;
            f.make_required_ready("head", "history").await;
            f.make_unknown_fence("attempt-1").await;
            f.refs.remove_added_repo(&f.repo_id).await.unwrap();
            if replacement {
                f.refs.add_repo(&f.repo("attempt-2")).await.unwrap();
            }
            let page = f
                .coordinator
                .reconcile_unknown_fences_page(None, 16)
                .await
                .unwrap();
            assert_eq!(page.processed, 1);
            assert_eq!(page.settled, 1);
            assert_eq!(page.retained, 0);
            let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ready_publication_fences")
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0
            );
        }
    }

    #[tokio::test]
    async fn unknown_fence_listing_is_bounded_and_cursor_monotonic() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let history = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        let expected = [
            (head.id, head.manifest.clone()),
            (history.id, history.manifest.clone()),
        ];
        for attempt in ["page-a", "page-b", "page-c"] {
            let fence = f
                .scheduler
                .fence_ready_publications(&expected, &f.provenance(attempt), 60)
                .await
                .unwrap()
                .unwrap();
            assert!(
                f.scheduler
                    .mark_activation_unknown(&fence, 60)
                    .await
                    .unwrap()
            );
        }
        let first = f
            .scheduler
            .unknown_activation_fences_page(None, 2)
            .await
            .unwrap();
        assert_eq!(first.fences.len(), 2);
        let cursor = first.next_generation.expect("full page has a cursor");
        let second = f
            .scheduler
            .unknown_activation_fences_page(Some(cursor), 2)
            .await
            .unwrap();
        assert_eq!(second.fences.len(), 1);
        assert!(second.next_generation.is_none());
        assert!(
            f.scheduler
                .unknown_activation_fences_page(None, 129)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn released_v2_migrates_atomically_to_union_v5_without_changing_jobs() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.coordinator
            .subscribe_pinned_attempt(&f.repo_id, "attempt-1")
            .await
            .unwrap();
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        sqlx::raw_sql(
            "DROP TABLE ready_publication_fence_members;
             DROP TABLE ready_publication_fences;
             DROP TABLE ready_publication_fence_sequence;
             DROP TABLE artifact_base_retention;
             DROP TABLE artifact_gc_sweep;
             DROP TABLE artifact_transport_leases;
             PRAGMA user_version=2;",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(RELEASED_FENCE_SCHEMA_V2)
            .execute(&pool)
            .await
            .unwrap();
        let jobs_before: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact_jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        let reopened = ArtifactScheduler::open(&f.db_url, SchedulerLimits::default())
            .await
            .unwrap();
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 5);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT generation FROM ready_publication_fence_sequence WHERE id=1",
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            42
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact_jobs")
                .fetch_one(&pool)
                .await
                .unwrap(),
            jobs_before
        );
        assert!(
            reopened
                .unknown_activation_fences_page(None, 1)
                .await
                .unwrap()
                .fences
                .is_empty()
        );
        let rolling_peer = ArtifactScheduler::open(&f.db_url, SchedulerLimits::default())
            .await
            .unwrap();
        assert!(
            rolling_peer
                .unknown_activation_fences_page(None, 1)
                .await
                .unwrap()
                .fences
                .is_empty()
        );
    }

    #[tokio::test]
    async fn mixed_v2_fence_schema_rolls_back_without_partial_v5_mutation() {
        let f = Fixture::new().await;
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        sqlx::raw_sql(
            "DROP TABLE ready_publication_fence_members;
             DROP TABLE ready_publication_fences;
             DROP TABLE ready_publication_fence_sequence;
             DROP TABLE artifact_base_retention;
             DROP TABLE artifact_gc_sweep;
             DROP TABLE artifact_transport_leases;
             CREATE TABLE ready_publication_fences(planted TEXT NOT NULL);
             PRAGMA user_version=2;",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            ArtifactScheduler::open(&f.db_url, SchedulerLimits::default())
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert_eq!(sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pragma_table_info('ready_publication_fences') WHERE name='planted'"
        ).fetch_one(&pool).await.unwrap(), 1);
        assert_eq!(sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ready_publication_fence_sequence'"
        ).fetch_one(&pool).await.unwrap(), 0, "failed v5 migration leaked an earlier DDL statement");
    }

    #[tokio::test]
    async fn live_released_v2_unknown_fence_fails_closed_and_rolls_back_unchanged() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("head", "history").await;
        let head = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::Head))
            .await
            .unwrap()
            .unwrap();
        let history = f
            .scheduler
            .get_by_key(&f.key(ArtifactKind::FullHistory))
            .await
            .unwrap()
            .unwrap();
        let pool = sqlx::SqlitePool::connect(&f.db_url).await.unwrap();
        sqlx::raw_sql(
            "DROP TABLE ready_publication_fence_members;
             DROP TABLE ready_publication_fences;
             DROP TABLE ready_publication_fence_sequence;
             DROP TABLE artifact_base_retention;
             DROP TABLE artifact_gc_sweep;
             DROP TABLE artifact_transport_leases;
             PRAGMA user_version=2;",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(RELEASED_FENCE_SCHEMA_V2)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO ready_publication_fences(token,generation,operation_id,expires_at,state)
             VALUES('legacy-live',42,'legacy-operation',9999999999,'activation_unknown')",
        )
        .execute(&pool)
        .await
        .unwrap();
        for record in [&head, &history] {
            sqlx::query(
                "INSERT INTO ready_publication_fence_members(token,generation,artifact_id,manifest)
                 VALUES('legacy-live',42,?,?)",
            )
            .bind(record.id)
            .bind(&record.manifest)
            .execute(&pool)
            .await
            .unwrap();
        }
        let error = match ArtifactScheduler::open(&f.db_url, SchedulerLimits::default()).await {
            Ok(_) => panic!("live provenance-free v2 fence was unsafely migrated"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("drain them with the v2 binary"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ready_publication_fences")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ready_publication_fence_members")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT generation FROM ready_publication_fence_sequence")
                .fetch_one(&pool)
                .await
                .unwrap(),
            42
        );
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
    async fn coordinator_requires_explicit_nonzero_format_version() {
        let f = Fixture::new().await;
        assert!(
            ArtifactAdmissionCoordinator::new(
                f.scheduler.clone(),
                f.refs.clone(),
                f.verifier.clone(),
                0,
            )
            .is_err()
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
            1,
        )
        .unwrap()
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

    #[tokio::test]
    async fn all_non_draining_slots_trip_observable_circuit_immediately() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("wedged-one", "history-one").await;
        let second = RepoId {
            workspace: WorkspaceId::new("workspace-a"),
            path: "owner/second-wedged".into(),
        };
        f.refs
            .add_repo(&f.repo_for(&second, "attempt-2"))
            .await
            .unwrap();
        f.make_ready_for(&second, "attempt-2", "wedged-two", "history-two")
            .await;
        f.verifier
            .wedged
            .lock()
            .await
            .extend(["wedged-one".into(), "wedged-two".into()]);
        let coordinator = ArtifactAdmissionCoordinator::new(
            f.scheduler.clone(),
            f.refs.clone(),
            f.verifier.clone(),
            1,
        )
        .unwrap()
        .with_verification_policy(
            2,
            Duration::from_millis(100),
            Duration::from_millis(100),
        );

        let outcomes = coordinator.reconcile_all().await.unwrap();
        assert_eq!(outcomes.len(), 2);
        let readiness = coordinator.readiness();
        assert!(!readiness.healthy);
        assert_eq!(readiness.poisoned_verifier_slots, 2);
        assert_eq!(readiness.verifier_slots, 2);

        let started = std::time::Instant::now();
        assert!(matches!(
            coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Degraded(message) if message.contains("replace")
        ));
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn repeated_outer_abort_cannot_detach_hostile_verifiers() {
        let f = Fixture::new().await;
        f.add("attempt-1").await;
        f.make_required_ready("outer-abort", "history").await;
        f.verifier
            .outer_abort
            .lock()
            .await
            .insert("outer-abort".into());
        let coordinator = Arc::new(
            ArtifactAdmissionCoordinator::new(
                f.scheduler.clone(),
                f.refs.clone(),
                f.verifier.clone(),
                1,
            )
            .unwrap()
            .with_verification_policy(
                2,
                Duration::from_secs(10),
                Duration::from_secs(1),
            ),
        );
        for poisoned in 1..=2 {
            let running_coordinator = coordinator.clone();
            let repo_id = f.repo_id.clone();
            let task =
                tokio::spawn(async move { running_coordinator.reconcile_repo(&repo_id).await });
            f.verifier.entered.notified().await;
            task.abort();
            let _ = task.await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while f.verifier.active.load(Ordering::SeqCst) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(coordinator.readiness().poisoned_verifier_slots, poisoned);
        }
        assert!(!coordinator.readiness().healthy);
        assert_eq!(f.verifier.active.load(Ordering::SeqCst), 0);
        assert!(matches!(
            coordinator.reconcile_repo(&f.repo_id).await.unwrap(),
            AdmissionOutcome::Degraded(_)
        ));
        assert_eq!(f.verifier.active.load(Ordering::SeqCst), 0);
    }
}
