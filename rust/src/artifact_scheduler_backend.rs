//! Backend contract for the normalized artifact scheduler state machine.
//!
//! Each mutating method is one database transaction. Implementations must not
//! split admission, generation checks, claim-cap accounting, or publication
//! alias updates across transactions.
//!
//! Schema v4 is an intentionally non-rolling safety boundary: every v3 process
//! must be drained before the first v4 process migrates the database. Migration
//! locks serialize v4 starters, and the v4 marker makes subsequent v3 startups
//! reject the database, but no database marker can revoke an already-running v3
//! process that lacks the GC publication fence.

#[cfg(test)]
use crate::artifact_scheduler::ArtifactTask;
use crate::artifact_scheduler::{
    ActivationFenceProvenance, ArtifactKey, ArtifactKind, ArtifactRecord, ClaimedArtifact,
    CompletionEvidence, CompletionSealAuthority, ExecutionContext, ExecutionOutcome, FailureClass,
    ObservationOutcome, ObservationSnapshot, QuarantineOutcome, ReadyPublicationFence,
    RetryOutcome, ScheduleOutcome, UnknownActivationFencePage, VerifiedCompletionEvidence,
    validate_evidence, validate_lease,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub type OwnedArtifactBuildFuture =
    Pin<Box<dyn Future<Output = Result<CompletionEvidence>> + Send + 'static>>;

/// The primary unit of scheduler-owned artifact work.
///
/// The closure is not invoked until the persistence backend confirms the
/// claim is still owned. Implementations must await every process or child
/// task they start before returning so cancellation can drain the whole
/// attempt before a successor is allowed to publish.
pub struct OwnedArtifactBuild(
    Box<dyn FnOnce(ExecutionContext) -> OwnedArtifactBuildFuture + Send + 'static>,
);

impl OwnedArtifactBuild {
    pub fn cooperative<F, Fut>(build: F) -> Self
    where
        F: FnOnce(ExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<CompletionEvidence>> + Send + 'static,
    {
        Self(Box::new(move |context| Box::pin(build(context))))
    }

    /// Adapt synchronous artifact construction without blocking Tokio's lease
    /// heartbeat driver. The blocking child is always awaited, so cancellation
    /// and attempt completion retain a single drain boundary.
    pub fn blocking<F>(build: F) -> Self
    where
        F: FnOnce(ExecutionContext) -> Result<CompletionEvidence> + Send + 'static,
    {
        Self::cooperative(move |context| async move {
            tokio::task::spawn_blocking(move || build(context))
                .await
                .context("owned blocking artifact build did not join")?
        })
    }

    fn start(self, context: ExecutionContext) -> OwnedArtifactBuildFuture {
        (self.0)(context)
    }
}

/// Durable request-scoped capability root. The canonical root manifest owns
/// the delegated child list; persistence stores only identity and expiry so
/// GC and authorization never diverge from authenticated manifest semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportRootLease {
    pub root_hash: String,
    pub session_id: String,
    pub workspace: String,
    pub repo: String,
    pub expires_at: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerGcRoot {
    pub artifact_id: i64,
    pub key: ArtifactKey,
    pub manifest: String,
}
pub const TRANSPORT_ROOT_PAGE_MAX: u32 = 512;

#[async_trait]
pub trait GcDeleteFence: Send {
    /// Release the database transaction/advisory lock held across one bounded
    /// external delete call. Dropping an implementation must also roll back.
    async fn release(self: Box<Self>) -> Result<()>;
}

pub(crate) fn validate_transport_lease_identity(
    root_hash: &str,
    session_id: &str,
    workspace: &str,
    repo: &str,
    ttl_secs: i64,
) -> Result<()> {
    crate::cas::Cas::validate_artifact_id(root_hash)?;
    if session_id.len() != 64
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || workspace.trim().is_empty()
        || repo.trim().is_empty()
        || ttl_secs <= 0
        || ttl_secs > 24 * 60 * 60
    {
        bail!("transport root lease identity or TTL is invalid");
    }
    Ok(())
}

struct PersistenceExecutionGuard {
    cancel: CancellationToken,
    scratch: PathBuf,
    armed: bool,
}
impl Drop for PersistenceExecutionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancel.cancel();
            let aborted = self.scratch.with_extension("aborted");
            let _ = std::fs::rename(&self.scratch, aborted);
        }
    }
}

#[async_trait]
pub trait ArtifactSchedulerPersistence: Send + Sync {
    fn completion_verifier(&self) -> Arc<dyn crate::artifact_scheduler::CompletionVerifier>;
    fn completion_sealer(&self) -> Arc<CompletionSealAuthority>;
    /// Admission must fail startup rather than discover after corruption that
    /// a backend cannot withdraw a Ready manifest atomically.
    fn full_admission_recovery_protocol_supported(&self) -> bool {
        false
    }

    /// Acquire the fleet-wide deletion fence. Root-creating mutations must
    /// reject while another live owner holds it. The database clock is the
    /// authority for expiry so a crashed collector cannot deadlock syncs.
    async fn acquire_gc_sweep(&self, owner: &str, ttl_secs: i64) -> Result<bool> {
        let _ = (owner, ttl_secs);
        bail!("GC sweep fencing is not implemented by this scheduler backend")
    }
    async fn renew_gc_sweep(&self, owner: &str, ttl_secs: i64) -> Result<bool> {
        let _ = (owner, ttl_secs);
        bail!("GC sweep fencing is not implemented by this scheduler backend")
    }
    async fn release_gc_sweep(&self, owner: &str) -> Result<()> {
        let _ = owner;
        bail!("GC sweep fencing is not implemented by this scheduler backend")
    }
    async fn lock_gc_delete_batch(&self, owner: &str) -> Result<Box<dyn GcDeleteFence>> {
        let _ = owner;
        bail!("transactional GC delete fencing is not implemented by this scheduler backend")
    }
    async fn register_transport_root(
        &self,
        root_hash: &str,
        session_id: &str,
        workspace: &str,
        repo: &str,
        ttl_secs: i64,
    ) -> Result<()> {
        let _ = (root_hash, session_id, workspace, repo, ttl_secs);
        bail!("transport root leases are not implemented by this scheduler backend")
    }
    /// Renew only an already-issued exact root/session/repository capability.
    /// Returns false for an unknown, expired, or cross-repository session.
    async fn renew_transport_root(
        &self,
        root_hash: &str,
        session_id: &str,
        workspace: &str,
        repo: &str,
        ttl_secs: i64,
    ) -> Result<bool> {
        let _ = (root_hash, session_id, workspace, repo, ttl_secs);
        bail!("transport root leases are not implemented by this scheduler backend")
    }
    async fn release_transport_root(
        &self,
        root_hash: &str,
        session_id: &str,
        workspace: &str,
        repo: &str,
    ) -> Result<bool> {
        let _ = (root_hash, session_id, workspace, repo);
        bail!("transport root leases are not implemented by this scheduler backend")
    }
    async fn live_transport_roots_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<TransportRootLease>> {
        let _ = (after, limit);
        bail!("transport root leases are not implemented by this scheduler backend")
    }
    async fn live_scheduler_roots_page(
        &self,
        after_artifact_id: Option<i64>,
        limit: u32,
    ) -> Result<Vec<SchedulerGcRoot>> {
        let _ = (after_artifact_id, limit);
        bail!("scheduler GC roots are not implemented by this scheduler backend")
    }
    async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome>;
    async fn subscribe_consumer(
        &self,
        key: &ArtifactKey,
        consumer_id: &str,
        ttl_secs: i64,
    ) -> Result<ScheduleOutcome>;
    /// Release only after the operation that consumes this artifact has durably
    /// settled its own publication. In particular, builders must keep a live
    /// consumer for every reused old base/level across child upload, root upload,
    /// verification, and `complete`; fresh-output grace does not protect reused
    /// old objects in the upload-to-completion gap.
    async fn release_consumer(&self, artifact_id: i64, consumer_id: &str) -> Result<()>;
    /// Atomically publish a resolved upstream tip using the generation returned
    /// by [`Self::observation_snapshot`]. Implementations must return
    /// `Unchanged` before the CAS check for an identical, fully-observed tip.
    async fn observe(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
        commit: &str,
        kinds: &[ArtifactKind],
        format_version: u32,
        expected_generation: Option<u64>,
    ) -> Result<ObservationOutcome>;
    /// Snapshot the branch commit and generation before resolving upstream.
    /// The later `observe` CAS fences concurrent fetches and force pushes.
    async fn observation_snapshot(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<ObservationSnapshot>;
    /// Safe production observation entry point. The snapshot carries its
    /// workspace/repo/branch identity, preventing a generation token from one
    /// branch being accidentally replayed against another.
    async fn observe_if_changed(
        &self,
        snapshot: &ObservationSnapshot,
        resolved_commit: &str,
        kinds: &[ArtifactKind],
        format_version: u32,
    ) -> Result<ObservationOutcome> {
        crate::artifact_scheduler::validate_canonical_commit_oid(resolved_commit)?;
        self.observe(
            snapshot.workspace(),
            snapshot.repo(),
            snapshot.branch(),
            resolved_commit,
            kinds,
            format_version,
            snapshot.generation(),
        )
        .await
    }
    async fn retry_failed(&self, key: &ArtifactKey) -> Result<RetryOutcome>;
    /// Manifest-CAS withdrawal of a corrupt Ready publication. Backends must
    /// clear publication aliases in the same transaction.
    async fn quarantine_ready(
        &self,
        id: i64,
        expected_manifest: Option<&str>,
        error: &str,
    ) -> Result<QuarantineOutcome> {
        let _ = (id, expected_manifest, error);
        bail!("artifact scheduler backend does not implement manifest-CAS quarantine")
    }
    async fn fence_ready_publications(
        &self,
        expected: &[(i64, Option<String>)],
        provenance: &ActivationFenceProvenance,
        ttl_secs: i64,
    ) -> Result<Option<ReadyPublicationFence>> {
        let _ = (expected, provenance, ttl_secs);
        bail!("artifact scheduler backend does not implement Ready publication fencing")
    }
    async fn release_ready_publication_fence(&self, fence: ReadyPublicationFence) -> Result<()> {
        let _ = fence;
        bail!("artifact scheduler backend does not implement Ready publication fencing")
    }
    async fn mark_activation_unknown(
        &self,
        fence: &ReadyPublicationFence,
        ttl_secs: i64,
    ) -> Result<bool> {
        let _ = (fence, ttl_secs);
        bail!("artifact scheduler backend does not implement activation recovery fencing")
    }
    async fn recover_activation_fence(
        &self,
        provenance: &ActivationFenceProvenance,
    ) -> Result<Option<ReadyPublicationFence>> {
        let _ = provenance;
        bail!("artifact scheduler backend does not implement activation recovery lookup")
    }
    async fn unknown_activation_fences_page(
        &self,
        after_generation: Option<u64>,
        limit: usize,
    ) -> Result<UnknownActivationFencePage> {
        let _ = (after_generation, limit);
        bail!("artifact scheduler backend does not implement bounded activation recovery listing")
    }
    async fn claim(&self, owner: &str, lease_secs: i64) -> Result<Option<ClaimedArtifact>>;
    async fn heartbeat(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        lease_secs: i64,
    ) -> Result<bool>;
    async fn owns(&self, claim: &ClaimedArtifact, owner: &str) -> Result<bool>;
    async fn complete_verified(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        evidence: &VerifiedCompletionEvidence,
    ) -> Result<bool>;
    /// Compatibility entrypoint for callers that hold raw builder output. Raw
    /// evidence is always verified by this scheduler's verifier, sealed for the
    /// exact claimed lease, and only then passed to DB-only settlement.
    #[cfg(test)]
    async fn complete(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        evidence: &CompletionEvidence,
    ) -> Result<bool> {
        validate_evidence(claim, evidence)?;
        let verifier = self.completion_verifier();
        verifier.verify(claim, evidence)?;
        let verified = self.completion_sealer().seal(claim, evidence.clone())?;
        self.complete_verified(claim, owner, &verified).await
    }
    async fn fail(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        class: FailureClass,
        error: &str,
    ) -> Result<bool>;
    async fn reconcile_expired(&self) -> Result<(u64, u64)>;
    async fn get(&self, id: i64) -> Result<Option<ArtifactRecord>>;
    async fn get_by_key(&self, key: &ArtifactKey) -> Result<Option<ArtifactRecord>>;
    /// Restartable maintenance scan ordered by durable job id.
    async fn ready_page(&self, after_id: i64, limit: usize) -> Result<Vec<ArtifactRecord>>;
    async fn ready_candidates(
        &self,
        workspace: &str,
        repo: &str,
        kind: ArtifactKind,
        format_version: u32,
        limit: u32,
    ) -> Result<Vec<ArtifactRecord>>;
    async fn published(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
        kind: ArtifactKind,
        format_version: u32,
    ) -> Result<Option<ArtifactRecord>>;
    /// Bounded newest-first immutable commits for which both Head and
    /// FullHistory are Ready, independent of mutable per-kind aliases.
    async fn complete_full_base_candidates(
        &self,
        workspace: &str,
        repo: &str,
        format_version: u32,
        limit: u32,
    ) -> Result<Vec<String>>;
    async fn quarantine_publication(
        &self,
        key: &ArtifactKey,
        expected_manifest: &str,
        reason: &str,
    ) -> Result<bool>;
    async fn counts(
        &self,
    ) -> Result<Vec<(ArtifactKind, crate::artifact_scheduler::ArtifactState, u64)>>;

    /// Run one evidence-producing artifact build under a fenced database
    /// lease. Ownership is checked before `build` is invoked. The lease is
    /// heartbeated while it runs, and the exact evidence returned by this
    /// owned attempt is passed to the backend's fenced `complete` operation.
    async fn run_owned_build(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        build: OwnedArtifactBuild,
        lease_secs: i64,
        scratch_root: &Path,
    ) -> Result<ExecutionOutcome> {
        validate_lease(owner, lease_secs)?;
        if !self.owns(claim, owner).await? {
            bail!("artifact lease is not currently owned")
        }

        let scratch = scratch_root.join(format!(
            "artifact-{}-lease-{}",
            claim.record.id, claim.record.lease_generation
        ));
        std::fs::create_dir(&scratch)
            .with_context(|| format!("create fenced scratch {}", scratch.display()))?;
        let cancel = CancellationToken::new();
        let mut guard = PersistenceExecutionGuard {
            cancel: cancel.clone(),
            scratch: scratch.clone(),
            armed: true,
        };
        let context = ExecutionContext {
            cancelled: cancel.clone(),
            scratch: scratch.clone(),
        };
        // Spawn only after the ownership preflight above. The join handle also
        // turns a build panic into an ordinary failed attempt we can drain.
        let mut running = tokio::spawn(build.start(context));
        let mut interval =
            tokio::time::interval(Duration::from_secs((lease_secs / 3).max(1) as u64));
        interval.tick().await;

        let build_result = loop {
            tokio::select! {
                result = &mut running => break Some(result),
                _ = interval.tick() => match self.heartbeat(claim, owner, lease_secs).await {
                    Ok(true) => {}
                    Ok(false) => break None,
                    Err(error) => {
                        cancel.cancel();
                        let _ = running.await;
                        let _ = std::fs::remove_dir_all(&scratch);
                        guard.armed = false;
                        return Err(error).context("artifact heartbeat failed after draining build");
                    }
                }
            }
        };

        let Some(build_result) = build_result else {
            cancel.cancel();
            let _ = running.await;
            let _ = std::fs::remove_dir_all(&scratch);
            guard.armed = false;
            return Ok(ExecutionOutcome::LostLease);
        };

        let evidence = match build_result {
            Ok(Ok(evidence)) => evidence,
            Ok(Err(error)) => {
                cancel.cancel();
                let message = error.to_string();
                let _ = std::fs::remove_dir_all(&scratch);
                if self.owns(claim, owner).await? {
                    let failed = self
                        .fail(claim, owner, FailureClass::Retryable, &message)
                        .await?;
                    guard.armed = false;
                    return Ok(if failed {
                        ExecutionOutcome::Failed
                    } else {
                        ExecutionOutcome::LostLease
                    });
                }
                guard.armed = false;
                return Ok(ExecutionOutcome::LostLease);
            }
            Err(error) => {
                cancel.cancel();
                let message = if error.is_panic() {
                    "artifact build panicked".to_owned()
                } else {
                    format!("artifact build cancelled: {error}")
                };
                let _ = std::fs::remove_dir_all(&scratch);
                if self.owns(claim, owner).await? {
                    let failed = self
                        .fail(claim, owner, FailureClass::Retryable, &message)
                        .await?;
                    guard.armed = false;
                    return Ok(if failed {
                        ExecutionOutcome::Failed
                    } else {
                        ExecutionOutcome::LostLease
                    });
                }
                guard.armed = false;
                return Ok(ExecutionOutcome::LostLease);
            }
        };

        if let Err(error) = validate_evidence(claim, &evidence) {
            cancel.cancel();
            let message = format!("artifact build returned invalid completion evidence: {error}");
            let _ = std::fs::remove_dir_all(&scratch);
            let outcome = fail_still_owned(self, claim, owner, &message).await?;
            guard.armed = false;
            return Ok(outcome);
        }
        // Verification is part of the owned operation, not publication. Run it
        // on the blocking pool while the same lease heartbeat/cancellation loop
        // remains active; only then stamp this in-memory receipt so `complete`
        // performs the DB-only fenced transition.
        let verifier = self.completion_verifier();
        let verifying_verifier = verifier.clone();
        let verify_claim = claim.clone();
        let verify_evidence = evidence.clone();
        let verify_context = ExecutionContext {
            cancelled: cancel.clone(),
            scratch: scratch.clone(),
        };
        let mut verifying = tokio::task::spawn_blocking(move || {
            verifying_verifier.verify_owned(&verify_claim, &verify_evidence, &verify_context)
        });
        let verification = loop {
            tokio::select! {
                result = &mut verifying => break Some(result),
                _ = interval.tick() => match self.heartbeat(claim, owner, lease_secs).await {
                    Ok(true) => {}
                    Ok(false) => break None,
                    Err(error) => {
                        cancel.cancel();
                        let _ = verifying.await;
                        let _ = std::fs::remove_dir_all(&scratch);
                        guard.armed = false;
                        return Err(error).context("artifact heartbeat failed after draining verifier");
                    }
                }
            }
        };
        let Some(verification) = verification else {
            cancel.cancel();
            let _ = verifying.await;
            let _ = std::fs::remove_dir_all(&scratch);
            guard.armed = false;
            return Ok(ExecutionOutcome::LostLease);
        };
        let verification_error = match verification {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.to_string()),
            Err(error) => Some(if error.is_panic() {
                "artifact verifier panicked".to_owned()
            } else {
                format!("artifact verifier cancelled: {error}")
            }),
        };
        if let Some(error) = verification_error {
            cancel.cancel();
            let message = format!("artifact completion evidence was rejected: {error}");
            let _ = std::fs::remove_dir_all(&scratch);
            let outcome = fail_still_owned(self, claim, owner, &message).await?;
            guard.armed = false;
            return Ok(outcome);
        }
        // The semantic verifier has accepted the exact evidence, but Ready is
        // still forbidden until all children are durable and the root manifest
        // has been published last. Keep heartbeating this same lease while the
        // verifier's storage policy performs and verifies that publication.
        let publish_context = ExecutionContext {
            cancelled: cancel.clone(),
            scratch: scratch.clone(),
        };
        let mut publishing = verifier.publish_owned(claim, &evidence, &publish_context);
        let publication = loop {
            tokio::select! {
                result = &mut publishing => break Some(result),
                _ = interval.tick() => match self.heartbeat(claim, owner, lease_secs).await {
                    Ok(true) => {}
                    Ok(false) => break None,
                    Err(error) => {
                        cancel.cancel();
                        let _ = publishing.await;
                        let _ = std::fs::remove_dir_all(&scratch);
                        guard.armed = false;
                        return Err(error).context("artifact heartbeat failed after draining publisher");
                    }
                }
            }
        };
        let Some(publication) = publication else {
            cancel.cancel();
            let _ = publishing.await;
            let _ = std::fs::remove_dir_all(&scratch);
            guard.armed = false;
            return Ok(ExecutionOutcome::LostLease);
        };
        if let Err(error) = publication {
            cancel.cancel();
            let message = format!("artifact durable publication failed: {error}");
            let _ = std::fs::remove_dir_all(&scratch);
            let outcome = fail_still_owned(self, claim, owner, &message).await?;
            guard.armed = false;
            return Ok(outcome);
        }
        drop(publishing);
        let verified = self.completion_sealer().seal(claim, evidence)?;
        let ready = match self.complete_verified(claim, owner, &verified).await {
            Ok(ready) => ready,
            Err(error) => {
                cancel.cancel();
                let message = format!("artifact completion evidence was rejected: {error}");
                let _ = std::fs::remove_dir_all(&scratch);
                let outcome = fail_still_owned(self, claim, owner, &message).await?;
                guard.armed = false;
                return Ok(outcome);
            }
        };
        let _ = std::fs::remove_dir_all(&scratch);
        guard.armed = false;
        Ok(if ready {
            ExecutionOutcome::Ready
        } else {
            ExecutionOutcome::LostLease
        })
    }

    /// Backend-independent worker protocol. Persistence implementations only
    /// provide fenced primitives; this method guarantees ownership preflight,
    /// internal heartbeats, cooperative cancellation, child draining, and
    /// attempt-unique scratch before any backend can publish.
    #[cfg(test)]
    async fn run_owned(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        tasks: Vec<ArtifactTask>,
        evidence: CompletionEvidence,
        lease_secs: i64,
        scratch_root: &Path,
    ) -> Result<ExecutionOutcome> {
        validate_lease(owner, lease_secs)?;
        validate_evidence(claim, &evidence)?;
        if !self.owns(claim, owner).await? {
            bail!("artifact lease is not currently owned")
        }
        let scratch = scratch_root.join(format!(
            "artifact-{}-lease-{}",
            claim.record.id, claim.record.lease_generation
        ));
        std::fs::create_dir(&scratch)
            .with_context(|| format!("create fenced scratch {}", scratch.display()))?;
        let cancel = CancellationToken::new();
        let mut guard = PersistenceExecutionGuard {
            cancel: cancel.clone(),
            scratch: scratch.clone(),
            armed: true,
        };
        let mut set = tokio::task::JoinSet::new();
        for task in tasks {
            set.spawn(task.start(ExecutionContext {
                cancelled: cancel.clone(),
                scratch: scratch.clone(),
            }));
        }
        let mut interval =
            tokio::time::interval(Duration::from_secs((lease_secs / 3).max(1) as u64));
        interval.tick().await;
        let mut failure = None;
        let mut heartbeat_error = None;
        while !set.is_empty() {
            tokio::select! {
                joined=set.join_next()=>if let Some(result)=joined {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => { failure=Some(error.to_string()); break }
                        Err(error) => {
                            failure=Some(if error.is_panic() {
                                "artifact child panicked".into()
                            } else {
                                format!("artifact child cancelled: {error}")
                            });
                            break
                        }
                    }
                },
                _=interval.tick()=>match self.heartbeat(claim,owner,lease_secs).await {
                    Ok(true) => {}
                    Ok(false) => { failure=Some("artifact lease lost".into()); break }
                    Err(error) => {
                        failure=Some("artifact heartbeat failed".into());
                        heartbeat_error=Some(error);
                        break
                    }
                }
            }
        }
        if let Some(error) = failure {
            cancel.cancel();
            while set.join_next().await.is_some() {}
            let _ = std::fs::remove_dir_all(&scratch);
            if let Some(heartbeat_error) = heartbeat_error {
                return Err(heartbeat_error)
                    .context("artifact heartbeat failed after draining children");
            }
            if self.owns(claim, owner).await? {
                let failed = self
                    .fail(claim, owner, FailureClass::Retryable, &error)
                    .await?;
                guard.armed = false;
                return Ok(if failed {
                    ExecutionOutcome::Failed
                } else {
                    ExecutionOutcome::LostLease
                });
            }
            guard.armed = false;
            return Ok(ExecutionOutcome::LostLease);
        }
        let ready = self.complete(claim, owner, &evidence).await?;
        let _ = std::fs::remove_dir_all(&scratch);
        guard.armed = false;
        Ok(if ready {
            ExecutionOutcome::Ready
        } else {
            ExecutionOutcome::LostLease
        })
    }
}

async fn fail_still_owned<P: ArtifactSchedulerPersistence + ?Sized>(
    persistence: &P,
    claim: &ClaimedArtifact,
    owner: &str,
    message: &str,
) -> Result<ExecutionOutcome> {
    if !persistence.owns(claim, owner).await? {
        return Ok(ExecutionOutcome::LostLease);
    }
    Ok(
        if persistence
            .fail(claim, owner, FailureClass::Retryable, message)
            .await?
        {
            ExecutionOutcome::Failed
        } else {
            ExecutionOutcome::LostLease
        },
    )
}

#[async_trait]
impl ArtifactSchedulerPersistence for crate::artifact_scheduler::ArtifactScheduler {
    fn completion_verifier(&self) -> Arc<dyn crate::artifact_scheduler::CompletionVerifier> {
        self.verifier.clone()
    }
    fn completion_sealer(&self) -> Arc<CompletionSealAuthority> {
        self.completion_sealer.clone()
    }
    fn full_admission_recovery_protocol_supported(&self) -> bool {
        true
    }

    async fn acquire_gc_sweep(&self, owner: &str, ttl_secs: i64) -> Result<bool> {
        self.acquire_gc_sweep(owner, ttl_secs).await
    }
    async fn renew_gc_sweep(&self, owner: &str, ttl_secs: i64) -> Result<bool> {
        self.renew_gc_sweep(owner, ttl_secs).await
    }
    async fn release_gc_sweep(&self, owner: &str) -> Result<()> {
        self.release_gc_sweep(owner).await
    }
    async fn lock_gc_delete_batch(&self, owner: &str) -> Result<Box<dyn GcDeleteFence>> {
        self.lock_gc_delete_batch(owner).await
    }
    async fn register_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
        ttl: i64,
    ) -> Result<()> {
        self.register_transport_root(root, session, workspace, repo, ttl)
            .await
    }
    async fn renew_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
        ttl: i64,
    ) -> Result<bool> {
        self.renew_transport_root(root, session, workspace, repo, ttl)
            .await
    }
    async fn release_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
    ) -> Result<bool> {
        self.release_transport_root(root, session, workspace, repo)
            .await
    }
    async fn live_transport_roots_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<TransportRootLease>> {
        self.live_transport_roots_page(after, limit).await
    }
    async fn live_scheduler_roots_page(
        &self,
        after: Option<i64>,
        limit: u32,
    ) -> Result<Vec<SchedulerGcRoot>> {
        self.live_scheduler_roots_page(after, limit).await
    }
    async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome> {
        self.schedule(key).await
    }
    async fn subscribe_consumer(
        &self,
        key: &ArtifactKey,
        id: &str,
        ttl: i64,
    ) -> Result<ScheduleOutcome> {
        self.subscribe_consumer(key, id, ttl).await
    }
    async fn release_consumer(&self, artifact_id: i64, id: &str) -> Result<()> {
        self.release_consumer(artifact_id, id).await
    }
    async fn observe(
        &self,
        w: &str,
        r: &str,
        b: &str,
        c: &str,
        k: &[ArtifactKind],
        v: u32,
        g: Option<u64>,
    ) -> Result<ObservationOutcome> {
        self.observe(w, r, b, c, k, v, g).await
    }
    async fn observation_snapshot(&self, w: &str, r: &str, b: &str) -> Result<ObservationSnapshot> {
        self.observation_snapshot(w, r, b).await
    }
    async fn retry_failed(&self, key: &ArtifactKey) -> Result<RetryOutcome> {
        self.retry_failed(key).await
    }
    async fn quarantine_ready(
        &self,
        id: i64,
        expected_manifest: Option<&str>,
        error: &str,
    ) -> Result<QuarantineOutcome> {
        self.quarantine_ready(id, expected_manifest, error).await
    }
    async fn fence_ready_publications(
        &self,
        expected: &[(i64, Option<String>)],
        provenance: &ActivationFenceProvenance,
        ttl_secs: i64,
    ) -> Result<Option<ReadyPublicationFence>> {
        self.fence_ready_publications(expected, provenance, ttl_secs)
            .await
    }
    async fn release_ready_publication_fence(&self, fence: ReadyPublicationFence) -> Result<()> {
        self.release_ready_publication_fence(fence).await
    }
    async fn mark_activation_unknown(
        &self,
        fence: &ReadyPublicationFence,
        ttl_secs: i64,
    ) -> Result<bool> {
        self.mark_activation_unknown(fence, ttl_secs).await
    }
    async fn recover_activation_fence(
        &self,
        provenance: &ActivationFenceProvenance,
    ) -> Result<Option<ReadyPublicationFence>> {
        self.recover_activation_fence(provenance).await
    }
    async fn unknown_activation_fences_page(
        &self,
        after_generation: Option<u64>,
        limit: usize,
    ) -> Result<UnknownActivationFencePage> {
        self.unknown_activation_fences_page(after_generation, limit)
            .await
    }
    async fn claim(&self, owner: &str, lease: i64) -> Result<Option<ClaimedArtifact>> {
        self.claim(owner, lease).await
    }
    async fn heartbeat(&self, claim: &ClaimedArtifact, owner: &str, lease: i64) -> Result<bool> {
        self.heartbeat(claim, owner, lease).await
    }
    async fn owns(&self, claim: &ClaimedArtifact, owner: &str) -> Result<bool> {
        self.owns(claim, owner).await
    }
    async fn complete_verified(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        evidence: &VerifiedCompletionEvidence,
    ) -> Result<bool> {
        self.complete_verified(claim, owner, evidence).await
    }
    async fn fail(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        class: FailureClass,
        error: &str,
    ) -> Result<bool> {
        self.fail(claim, owner, class, error).await
    }
    async fn reconcile_expired(&self) -> Result<(u64, u64)> {
        self.reconcile_expired().await
    }
    async fn get(&self, id: i64) -> Result<Option<ArtifactRecord>> {
        self.get(id).await
    }
    async fn get_by_key(&self, key: &ArtifactKey) -> Result<Option<ArtifactRecord>> {
        self.get_by_key(key).await
    }
    async fn ready_page(&self, after_id: i64, limit: usize) -> Result<Vec<ArtifactRecord>> {
        self.ready_page(after_id, limit).await
    }
    async fn ready_candidates(
        &self,
        w: &str,
        r: &str,
        k: ArtifactKind,
        v: u32,
        limit: u32,
    ) -> Result<Vec<ArtifactRecord>> {
        self.ready_candidates(w, r, k, v, limit).await
    }
    async fn published(
        &self,
        w: &str,
        r: &str,
        b: &str,
        k: ArtifactKind,
        v: u32,
    ) -> Result<Option<ArtifactRecord>> {
        self.published(w, r, b, k, v).await
    }
    async fn complete_full_base_candidates(
        &self,
        w: &str,
        r: &str,
        v: u32,
        limit: u32,
    ) -> Result<Vec<String>> {
        self.complete_full_base_candidates(w, r, v, limit).await
    }
    async fn quarantine_publication(
        &self,
        key: &ArtifactKey,
        manifest: &str,
        reason: &str,
    ) -> Result<bool> {
        self.quarantine_publication(key, manifest, reason).await
    }
    async fn counts(
        &self,
    ) -> Result<Vec<(ArtifactKind, crate::artifact_scheduler::ArtifactState, u64)>> {
        self.counts().await
    }
}
