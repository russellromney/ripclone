//! Backend contract for the normalized artifact scheduler state machine.
//!
//! Each mutating method is one database transaction. Implementations must not
//! split admission, generation checks, claim-cap accounting, or publication
//! alias updates across transactions.

use crate::artifact_scheduler::{
    ArtifactKey, ArtifactKind, ArtifactRecord, ArtifactTask, ClaimedArtifact, CompletionEvidence,
    ExecutionContext, ExecutionOutcome, FailureClass, ObservationOutcome, ObservationSnapshot,
    RetryOutcome, ScheduleOutcome, validate_evidence, validate_lease,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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
    async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome>;
    async fn subscribe_consumer(
        &self,
        key: &ArtifactKey,
        consumer_id: &str,
        ttl_secs: i64,
    ) -> Result<ScheduleOutcome>;
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
    async fn claim(&self, owner: &str, lease_secs: i64) -> Result<Option<ClaimedArtifact>>;
    async fn heartbeat(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        lease_secs: i64,
    ) -> Result<bool>;
    async fn owns(&self, claim: &ClaimedArtifact, owner: &str) -> Result<bool>;
    async fn complete(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        evidence: &CompletionEvidence,
    ) -> Result<bool>;
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
    async fn published(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
        kind: ArtifactKind,
        format_version: u32,
    ) -> Result<Option<ArtifactRecord>>;
    async fn counts(
        &self,
    ) -> Result<Vec<(ArtifactKind, crate::artifact_scheduler::ArtifactState, u64)>>;

    /// Backend-independent worker protocol. Persistence implementations only
    /// provide fenced primitives; this method guarantees ownership preflight,
    /// internal heartbeats, cooperative cancellation, child draining, and
    /// attempt-unique scratch before any backend can publish.
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

#[async_trait]
impl ArtifactSchedulerPersistence for crate::artifact_scheduler::ArtifactScheduler {
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
    async fn claim(&self, owner: &str, lease: i64) -> Result<Option<ClaimedArtifact>> {
        self.claim(owner, lease).await
    }
    async fn heartbeat(&self, claim: &ClaimedArtifact, owner: &str, lease: i64) -> Result<bool> {
        self.heartbeat(claim, owner, lease).await
    }
    async fn owns(&self, claim: &ClaimedArtifact, owner: &str) -> Result<bool> {
        self.owns(claim, owner).await
    }
    async fn complete(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        evidence: &CompletionEvidence,
    ) -> Result<bool> {
        self.complete(claim, owner, evidence).await
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
    async fn counts(
        &self,
    ) -> Result<Vec<(ArtifactKind, crate::artifact_scheduler::ArtifactState, u64)>> {
        self.counts().await
    }
}
