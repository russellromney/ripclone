use anyhow::{Result, bail};
use async_trait::async_trait;
use ripclone::artifact_manifest::CasBlob;
use ripclone::artifact_scheduler::{
    ArtifactKey, ArtifactKind, ArtifactRecord, ClaimedArtifact, CompletionSealAuthority,
    FailureClass, ObservationOutcome, ObservationSnapshot, RetryOutcome, ScheduleOutcome,
    VerifiedCompletionEvidence,
};
use ripclone::artifact_scheduler_backend::ArtifactSchedulerPersistence;
use ripclone::git_source::{GitSourceLoader, GitSourceUploader};
use ripclone::meta::{MetaDb, RefRow};
use ripclone::queue::DeadLetteredInitialization;
use ripclone::queue::sql::QueueDb;
use ripclone::sync_coordinator::{DurableSourceAcquireOutcome, DurableSourceAcquirer, SyncIntent};
use std::path::Path;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn neutral_error<T>() -> Result<T> {
    bail!("database-agnostic test adapter")
}

struct NeutralMeta;

#[async_trait]
impl MetaDb for NeutralMeta {
    async fn init(&self) -> Result<()> {
        neutral_error()
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<RefRow>> {
        neutral_error()
    }
    async fn get_by_commit(&self, _: &str, _: &str) -> Result<Vec<RefRow>> {
        neutral_error()
    }
    async fn save_ordered(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<i64>,
        _: Option<i64>,
    ) -> Result<()> {
        neutral_error()
    }
    async fn compare_and_swap_data(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<bool> {
        neutral_error()
    }
    async fn list_repos(&self) -> Result<Vec<String>> {
        neutral_error()
    }
    async fn list_branches(&self, _: &str) -> Result<Vec<String>> {
        neutral_error()
    }
    async fn add_repo(&self, _: &str, _: &str) -> Result<()> {
        neutral_error()
    }
    async fn insert_added_repo(&self, _: &str, _: &str) -> Result<bool> {
        neutral_error()
    }
    async fn compare_and_swap_added_repo(&self, _: &str, _: &str, _: &str) -> Result<bool> {
        neutral_error()
    }
    async fn get_added_repo(&self, _: &str) -> Result<Option<String>> {
        neutral_error()
    }
    async fn remove_added_repo(&self, _: &str) -> Result<()> {
        neutral_error()
    }
    async fn list_added_repos(&self) -> Result<Vec<String>> {
        neutral_error()
    }
    async fn health(&self) -> Result<()> {
        neutral_error()
    }
}

struct NeutralQueue;

#[async_trait]
impl QueueDb for NeutralQueue {
    async fn init(&self) -> Result<()> {
        neutral_error()
    }
    async fn active_job_id(&self, _: &str) -> Result<Option<i64>> {
        neutral_error()
    }
    async fn insert_job(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: Option<&str>,
        _: i64,
        _: i64,
    ) -> Result<i64> {
        neutral_error()
    }
    async fn raise_size_class(&self, _: i64, _: i64) -> Result<()> {
        neutral_error()
    }
    async fn reclaim_stale(&self, _: i64, _: i64, _: i64, _: &str) -> Result<()> {
        neutral_error()
    }
    async fn dead_lettered_initializations(&self) -> Result<Vec<DeadLetteredInitialization>> {
        neutral_error()
    }
    async fn acknowledge_dead_lettered_initialization(&self, _: i64, _: &str) -> Result<()> {
        neutral_error()
    }
    async fn job_size_class(&self, _: i64) -> Result<Option<i64>> {
        neutral_error()
    }
    async fn next_queued_id(&self, _: Option<i64>) -> Result<Option<i64>> {
        neutral_error()
    }
    async fn try_claim(&self, _: i64, _: &str, _: i64) -> Result<bool> {
        neutral_error()
    }
    async fn job_fields(
        &self,
        _: i64,
    ) -> Result<Option<(String, String, String, Option<String>, Option<String>)>> {
        neutral_error()
    }
    async fn finish(&self, _: i64, _: &str, _: &str, _: i64, _: Option<&str>) -> Result<bool> {
        neutral_error()
    }
    async fn claimed_attempts(&self, _: i64, _: &str) -> Result<Option<i64>> {
        neutral_error()
    }
    async fn requeue_claim(&self, _: i64, _: &str, _: &str) -> Result<bool> {
        neutral_error()
    }
    async fn status(&self, _: i64) -> Result<Option<(String, Option<String>)>> {
        neutral_error()
    }
    async fn count_queued(&self) -> Result<i64> {
        neutral_error()
    }
    async fn count_queued_by_size_class(&self) -> Result<Vec<(i64, i64)>> {
        neutral_error()
    }
    async fn prune_failed(&self, _: i64) -> Result<u64> {
        neutral_error()
    }
}

struct NeutralSources;

struct NeutralScheduler;

#[async_trait]
impl ArtifactSchedulerPersistence for NeutralScheduler {
    fn completion_verifier(&self) -> Arc<dyn ripclone::artifact_scheduler::CompletionVerifier> {
        panic!("database-agnostic test adapter")
    }

    fn completion_sealer(&self) -> Arc<CompletionSealAuthority> {
        panic!("database-agnostic test adapter")
    }

    async fn schedule(&self, _: &ArtifactKey) -> Result<ScheduleOutcome> {
        neutral_error()
    }

    async fn subscribe_consumer(
        &self,
        _: &ArtifactKey,
        _: &str,
        _: i64,
    ) -> Result<ScheduleOutcome> {
        neutral_error()
    }

    async fn release_consumer(&self, _: i64, _: &str) -> Result<()> {
        neutral_error()
    }

    async fn observe(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &[ArtifactKind],
        _: u32,
        _: Option<u64>,
    ) -> Result<ObservationOutcome> {
        neutral_error()
    }

    async fn observation_snapshot(&self, _: &str, _: &str, _: &str) -> Result<ObservationSnapshot> {
        neutral_error()
    }

    async fn retry_failed(&self, _: &ArtifactKey) -> Result<RetryOutcome> {
        neutral_error()
    }

    async fn claim(&self, _: &str, _: i64) -> Result<Option<ClaimedArtifact>> {
        neutral_error()
    }

    async fn heartbeat(&self, _: &ClaimedArtifact, _: &str, _: i64) -> Result<bool> {
        neutral_error()
    }

    async fn owns(&self, _: &ClaimedArtifact, _: &str) -> Result<bool> {
        neutral_error()
    }

    async fn complete_verified(
        &self,
        _: &ClaimedArtifact,
        _: &str,
        _: &VerifiedCompletionEvidence,
    ) -> Result<bool> {
        neutral_error()
    }

    async fn fail(&self, _: &ClaimedArtifact, _: &str, _: FailureClass, _: &str) -> Result<bool> {
        neutral_error()
    }

    async fn reconcile_expired(&self) -> Result<(u64, u64)> {
        neutral_error()
    }

    async fn get(&self, _: i64) -> Result<Option<ArtifactRecord>> {
        neutral_error()
    }

    async fn get_by_key(&self, _: &ArtifactKey) -> Result<Option<ArtifactRecord>> {
        neutral_error()
    }

    async fn ready_page(&self, _: i64, _: usize) -> Result<Vec<ArtifactRecord>> {
        neutral_error()
    }

    async fn ready_candidates(
        &self,
        _: &str,
        _: &str,
        _: ArtifactKind,
        _: u32,
        _: u32,
    ) -> Result<Vec<ArtifactRecord>> {
        neutral_error()
    }

    async fn published(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: ArtifactKind,
        _: u32,
    ) -> Result<Option<ArtifactRecord>> {
        neutral_error()
    }

    async fn complete_full_base_candidates(
        &self,
        _: &str,
        _: &str,
        _: u32,
        _: u32,
    ) -> Result<Vec<String>> {
        neutral_error()
    }

    async fn quarantine_publication(&self, _: &ArtifactKey, _: &str, _: &str) -> Result<bool> {
        neutral_error()
    }

    async fn counts(
        &self,
    ) -> Result<
        Vec<(
            ArtifactKind,
            ripclone::artifact_scheduler::ArtifactState,
            u64,
        )>,
    > {
        neutral_error()
    }
}

#[async_trait]
impl DurableSourceAcquirer for NeutralSources {
    async fn acquire_exact(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: SyncIntent,
    ) -> Result<DurableSourceAcquireOutcome> {
        neutral_error()
    }
}

impl GitSourceUploader for NeutralSources {
    fn put_file(&self, _: &CasBlob, _: &Path, _: &CancellationToken) -> Result<()> {
        neutral_error()
    }
    fn put_bytes(&self, _: &CasBlob, _: &[u8], _: &CancellationToken) -> Result<()> {
        neutral_error()
    }
}

impl GitSourceLoader for NeutralSources {
    fn load_file(&self, _: &CasBlob, _: &Path, _: &CancellationToken) -> Result<()> {
        neutral_error()
    }
    fn load_bytes(&self, _: &CasBlob, _: u64, _: &CancellationToken) -> Result<Vec<u8>> {
        neutral_error()
    }
}

fn assert_contracts<M, Q, A, S>()
where
    M: MetaDb,
    Q: QueueDb,
    A: ArtifactSchedulerPersistence,
    S: DurableSourceAcquirer + GitSourceUploader + GitSourceLoader,
{
}

#[tokio::test]
async fn shared_backend_contracts_compile_without_database_types() {
    assert_contracts::<NeutralMeta, NeutralQueue, NeutralScheduler, NeutralSources>();

    assert!(
        NeutralMeta
            .health()
            .await
            .unwrap_err()
            .to_string()
            .contains("database-agnostic")
    );
    assert!(
        NeutralQueue
            .init()
            .await
            .unwrap_err()
            .to_string()
            .contains("database-agnostic")
    );
    assert!(
        NeutralSources
            .put_bytes(
                &CasBlob {
                    hash: "0".repeat(64),
                    len: 1
                },
                b"x",
                &CancellationToken::new(),
            )
            .is_err()
    );
}
