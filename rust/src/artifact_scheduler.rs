//! Durable, commit-addressed scheduling for independently publishable artifacts.
//!
//! SQLite is both the local and cross-process implementation: all admission,
//! observation, lease, retry, fairness, and publication decisions are fenced by
//! transactions in this database. Builders may only publish through a live
//! [`ClaimedArtifact`] and typed [`CompletionEvidence`].

use crate::cas::Cas;
use anyhow::{Context, Result, bail};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{Sqlite, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};
#[cfg(test)]
use std::future::Future;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
#[cfg(test)]
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Head,
    FullHistory,
    Files,
}
impl ArtifactKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::FullHistory => "full_history",
            Self::Files => "files",
        }
    }
    pub(crate) fn parse(s: &str) -> Result<Self> {
        match s {
            "head" => Ok(Self::Head),
            "full_history" => Ok(Self::FullHistory),
            "files" => Ok(Self::Files),
            _ => bail!("unknown artifact kind {s}"),
        }
    }
    pub(crate) fn expensive(self) -> bool {
        matches!(self, Self::FullHistory | Self::Files)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArtifactKey {
    pub workspace: String,
    pub repo: String,
    pub commit: String,
    pub kind: ArtifactKind,
    pub format_version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactState {
    Queued,
    Running,
    Ready,
    Failed,
}
impl ArtifactState {
    pub(crate) fn parse(s: &str) -> Result<Self> {
        match s {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "ready" => Ok(Self::Ready),
            "failed" => Ok(Self::Failed),
            _ => bail!("unknown artifact state {s}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Retryable,
    Permanent,
    DeadLetter,
}
impl FailureClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::DeadLetter => "dead_letter",
        }
    }
    pub(crate) fn parse(s: &str) -> Result<Self> {
        match s {
            "retryable" => Ok(Self::Retryable),
            "permanent" => Ok(Self::Permanent),
            "dead_letter" => Ok(Self::DeadLetter),
            _ => bail!("unknown failure class {s}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactRecord {
    pub id: i64,
    pub key: ArtifactKey,
    pub state: ArtifactState,
    pub owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub lease_generation: u64,
    pub claim_attempts: u32,
    pub retry_count: u32,
    pub manifest: Option<String>,
    pub error: Option<String>,
    pub failure_class: Option<FailureClass>,
}

#[derive(Debug, Clone)]
pub struct ClaimedArtifact {
    pub record: ArtifactRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleOutcome {
    Enqueued(i64),
    Subscribed(i64),
    AlreadyReady(i64),
    Failed(i64, FailureClass),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationOutcome {
    Accepted {
        generation: u64,
        artifacts: Vec<(ArtifactKind, ScheduleOutcome)>,
    },
    Stale {
        current_generation: u64,
    },
    /// The branch already points at this exact commit. No generation changed
    /// and no artifact work was scheduled.
    Unchanged {
        generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationSnapshot {
    workspace: String,
    repo: String,
    branch: String,
    generation: Option<u64>,
    commit: Option<String>,
}
impl ObservationSnapshot {
    pub(crate) fn new(
        workspace: &str,
        repo: &str,
        branch: &str,
        generation: Option<u64>,
        commit: Option<String>,
    ) -> Self {
        Self {
            workspace: workspace.into(),
            repo: repo.into(),
            branch: branch.into(),
            generation,
            commit,
        }
    }
    pub fn workspace(&self) -> &str {
        &self.workspace
    }
    pub fn repo(&self) -> &str {
        &self.repo
    }
    pub fn branch(&self) -> &str {
        &self.branch
    }
    pub fn generation(&self) -> Option<u64> {
        self.generation
    }
    pub fn commit(&self) -> Option<&str> {
        self.commit.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryOutcome {
    Requeued(i64),
    NotFailed,
    NotRetryable(FailureClass),
    Exhausted,
}

/// Result of atomically withdrawing a Ready publication whose immutable
/// manifest failed verification.  The manifest value is part of the compare
/// and swap, so a verifier can never quarantine a replacement publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineOutcome {
    Requeued(i64),
    LostRace,
    Exhausted,
}

/// Opaque, expiring proof that exact Ready manifest rows were fenced together.
/// While live, manifest quarantine cannot withdraw any member publication.
#[derive(Debug)]
pub struct ReadyPublicationFence {
    token: String,
    generation: u64,
    operation_id: String,
    provenance: ActivationFenceProvenance,
    expected: Vec<(i64, Option<String>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationFenceProvenance {
    pub workspace: String,
    pub repo: String,
    pub branch: String,
    pub target: String,
    pub attempt_id: String,
}

impl ActivationFenceProvenance {
    pub fn operation_id(&self) -> String {
        let mut digest = Sha256::new();
        for component in [
            self.workspace.as_str(),
            self.repo.as_str(),
            self.branch.as_str(),
            self.target.as_str(),
            self.attempt_id.as_str(),
        ] {
            digest.update((component.len() as u64).to_be_bytes());
            digest.update(component.as_bytes());
        }
        format!("admission-operation-{}", hex::encode(digest.finalize()))
    }
}

#[derive(Debug)]
pub struct UnknownActivationFencePage {
    pub fences: Vec<ReadyPublicationFence>,
    pub next_generation: Option<u64>,
}

impl ReadyPublicationFence {
    pub(crate) fn new(
        token: String,
        generation: u64,
        operation_id: String,
        provenance: ActivationFenceProvenance,
        expected: Vec<(i64, Option<String>)>,
    ) -> Self {
        Self {
            token,
            generation,
            operation_id,
            provenance,
            expected,
        }
    }
    pub(crate) fn parts(&self) -> (&str, u64, &str, &[(i64, Option<String>)]) {
        (
            &self.token,
            self.generation,
            &self.operation_id,
            &self.expected,
        )
    }
    pub fn provenance(&self) -> &ActivationFenceProvenance {
        &self.provenance
    }
}

#[derive(Debug, Clone)]
pub struct CompletionEvidence {
    key: ArtifactKey,
    manifest: String,
    artifact_count: u64,
}
impl CompletionEvidence {
    pub fn new(key: ArtifactKey, manifest: impl Into<String>) -> Result<Self> {
        let manifest = manifest.into();
        if manifest.trim().is_empty() {
            bail!("artifact completion manifest is empty")
        };
        Ok(Self {
            key,
            manifest,
            artifact_count: 1,
        })
    }

    pub fn from_manifest(
        key: ArtifactKey,
        manifest: impl Into<String>,
        artifact_count: u64,
    ) -> Result<Self> {
        let mut evidence = Self::new(key, manifest)?;
        if artifact_count == 0 {
            bail!("artifact completion contains no artifacts");
        }
        evidence.artifact_count = artifact_count;
        Ok(evidence)
    }

    pub fn key(&self) -> &ArtifactKey {
        &self.key
    }

    pub fn manifest(&self) -> &str {
        &self.manifest
    }

    pub fn artifact_count(&self) -> u64 {
        self.artifact_count
    }
}

/// An immutable, attempt-specific capability produced only after the configured
/// verifier accepts completion evidence. Its authentication tag is bound to
/// the verifier instance, claim identity, lease generation, exact artifact key,
/// manifest digest, and artifact count. Callers can inspect raw evidence through
/// getters, but cannot forge or mutate either raw fields or this capability:
///
/// ```compile_fail
/// use ripclone::artifact_scheduler::CompletionEvidence;
/// fn forge(evidence: &mut CompletionEvidence) {
///     evidence.manifest.clear();
/// }
/// ```
#[derive(Clone)]
pub struct VerifiedCompletionEvidence {
    evidence: CompletionEvidence,
    artifact_id: i64,
    lease_generation: u64,
    manifest_hash: [u8; 32],
    tag: [u8; 32],
}

impl std::fmt::Debug for VerifiedCompletionEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedCompletionEvidence")
            .field("key", self.evidence.key())
            .field("artifact_id", &self.artifact_id)
            .field("lease_generation", &self.lease_generation)
            .field("artifact_count", &self.evidence.artifact_count())
            .finish_non_exhaustive()
    }
}

impl VerifiedCompletionEvidence {
    pub fn evidence(&self) -> &CompletionEvidence {
        &self.evidence
    }
}

type CompletionHmac = Hmac<Sha256>;

/// Per-scheduler-process sealing authority. Even verifiers with the same
/// advertised identity receive distinct authorities, so capabilities cannot be
/// transferred between verifier instances or scheduler handles opened with one.
#[doc(hidden)]
pub struct CompletionSealAuthority {
    secret: [u8; 32],
    verifier_identity: String,
}

impl CompletionSealAuthority {
    pub(crate) fn new(verifier_identity: &str) -> Result<Self> {
        let verifier_identity = verifier_identity.trim();
        if verifier_identity.is_empty() {
            bail!("completion verifier identity is empty");
        }
        Ok(Self {
            secret: rand::random(),
            verifier_identity: verifier_identity.to_owned(),
        })
    }

    pub(crate) fn seal(
        &self,
        claim: &ClaimedArtifact,
        evidence: CompletionEvidence,
    ) -> Result<VerifiedCompletionEvidence> {
        validate_evidence(claim, &evidence)?;
        let manifest_hash: [u8; 32] = Sha256::digest(evidence.manifest.as_bytes()).into();
        let tag = self.tag(claim, &evidence, &manifest_hash)?;
        Ok(VerifiedCompletionEvidence {
            evidence,
            artifact_id: claim.record.id,
            lease_generation: claim.record.lease_generation,
            manifest_hash,
            tag,
        })
    }

    pub(crate) fn verify<'a>(
        &self,
        claim: &ClaimedArtifact,
        verified: &'a VerifiedCompletionEvidence,
    ) -> Result<&'a CompletionEvidence> {
        if verified.artifact_id != claim.record.id
            || verified.lease_generation != claim.record.lease_generation
            || verified.evidence.key != claim.record.key
        {
            bail!("verified completion capability does not match claimed artifact attempt");
        }
        let manifest_hash: [u8; 32] = Sha256::digest(verified.evidence.manifest.as_bytes()).into();
        if manifest_hash != verified.manifest_hash {
            bail!("verified completion capability manifest digest changed");
        }
        let mac = self.mac(claim, &verified.evidence, &manifest_hash)?;
        mac.verify_slice(&verified.tag)
            .map_err(|_| anyhow::anyhow!("verified completion capability has an invalid seal"))?;
        Ok(&verified.evidence)
    }

    fn tag(
        &self,
        claim: &ClaimedArtifact,
        evidence: &CompletionEvidence,
        manifest_hash: &[u8; 32],
    ) -> Result<[u8; 32]> {
        Ok(self
            .mac(claim, evidence, manifest_hash)?
            .finalize()
            .into_bytes()
            .into())
    }

    fn mac(
        &self,
        claim: &ClaimedArtifact,
        evidence: &CompletionEvidence,
        manifest_hash: &[u8; 32],
    ) -> Result<CompletionHmac> {
        let mut mac = CompletionHmac::new_from_slice(&self.secret)
            .map_err(|_| anyhow::anyhow!("invalid completion sealing key"))?;
        mac.update(b"ripclone-verified-completion-v1\0");
        update_len_prefixed(&mut mac, self.verifier_identity.as_bytes());
        mac.update(&claim.record.id.to_be_bytes());
        mac.update(&claim.record.lease_generation.to_be_bytes());
        for value in [
            evidence.key.workspace.as_bytes(),
            evidence.key.repo.as_bytes(),
            evidence.key.commit.as_bytes(),
            evidence.key.kind.as_str().as_bytes(),
        ] {
            update_len_prefixed(&mut mac, value);
        }
        mac.update(&evidence.key.format_version.to_be_bytes());
        mac.update(manifest_hash);
        mac.update(&evidence.artifact_count.to_be_bytes());
        Ok(mac)
    }
}

fn update_len_prefixed(mac: &mut CompletionHmac, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}

/// Integration hook for mode-specific manifest/CAS validation. Production
/// implementations verify every referenced object before the fenced publish.
pub trait CompletionVerifier: Send + Sync {
    fn identity(&self) -> &str;
    fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()>;
    /// Expand the immutable CAS graph delegated by an already-bound root.
    /// FullHistory implementations must authenticate every nested level before
    /// returning its physical pack/index children. The loader must return the
    /// exact bytes for the supplied descriptor.
    fn authenticated_referenced_blobs(
        &self,
        manifest: &crate::artifact_manifest::ArtifactManifest,
        _load_level: &mut dyn FnMut(&crate::artifact_manifest::CasBlob) -> Result<Vec<u8>>,
    ) -> Result<Vec<crate::artifact_manifest::CasBlob>> {
        if matches!(
            &manifest.payload,
            crate::artifact_manifest::ArtifactPayload::FullHistory(history)
                if !history.levels.is_empty()
        ) {
            bail!("completion verifier cannot authenticate nested history manifests")
        }
        Ok(manifest
            .payload
            .referenced_blobs()
            .into_iter()
            .cloned()
            .collect())
    }
    fn verify_owned(
        &self,
        claim: &ClaimedArtifact,
        evidence: &CompletionEvidence,
        context: &ExecutionContext,
    ) -> Result<()> {
        if context.cancelled.is_cancelled() {
            bail!("artifact verification cancelled");
        }
        self.verify(claim, evidence)?;
        if context.cancelled.is_cancelled() {
            bail!("artifact verification cancelled");
        }
        Ok(())
    }

    /// Durably publish already-verified evidence before the scheduler may
    /// transition the claim to Ready. Implementations publish children first
    /// and the root manifest last, then verify durable presence.
    fn publish_owned<'a>(
        &'a self,
        _claim: &'a ClaimedArtifact,
        _evidence: &'a CompletionEvidence,
        context: &'a ExecutionContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if context.cancelled.is_cancelled() {
                bail!("artifact publication cancelled");
            }
            #[cfg(test)]
            {
                Ok(())
            }
            #[cfg(not(test))]
            {
                bail!("durable artifact publisher is not configured")
            }
        })
    }
}
struct StructuralVerifier;
impl CompletionVerifier for StructuralVerifier {
    fn identity(&self) -> &str {
        "structural-test-only-v1"
    }
    fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
        validate_evidence(claim, evidence)?;
        if evidence.artifact_count() == 0 {
            bail!("completion evidence contains no artifacts")
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SchedulerLimits {
    pub total_backlog: usize,
    pub workspace_backlog: usize,
    pub head_reserved: usize,
    pub head_backlog: usize,
    pub full_history_backlog: usize,
    pub files_backlog: usize,
    pub total_running: usize,
    /// Also reserved from the combined FullHistory+Files running caps.
    pub head_running: usize,
    pub full_history_running: usize,
    pub files_running: usize,
    pub workspace_running: usize,
    pub max_claim_attempts: u32,
    pub max_manual_retries: u32,
}
impl Default for SchedulerLimits {
    fn default() -> Self {
        Self {
            total_backlog: 4096,
            workspace_backlog: 1024,
            head_reserved: 256,
            head_backlog: 2048,
            full_history_backlog: 1024,
            files_backlog: 1024,
            total_running: 32,
            head_running: 16,
            full_history_running: 8,
            files_running: 8,
            workspace_running: 8,
            max_claim_attempts: 5,
            max_manual_retries: 3,
        }
    }
}

#[derive(Clone)]
pub struct ArtifactScheduler {
    pool: SqlitePool,
    limits: SchedulerLimits,
    pub(crate) verifier: Arc<dyn CompletionVerifier>,
    pub(crate) completion_sealer: Arc<CompletionSealAuthority>,
}

struct ImmediateTransaction {
    connection: Option<PoolConnection<Sqlite>>,
}

impl ImmediateTransaction {
    async fn begin(pool: &SqlitePool) -> Result<Self> {
        let mut transaction = Self {
            connection: Some(pool.acquire().await?),
        };
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *transaction)
            .await?;
        Ok(transaction)
    }
    fn release(mut self) {
        drop(self.connection.take());
    }
}
impl std::ops::Deref for ImmediateTransaction {
    type Target = sqlx::SqliteConnection;
    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().expect("transaction connection")
    }
}
impl std::ops::DerefMut for ImmediateTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().expect("transaction connection")
    }
}
impl Drop for ImmediateTransaction {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            drop(connection.detach());
        }
    }
}

struct SqliteGcDeleteFence(Option<ImmediateTransaction>);
#[async_trait::async_trait]
impl crate::artifact_scheduler_backend::GcDeleteFence for SqliteGcDeleteFence {
    async fn release(mut self: Box<Self>) -> Result<()> {
        if let Some(mut connection) = self.0.take() {
            if let Err(error) = sqlx::query("COMMIT").execute(&mut *connection).await {
                return Err(error).context("commit GC delete fence; connection retired");
            }
            connection.release();
        }
        Ok(())
    }
}

/// Context passed to cooperative work. Blocking/external children must be
/// awaited by the returned future, observe `cancelled`, and write only beneath
/// `scratch`. Publication outside [`ArtifactScheduler::run_owned`] is forbidden.
#[derive(Clone)]
pub struct ExecutionContext {
    pub cancelled: CancellationToken,
    pub scratch: PathBuf,
}
#[cfg(test)]
pub type ArtifactTaskFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
#[cfg(test)]
pub struct ArtifactTask(Box<dyn FnOnce(ExecutionContext) -> ArtifactTaskFuture + Send + 'static>);
#[cfg(test)]
impl ArtifactTask {
    pub fn cooperative<F, Fut>(f: F) -> Self
    where
        F: FnOnce(ExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self(Box::new(move |c| Box::pin(f(c))))
    }
    pub(crate) fn start(self, context: ExecutionContext) -> ArtifactTaskFuture {
        (self.0)(context)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Ready,
    Failed,
    LostLease,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS artifact_jobs(
 id INTEGER PRIMARY KEY AUTOINCREMENT, workspace TEXT NOT NULL, repo TEXT NOT NULL,
 commit_oid TEXT NOT NULL, kind TEXT NOT NULL,
 format_version INTEGER NOT NULL CHECK(format_version BETWEEN 1 AND 4294967295),
 state TEXT NOT NULL CHECK(state IN('queued','running','ready','failed')), owner TEXT,
 heartbeat_at INTEGER, lease_expires_at INTEGER, lease_generation INTEGER NOT NULL DEFAULT 0,
 claim_attempts INTEGER NOT NULL DEFAULT 0, retry_count INTEGER NOT NULL DEFAULT 0,
 manifest TEXT, error TEXT, failure_class TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 UNIQUE(workspace,repo,commit_oid,kind,format_version));
CREATE INDEX IF NOT EXISTS artifact_jobs_claim ON artifact_jobs(state,kind,created_at,id);
CREATE INDEX IF NOT EXISTS artifact_jobs_lease ON artifact_jobs(state,lease_expires_at);
CREATE TABLE IF NOT EXISTS artifact_base_retention(
 artifact_id INTEGER PRIMARY KEY,workspace TEXT NOT NULL,repo TEXT NOT NULL,format_version INTEGER NOT NULL,
 head_rank INTEGER CHECK(head_rank BETWEEN 1 AND 8),pair_rank INTEGER CHECK(pair_rank BETWEEN 1 AND 8),
 CHECK(head_rank IS NOT NULL OR pair_rank IS NOT NULL),
 FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS artifact_base_retention_repo ON artifact_base_retention(workspace,repo,format_version,artifact_id);
CREATE TABLE IF NOT EXISTS branch_observations(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation INTEGER NOT NULL,
 desired_commit TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch));
CREATE TABLE IF NOT EXISTS artifact_observations(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,kind TEXT NOT NULL,
 desired_commit TEXT NOT NULL,desired_artifact_id INTEGER NOT NULL,desired_generation INTEGER NOT NULL,
 published_artifact_id INTEGER,format_version INTEGER NOT NULL DEFAULT 1,observed_at INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(workspace,repo,branch,kind));
CREATE INDEX IF NOT EXISTS artifact_observations_published ON artifact_observations(published_artifact_id);
CREATE TABLE IF NOT EXISTS artifact_consumers(artifact_id INTEGER NOT NULL,consumer_id TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(artifact_id,consumer_id));
CREATE TABLE IF NOT EXISTS artifact_transport_leases(
 root_hash TEXT NOT NULL,session_id TEXT NOT NULL,workspace TEXT NOT NULL,repo TEXT NOT NULL,
 expires_at INTEGER NOT NULL,PRIMARY KEY(root_hash,session_id));
CREATE INDEX IF NOT EXISTS artifact_transport_leases_expiry ON artifact_transport_leases(expires_at);
CREATE TABLE IF NOT EXISTS artifact_gc_sweep(
 id INTEGER PRIMARY KEY CHECK(id=1),owner TEXT NOT NULL,expires_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS scheduler_state(id INTEGER PRIMARY KEY CHECK(id=1),fairness_cursor INTEGER NOT NULL,workspace_cursor TEXT NOT NULL DEFAULT '',config_fingerprint TEXT NOT NULL DEFAULT '',limits_fingerprint TEXT NOT NULL DEFAULT '');
INSERT OR IGNORE INTO scheduler_state(id,fairness_cursor) VALUES(1,0);
"#;

const FENCE_SCHEMA_V3: &str = r#"
CREATE TABLE ready_publication_fence_sequence(id INTEGER PRIMARY KEY CHECK(id=1),generation INTEGER NOT NULL CHECK(generation>=0));
INSERT INTO ready_publication_fence_sequence(id,generation) VALUES(1,0);
CREATE TABLE ready_publication_fences(
 token TEXT PRIMARY KEY,generation INTEGER NOT NULL UNIQUE CHECK(generation>0),operation_id TEXT NOT NULL UNIQUE,
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,target TEXT NOT NULL,attempt_id TEXT NOT NULL,
 expires_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('held','activation_unknown')),
 UNIQUE(token,generation));
CREATE TABLE ready_publication_fence_members(
 token TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),artifact_id INTEGER NOT NULL,manifest TEXT NOT NULL CHECK(length(trim(manifest))>0),
 PRIMARY KEY(token,artifact_id),
 FOREIGN KEY(token,generation) REFERENCES ready_publication_fences(token,generation) ON DELETE CASCADE,
 FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE);
CREATE INDEX ready_publication_fences_recovery ON ready_publication_fences(state,generation,token);
"#;

impl ArtifactScheduler {
    pub async fn open(path: &str, limits: SchedulerLimits) -> Result<Self> {
        Self::open_with_verifier(path, limits, Arc::new(StructuralVerifier)).await
    }
    pub async fn open_with_verifier(
        path: &str,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(path)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(10))
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await?;
        Self::from_pool(pool, limits, verifier).await
    }

    /// Construct the scheduler on an existing metadata pool. This is the
    /// production path: ref metadata and artifact scheduling share one
    /// authenticated SQLite database instead of opening a second DSN.
    pub async fn from_pool(
        pool: SqlitePool,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        validate_limits(&limits)?;
        let verifier_id = verifier.identity().trim();
        if verifier_id.is_empty() {
            bail!("completion verifier identity is empty")
        }
        let fingerprint = scheduler_fingerprint(&limits, verifier_id);
        let mut migration = ImmediateTransaction::begin(&pool).await?;
        let migration_result: Result<()> = async {
        let prior_version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut *migration)
            .await?;
        if prior_version > 7 {
            bail!("artifact scheduler database is newer than this binary")
        }
        preflight_sqlite_schema(&mut migration, prior_version).await?;
        sqlx::raw_sql(SCHEMA)
            .execute(&mut *migration)
            .await
            .context("initialize artifact scheduler")?;
        for (table, column, definition) in [
            (
                "artifact_jobs",
                "lease_generation",
                "lease_generation INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "artifact_jobs",
                "claim_attempts",
                "claim_attempts INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "artifact_jobs",
                "retry_count",
                "retry_count INTEGER NOT NULL DEFAULT 0",
            ),
            ("artifact_jobs", "failure_class", "failure_class TEXT"),
            (
                "artifact_observations",
                "desired_generation",
                "desired_generation INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "artifact_observations",
                "observed_at",
                "observed_at INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "artifact_observations",
                "format_version",
                "format_version INTEGER NOT NULL DEFAULT 1",
            ),
            (
                "artifact_consumers",
                "expires_at",
                // Legacy consumers had no lease and could pin backlog forever;
                // zero deliberately expires them on first reconciliation.
                "expires_at INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "scheduler_state",
                "workspace_cursor",
                "workspace_cursor TEXT NOT NULL DEFAULT ''",
            ),
            (
                "scheduler_state",
                "config_fingerprint",
                "config_fingerprint TEXT NOT NULL DEFAULT ''",
            ),
            (
                "scheduler_state",
                "limits_fingerprint",
                "limits_fingerprint TEXT NOT NULL DEFAULT ''",
            ),
        ] {
            let exists: i64 =
                sqlx::query_scalar("SELECT count(*) FROM pragma_table_info(?) WHERE name=?")
                    .bind(table)
                    .bind(column)
                    .fetch_one(&mut *migration)
                    .await?;
            if exists == 0 {
                // `table` and `definition` come solely from the static migration
                // table above, never from runtime/user input.
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "ALTER TABLE {table} ADD COLUMN {definition}"
                )))
                .execute(&mut *migration)
                .await
                .with_context(|| format!("migrate {table}.{column}"))?;
            }
        }
        let old_attempts: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pragma_table_info('artifact_jobs') WHERE name='attempts'",
        )
        .fetch_one(&mut *migration)
        .await?;
        if old_attempts > 0 {
            sqlx::raw_sql(
                "UPDATE artifact_jobs SET claim_attempts=attempts WHERE claim_attempts=0",
            )
            .execute(&mut *migration)
            .await?;
        }
        let missing_desired:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_observations a LEFT JOIN artifact_jobs j ON j.id=a.desired_artifact_id AND j.workspace=a.workspace AND j.repo=a.repo AND j.kind=a.kind AND j.commit_oid=a.desired_commit WHERE j.id IS NULL").fetch_one(&mut *migration).await?;
        if missing_desired > 0 {
            bail!("legacy observation references a missing or mismatched desired artifact")
        }
        let conflicting:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT a.workspace,a.repo,a.branch FROM artifact_observations a WHERE a.observed_at=(SELECT max(b.observed_at) FROM artifact_observations b WHERE b.workspace=a.workspace AND b.repo=a.repo AND b.branch=a.branch) GROUP BY a.workspace,a.repo,a.branch HAVING count(DISTINCT a.desired_commit)>1)").fetch_one(&mut *migration).await?;
        if conflicting > 0 {
            bail!("legacy branch has conflicting latest observations")
        }
        // Seed a durable CAS generation for legacy branch subscriptions. This
        // is only a migration choice; all subsequent ordering is generation-
        // based and never compares these legacy wall-clock values again.
        sqlx::raw_sql(
            "INSERT OR IGNORE INTO branch_observations(workspace,repo,branch,generation,desired_commit,updated_at)
             SELECT a.workspace,a.repo,a.branch,1,a.desired_commit,unixepoch()
             FROM artifact_observations a
             WHERE a.observed_at=(SELECT MAX(b.observed_at) FROM artifact_observations b
                 WHERE b.workspace=a.workspace AND b.repo=a.repo AND b.branch=a.branch);
             UPDATE artifact_observations SET desired_generation=1 WHERE desired_generation=0",
        )
        .execute(&mut *migration)
        .await?;
        sqlx::raw_sql("UPDATE artifact_observations SET format_version=(SELECT format_version FROM artifact_jobs WHERE id=desired_artifact_id); UPDATE artifact_observations SET published_artifact_id=NULL WHERE published_artifact_id IS NOT NULL AND (SELECT count(*) FROM artifact_jobs j WHERE j.id=published_artifact_id AND j.workspace=artifact_observations.workspace AND j.repo=artifact_observations.repo AND j.kind=artifact_observations.kind AND j.format_version=artifact_observations.format_version AND j.state='ready' AND j.manifest IS NOT NULL AND length(trim(j.manifest))>0)=0").execute(&mut *migration).await?;
        if prior_version < 2 {
            // Legacy completion evidence predates the mandatory verifier. Keep
            // queued intent, but force any running/ready work back through the
            // new fenced verifier before it can publish.
            sqlx::raw_sql(
                "UPDATE artifact_observations SET published_artifact_id=NULL;
                 UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,
                   lease_expires_at=NULL,manifest=NULL,error=NULL,failure_class=NULL
                 WHERE state IN('running','ready');
                 UPDATE scheduler_state SET config_fingerprint='__legacy_migration_pending__'
                 WHERE id=1 AND config_fingerprint='';",
            )
            .execute(&mut *migration)
            .await?;
        }
        let invalid_after:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_observations a LEFT JOIN artifact_jobs d ON d.id=a.desired_artifact_id AND d.workspace=a.workspace AND d.repo=a.repo AND d.kind=a.kind AND d.commit_oid=a.desired_commit AND d.format_version=a.format_version AND d.format_version BETWEEN 1 AND 4294967295 LEFT JOIN artifact_jobs p ON p.id=a.published_artifact_id AND p.workspace=a.workspace AND p.repo=a.repo AND p.kind=a.kind AND p.format_version=a.format_version WHERE d.id IS NULL OR (a.published_artifact_id IS NOT NULL AND p.id IS NULL)").fetch_one(&mut *migration).await?;
        if invalid_after > 0 {
            bail!("artifact observation migration validation failed")
        }
        let invalid_job_formats: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE format_version IS NULL OR format_version NOT BETWEEN 1 AND 4294967295",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_job_formats != 0 {
            bail!("artifact scheduler contains invalid job format versions")
        }
        let fence_tables_before: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN(
               'ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')",
        )
        .fetch_one(&mut *migration)
        .await?;
        if prior_version >= 3 && fence_tables_before == 0 {
            sqlx::raw_sql(FENCE_SCHEMA_V3)
                .execute(&mut *migration)
                .await
                .context("add admission fences to transport scheduler lineage")?;
        }
        if prior_version < 3 {
            let legacy_tables: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN(
                   'ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')",
            ).fetch_one(&mut *migration).await?;
            let preserved_generation = if legacy_tables == 0 {
                0
            } else if legacy_tables == 3 && prior_version == 2 {
                let legacy_columns: i64 = sqlx::query_scalar(
                    "SELECT (SELECT count(*) FROM pragma_table_info('ready_publication_fence_sequence'))
                          +(SELECT count(*) FROM pragma_table_info('ready_publication_fences'))
                          +(SELECT count(*) FROM pragma_table_info('ready_publication_fence_members'))",
                ).fetch_one(&mut *migration).await?;
                let exact_columns: i64 = sqlx::query_scalar(
                    "SELECT (SELECT count(*) FROM pragma_table_info('ready_publication_fence_sequence') WHERE name IN('id','generation'))
                          +(SELECT count(*) FROM pragma_table_info('ready_publication_fences') WHERE name IN('token','generation','operation_id','expires_at','state'))
                          +(SELECT count(*) FROM pragma_table_info('ready_publication_fence_members') WHERE name IN('token','generation','artifact_id','manifest'))",
                ).fetch_one(&mut *migration).await?;
                if legacy_columns != 11 || exact_columns != 11 {
                    bail!(
                        "v2 Ready fence schema shape is not the released schema; manual repair required"
                    )
                }
                let legacy_sql: String = sqlx::query_scalar(
                    "SELECT group_concat(lower(sql),' ') FROM sqlite_master
                     WHERE type='table' AND name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')",
                ).fetch_one(&mut *migration).await?;
                let normalized = legacy_sql.split_whitespace().collect::<String>();
                for required in [
                    "operation_id text not null unique",
                    "check(state in('held','activation_unknown'))",
                    "foreign key(token,generation) references ready_publication_fences(token,generation) on delete cascade",
                    "foreign key(artifact_id) references artifact_jobs(id) on delete cascade",
                ] {
                    if !normalized.contains(&required.split_whitespace().collect::<String>()) {
                        bail!(
                            "v2 Ready fence schema provenance is not the released schema; manual repair required"
                        )
                    }
                }
                let live_fences: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM ready_publication_fences")
                        .fetch_one(&mut *migration)
                        .await?;
                if live_fences != 0 {
                    bail!(
                        "v2 Ready fences contain live operations without branch/attempt provenance; drain them with the v2 binary before upgrading"
                    )
                }
                let generation: i64 = sqlx::query_scalar(
                    "SELECT generation FROM ready_publication_fence_sequence WHERE id=1",
                )
                .fetch_one(&mut *migration)
                .await?;
                if generation < 0 {
                    bail!("v2 Ready fence sequence is invalid")
                }
                sqlx::raw_sql(
                    "DROP TABLE ready_publication_fence_members;
                     DROP TABLE ready_publication_fences;
                     DROP TABLE ready_publication_fence_sequence;",
                )
                .execute(&mut *migration)
                .await?;
                generation
            } else {
                bail!(
                    "mixed-version Ready fence schema detected; migration refused without mutation"
                )
            };
            sqlx::raw_sql(FENCE_SCHEMA_V3)
                .execute(&mut *migration)
                .await
                .context("migrate artifact scheduler v2 to v3 Ready fences")?;
            if preserved_generation != 0 {
                sqlx::query("UPDATE ready_publication_fence_sequence SET generation=? WHERE id=1")
                    .bind(preserved_generation)
                    .execute(&mut *migration)
                    .await?;
            }
        }
        let fence_schema: String = sqlx::query_scalar(
            "SELECT lower(sql) FROM sqlite_master WHERE type='table' AND name='ready_publication_fence_members'",
        )
        .fetch_one(&mut *migration)
        .await?;
        for required in [
            "foreign key(token,generation)",
            "references ready_publication_fences(token,generation) on delete cascade",
            "foreign key(artifact_id) references artifact_jobs(id) on delete cascade",
        ] {
            if !fence_schema
                .split_whitespace()
                .collect::<String>()
                .contains(&required.split_whitespace().collect::<String>())
            {
                bail!("ready publication fence schema provenance is invalid")
            }
        }
        let fence_sequence: i64 = sqlx::query_scalar(
            "SELECT generation FROM ready_publication_fence_sequence WHERE id=1",
        )
        .fetch_one(&mut *migration)
        .await?;
        let maximum_fence_generation: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(generation),0) FROM ready_publication_fences")
                .fetch_one(&mut *migration)
                .await?;
        if fence_sequence < maximum_fence_generation {
            bail!("ready publication fence sequence is behind persisted fence state")
        }
        let invalid_fences: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ready_publication_fences f WHERE
               trim(f.workspace)='' OR trim(f.repo)='' OR trim(f.branch)='' OR trim(f.target)='' OR trim(f.attempt_id)=''
               OR (SELECT count(*) FROM ready_publication_fence_members m WHERE m.token=f.token AND m.generation=f.generation)<>2
               OR (SELECT count(*) FROM ready_publication_fence_members m JOIN artifact_jobs j ON j.id=m.artifact_id
                   WHERE m.token=f.token AND m.generation=f.generation AND j.kind='head' AND j.state='ready'
                     AND j.manifest=m.manifest AND length(trim(m.manifest))>0
                     AND j.workspace=f.workspace AND j.repo=f.repo AND j.commit_oid=f.target)<>1
               OR (SELECT count(*) FROM ready_publication_fence_members m JOIN artifact_jobs j ON j.id=m.artifact_id
                   WHERE m.token=f.token AND m.generation=f.generation AND j.kind='full_history' AND j.state='ready'
                     AND j.manifest=m.manifest AND length(trim(m.manifest))>0
                     AND j.workspace=f.workspace AND j.repo=f.repo AND j.commit_oid=f.target)<>1
               OR (SELECT count(DISTINCT j.format_version) FROM ready_publication_fence_members m
                   JOIN artifact_jobs j ON j.id=m.artifact_id WHERE m.token=f.token AND m.generation=f.generation)<>1",
        )
        .fetch_one(&mut *migration)
        .await?;
        let orphan_members: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ready_publication_fence_members m
             LEFT JOIN ready_publication_fences f ON f.token=m.token AND f.generation=m.generation
             LEFT JOIN artifact_jobs j ON j.id=m.artifact_id WHERE f.token IS NULL OR j.id IS NULL",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_fences != 0 || orphan_members != 0 {
            bail!("ready publication fence integrity validation failed")
        }
        let provenances: Vec<(String,String,String,String,String,String)> = sqlx::query_as(
            "SELECT operation_id,workspace,repo,branch,target,attempt_id FROM ready_publication_fences",
        ).fetch_all(&mut *migration).await?;
        if provenances.into_iter().any(
            |(operation_id, workspace, repo, branch, target, attempt_id)| {
                ActivationFenceProvenance {
                    workspace,
                    repo,
                    branch,
                    target,
                    attempt_id,
                }
                .operation_id()
                    != operation_id
            },
        ) {
            bail!("ready publication fence operation provenance is invalid")
        }
        sqlx::raw_sql("DELETE FROM artifact_base_retention;
          INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank)
          SELECT id,workspace,repo,format_version,head_rank,NULL FROM (SELECT id,workspace,repo,format_version,row_number() OVER(PARTITION BY workspace,repo,format_version ORDER BY updated_at DESC,id DESC) head_rank FROM artifact_jobs WHERE kind='head' AND state='ready' AND manifest IS NOT NULL AND length(trim(manifest))>0) WHERE head_rank<=8;
          INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank)
          SELECT head_id,workspace,repo,format_version,NULL,pair_rank FROM (SELECT h.id head_id,h.workspace,h.repo,h.format_version,row_number() OVER(PARTITION BY h.workspace,h.repo,h.format_version ORDER BY CASE WHEN h.updated_at>f.updated_at THEN h.updated_at ELSE f.updated_at END DESC,CASE WHEN h.id>f.id THEN h.id ELSE f.id END DESC) pair_rank FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' AND f.state='ready' AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 WHERE h.kind='head' AND h.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0) WHERE pair_rank<=8 ON CONFLICT(artifact_id) DO UPDATE SET pair_rank=excluded.pair_rank;
          INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank)
          SELECT history_id,workspace,repo,format_version,NULL,pair_rank FROM (SELECT f.id history_id,h.workspace,h.repo,h.format_version,row_number() OVER(PARTITION BY h.workspace,h.repo,h.format_version ORDER BY CASE WHEN h.updated_at>f.updated_at THEN h.updated_at ELSE f.updated_at END DESC,CASE WHEN h.id>f.id THEN h.id ELSE f.id END DESC) pair_rank FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' AND f.state='ready' AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 WHERE h.kind='head' AND h.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0) WHERE pair_rank<=8 ON CONFLICT(artifact_id) DO UPDATE SET pair_rank=excluded.pair_rank;
          PRAGMA user_version=6")
            .execute(&mut *migration)
            .await?;
            if prior_version < 7 {
                crate::git_source_registry::migrate_sqlite_v7_in(&mut migration).await?;
            } else {
                crate::git_source_registry::validate_sqlite_v7_in(&mut migration).await?;
            }
            sqlx::query("PRAGMA user_version=7")
                .execute(&mut *migration)
                .await?;
            Ok(())
        }
        .await;
        if let Err(error) = migration_result {
            if let Err(rollback) = sqlx::query("ROLLBACK").execute(&mut *migration).await {
                return Err(error).context(format!(
                    "sqlite scheduler migration also failed to roll back; connection retired: {rollback:#}"
                ));
            }
            migration.release();
            return Err(error);
        }
        if let Err(error) = sqlx::query("COMMIT").execute(&mut *migration).await {
            return Err(error).context("commit sqlite scheduler migration; connection retired");
        }
        migration.release();
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        let required:i64=sqlx::query_scalar("SELECT count(*) FROM pragma_table_info('artifact_jobs') WHERE name IN('lease_generation','claim_attempts','retry_count','failure_class')").fetch_one(&pool).await?;
        let fence_tables: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN(
               'ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')",
        ).fetch_one(&pool).await?;
        let fence_columns: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM pragma_table_info('ready_publication_fence_sequence'))
                  +(SELECT count(*) FROM pragma_table_info('ready_publication_fences'))
                  +(SELECT count(*) FROM pragma_table_info('ready_publication_fence_members'))",
        )
        .fetch_one(&pool)
        .await?;
        let fence_foreign_keys: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pragma_foreign_key_list('ready_publication_fence_members')",
        )
        .fetch_one(&pool)
        .await?;
        if version != 7
            || required != 4
            || fence_tables != 3
            || fence_columns != 16
            || fence_foreign_keys != 3
        {
            bail!("artifact scheduler migration post-validation failed")
        }
        let base_columns:i64=sqlx::query_scalar("SELECT count(*) FROM pragma_table_info('artifact_base_retention') WHERE name IN('artifact_id','workspace','repo','format_version','head_rank','pair_rank')").fetch_one(&pool).await?;
        let invalid_base:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_base_retention b LEFT JOIN artifact_jobs j ON j.id=b.artifact_id WHERE j.id IS NULL OR j.workspace<>b.workspace OR j.repo<>b.repo OR j.format_version<>b.format_version OR (b.head_rank IS NULL AND b.pair_rank IS NULL) OR b.head_rank NOT BETWEEN 1 AND 8 OR b.pair_rank NOT BETWEEN 1 AND 8").fetch_one(&pool).await?;
        let gc_columns:i64=sqlx::query_scalar("SELECT count(*) FROM pragma_table_info('artifact_gc_sweep') WHERE (name='id' AND type='INTEGER' AND \"notnull\"=0 AND pk=1) OR (name='owner' AND type='TEXT' AND \"notnull\"=1 AND pk=0) OR (name='expires_at' AND type='INTEGER' AND \"notnull\"=1 AND pk=0)").fetch_one(&pool).await?;
        let gc_sql: String = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='artifact_gc_sweep'",
        )
        .fetch_one(&pool)
        .await?;
        let normalized_gc_sql = gc_sql
            .chars()
            .filter(|c| !c.is_whitespace())
            .flat_map(char::to_lowercase)
            .collect::<String>();
        if version != 7
            || required != 4
            || base_columns != 6
            || invalid_base != 0
            || gc_columns != 3
            || normalized_gc_sql
                != "createtableartifact_gc_sweep(idintegerprimarykeycheck(id=1),ownertextnotnull,expires_atintegernotnull)"
        {
            bail!("artifact scheduler migration post-validation failed")
        }
        let mut config = ImmediateTransaction::begin(&pool).await?;
        let limits_fingerprint = scheduler_limits_fingerprint(&limits);
        let config_result:Result<()>=async{
        let state_rows:Vec<(i64,String,String)>=sqlx::query_as("SELECT id,config_fingerprint,limits_fingerprint FROM scheduler_state").fetch_all(&mut *config).await?;
        let limits_column:i64=sqlx::query_scalar("SELECT count(*) FROM pragma_table_info('scheduler_state') WHERE name='limits_fingerprint' AND upper(type)='TEXT' AND [notnull]=1 AND dflt_value=\"''\" AND pk=0").fetch_one(&mut *config).await?;
        if state_rows.len()!=1||state_rows[0].0!=1||limits_column!=1{bail!("scheduler limits state schema or singleton is invalid")}
        let stored: String =
            sqlx::query_scalar("SELECT config_fingerprint FROM scheduler_state WHERE id=1")
                .fetch_one(&mut *config)
                .await?;
        let accepted = if stored == "__legacy_migration_pending__" {
            let unsafe_legacy_state:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM artifact_jobs WHERE state IN('running','ready') OR (manifest IS NOT NULL AND length(trim(manifest))>0))+(SELECT count(*) FROM artifact_observations WHERE published_artifact_id IS NOT NULL)").fetch_one(&mut *config).await?;
            unsafe_legacy_state==0 && sqlx::query("UPDATE scheduler_state SET config_fingerprint=? WHERE id=1 AND config_fingerprint='__legacy_migration_pending__'")
                    .bind(&fingerprint)
                    .execute(&mut *config)
                    .await?
                    .rows_affected()==1
        } else if stored.is_empty() {
            let existing:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM artifact_jobs)+(SELECT count(*) FROM branch_observations)+(SELECT count(*) FROM artifact_observations)+(SELECT count(*) FROM artifact_consumers)").fetch_one(&mut *config).await?;
            existing==0 && sqlx::query("UPDATE scheduler_state SET config_fingerprint=? WHERE id=1 AND config_fingerprint=''").bind(&fingerprint).execute(&mut *config).await?.rows_affected()==1
        } else {
            stored == fingerprint
        };
        if !accepted {
            bail!("scheduler running-limit configuration differs from existing fleet")
        }
        let stored: String =
            sqlx::query_scalar("SELECT config_fingerprint FROM scheduler_state WHERE id=1")
                .fetch_one(&mut *config)
                .await?;
        if stored != fingerprint {
            bail!("scheduler configuration CAS verification failed")
        }
        let stored_limits: String =
            sqlx::query_scalar("SELECT limits_fingerprint FROM scheduler_state WHERE id=1")
                .fetch_one(&mut *config)
                .await?;
        if stored_limits.is_empty() {
            if sqlx::query("UPDATE scheduler_state SET limits_fingerprint=? WHERE id=1 AND limits_fingerprint=''")
                .bind(&limits_fingerprint)
                .execute(&mut *config)
                .await?
                .rows_affected()
                != 1
            {
                bail!("scheduler limits fingerprint CAS failed")
            }
        } else if stored_limits != limits_fingerprint {
            bail!("scheduler limits fingerprint differs from existing fleet")
        }
        let sealed_limits:String=sqlx::query_scalar("SELECT limits_fingerprint FROM scheduler_state WHERE id=1").fetch_one(&mut *config).await?;
        if sealed_limits!=limits_fingerprint||sealed_limits.len()!=64||sealed_limits.as_bytes().iter().any(|byte|!(byte.is_ascii_digit()||(*byte>=b'a'&&*byte<=b'f'))){bail!("scheduler limits fingerprint sealing failed")}
        Ok(())}.await;
        finish(config, config_result).await?;
        let completion_sealer = Arc::new(CompletionSealAuthority::new(verifier_id)?);
        Ok(Self {
            pool,
            limits,
            verifier,
            completion_sealer,
        })
    }

    pub async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome> {
        validate_format_version(key.format_version)?;
        let mut c = self.immediate().await?;
        let result = self.schedule_in(&mut c, key).await;
        finish(c, result).await
    }

    pub async fn acquire_gc_sweep(&self, owner: &str, ttl_secs: i64) -> Result<bool> {
        validate_gc_sweep(owner, ttl_secs)?;
        let mut c = self.immediate().await?;
        let result: Result<bool> = async {
            let now = db_now(&mut c).await?;
            Ok(sqlx::query("INSERT INTO artifact_gc_sweep(id,owner,expires_at) VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET owner=excluded.owner,expires_at=excluded.expires_at WHERE artifact_gc_sweep.expires_at<=? OR artifact_gc_sweep.owner=excluded.owner")
                .bind(owner).bind(now + ttl_secs).bind(now).execute(&mut *c).await?.rows_affected() == 1)
        }.await;
        finish(c, result).await
    }

    pub async fn renew_gc_sweep(&self, owner: &str, ttl_secs: i64) -> Result<bool> {
        validate_gc_sweep(owner, ttl_secs)?;
        let mut c = self.immediate().await?;
        let result: Result<bool> = async {
            let now = db_now(&mut c).await?;
            Ok(sqlx::query(
                "UPDATE artifact_gc_sweep SET expires_at=? WHERE id=1 AND owner=? AND expires_at>?",
            )
            .bind(now + ttl_secs)
            .bind(owner)
            .bind(now)
            .execute(&mut *c)
            .await?
            .rows_affected()
                == 1)
        }
        .await;
        finish(c, result).await
    }

    pub async fn release_gc_sweep(&self, owner: &str) -> Result<()> {
        validate_gc_sweep(owner, 1)?;
        let mut c = self.immediate().await?;
        let result: Result<()> = async {
            sqlx::query("DELETE FROM artifact_gc_sweep WHERE id=1 AND owner=?")
                .bind(owner)
                .execute(&mut *c)
                .await?;
            Ok(())
        }
        .await;
        finish(c, result).await
    }

    pub async fn lock_gc_delete_batch(
        &self,
        owner: &str,
    ) -> Result<Box<dyn crate::artifact_scheduler_backend::GcDeleteFence>> {
        validate_gc_sweep(owner, 1)?;
        let mut c = self.immediate().await?;
        let now = db_now(&mut c).await?;
        let held: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_gc_sweep WHERE id=1 AND owner=? AND expires_at>?",
        )
        .bind(owner)
        .bind(now)
        .fetch_one(&mut *c)
        .await?;
        if held != 1 {
            if let Err(rollback) = sqlx::query("ROLLBACK").execute(&mut *c).await {
                return Err(rollback)
                    .context("rollback failed GC delete fence; connection retired");
            }
            c.release();
            bail!("remote GC does not own the live publication fence")
        }
        Ok(Box::new(SqliteGcDeleteFence(Some(c))))
    }

    pub async fn subscribe_consumer(
        &self,
        key: &ArtifactKey,
        consumer_id: &str,
        ttl_secs: i64,
    ) -> Result<ScheduleOutcome> {
        validate_format_version(key.format_version)?;
        crate::artifact_scheduler_backend::validate_public_consumer_id(consumer_id)?;
        if !(2..=86400).contains(&ttl_secs) {
            bail!("consumer subscription TTL is invalid")
        }
        let mut c = self.immediate().await?;
        let result: Result<ScheduleOutcome> = async {
            assert_gc_unfenced(&mut c).await?;
            let outcome = self.schedule_in(&mut c, key).await?;
            let now=db_now(&mut c).await?;
            sqlx::query(
                "INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?) ON CONFLICT(artifact_id,consumer_id) DO UPDATE SET expires_at=excluded.expires_at",
            )
            .bind(outcome_id(&outcome))
            .bind(consumer_id)
            .bind(now+ttl_secs)
            .execute(&mut *c)
            .await?;
            Ok(outcome)
        }
        .await;
        finish(c, result).await
    }
    pub async fn release_consumer(&self, artifact_id: i64, consumer_id: &str) -> Result<()> {
        crate::artifact_scheduler_backend::validate_public_consumer_id(consumer_id)?;
        let mut c = self.immediate().await?;
        let result:Result<()>=async{
            sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?").bind(artifact_id).bind(consumer_id).execute(&mut *c).await?;
            sqlx::query("DELETE FROM artifact_jobs WHERE id=? AND state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)").bind(artifact_id).execute(&mut *c).await?;Ok(())
        }.await;
        finish(c, result).await
    }

    pub async fn register_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
        ttl: i64,
    ) -> Result<()> {
        crate::artifact_scheduler_backend::validate_transport_lease_identity(
            root, session, workspace, repo, ttl,
        )?;
        let mut c = self.immediate().await?;
        let result:Result<()>=async{
            assert_gc_unfenced(&mut c).await?;
            let foreign:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_transport_leases WHERE session_id=? AND (workspace<>? OR repo<>?)").bind(session).bind(workspace).bind(repo).fetch_one(&mut *c).await?;
            if foreign != 0 { bail!("transport session is already bound to another repository") }
            let now=db_now(&mut c).await?;
            let changed=sqlx::query("INSERT INTO artifact_transport_leases(root_hash,session_id,workspace,repo,expires_at) VALUES(?,?,?,?,?) ON CONFLICT(root_hash,session_id) DO UPDATE SET expires_at=excluded.expires_at WHERE artifact_transport_leases.workspace=excluded.workspace AND artifact_transport_leases.repo=excluded.repo").bind(root).bind(session).bind(workspace).bind(repo).bind(now+ttl).execute(&mut *c).await?.rows_affected();
            if changed != 1 { bail!("transport root identity conflict") } Ok(())}.await;
        finish(c, result).await
    }
    pub async fn renew_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
        ttl: i64,
    ) -> Result<bool> {
        crate::artifact_scheduler_backend::validate_transport_lease_identity(
            root, session, workspace, repo, ttl,
        )?;
        let mut c = self.immediate().await?;
        let result:Result<bool>=async{let now=db_now(&mut c).await?;Ok(sqlx::query("UPDATE artifact_transport_leases SET expires_at=? WHERE root_hash=? AND session_id=? AND workspace=? AND repo=? AND expires_at>?").bind(now+ttl).bind(root).bind(session).bind(workspace).bind(repo).bind(now).execute(&mut *c).await?.rows_affected()==1)}.await;
        finish(c, result).await
    }
    pub async fn release_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
    ) -> Result<bool> {
        crate::artifact_scheduler_backend::validate_transport_lease_identity(
            root, session, workspace, repo, 1,
        )?;
        let mut c = self.immediate().await?;
        let result:Result<bool>=async{Ok(sqlx::query("DELETE FROM artifact_transport_leases WHERE root_hash=? AND session_id=? AND workspace=? AND repo=?").bind(root).bind(session).bind(workspace).bind(repo).execute(&mut *c).await?.rows_affected()==1)}.await;
        finish(c, result).await
    }
    pub async fn live_transport_roots_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<crate::artifact_scheduler_backend::TransportRootLease>> {
        if limit == 0 || limit > crate::artifact_scheduler_backend::TRANSPORT_ROOT_PAGE_MAX {
            bail!("transport root page limit is invalid")
        }
        let mut c = self.pool.acquire().await?;
        let now = db_now(&mut c).await?;
        let rows = match after {
            Some((root, session)) => {
                crate::cas::Cas::validate_artifact_id(root)?;
                sqlx::query("SELECT root_hash,session_id,workspace,repo,expires_at FROM artifact_transport_leases WHERE expires_at>? AND (root_hash>? OR (root_hash=? AND session_id>?)) ORDER BY root_hash,session_id LIMIT ?")
                    .bind(now).bind(root).bind(root).bind(session).bind(limit as i64).fetch_all(&mut *c).await?
            }
            None => sqlx::query("SELECT root_hash,session_id,workspace,repo,expires_at FROM artifact_transport_leases WHERE expires_at>? ORDER BY root_hash,session_id LIMIT ?")
                .bind(now).bind(limit as i64).fetch_all(&mut *c).await?,
        };
        rows.into_iter()
            .map(|r| {
                Ok(crate::artifact_scheduler_backend::TransportRootLease {
                    root_hash: r.try_get("root_hash")?,
                    session_id: r.try_get("session_id")?,
                    workspace: r.try_get("workspace")?,
                    repo: r.try_get("repo")?,
                    expires_at: r.try_get("expires_at")?,
                })
            })
            .collect()
    }
    pub async fn live_scheduler_roots_page(
        &self,
        after: Option<i64>,
        limit: u32,
    ) -> Result<Vec<crate::artifact_scheduler_backend::SchedulerGcRoot>> {
        if limit == 0 || limit > crate::artifact_scheduler_backend::TRANSPORT_ROOT_PAGE_MAX {
            bail!("scheduler GC root page limit is invalid")
        }
        if after.is_some_and(|id| id < 0) {
            bail!("scheduler GC cursor is invalid")
        }
        let mut c = self.pool.acquire().await?;
        let now = db_now(&mut c).await?;
        // Seek each root source directly.  Driving this page from artifact_jobs
        // would scan every unrooted job between sparse live ids (potentially the
        // whole fleet) before finding `limit` rows.
        let rows=sqlx::query("WITH candidates(id) AS (SELECT published_artifact_id FROM (SELECT published_artifact_id FROM artifact_observations WHERE published_artifact_id>? ORDER BY published_artifact_id LIMIT ?) UNION ALL SELECT artifact_id FROM (SELECT artifact_id FROM artifact_consumers WHERE artifact_id>? AND expires_at>? ORDER BY artifact_id LIMIT ?) UNION ALL SELECT artifact_id FROM (SELECT artifact_id FROM artifact_base_retention WHERE artifact_id>? ORDER BY artifact_id LIMIT ?)), page_ids(id) AS (SELECT DISTINCT id FROM candidates ORDER BY id LIMIT ?) SELECT j.id,j.workspace,j.repo,j.commit_oid,j.kind,j.format_version,j.manifest FROM page_ids p JOIN artifact_jobs j ON j.id=p.id WHERE j.state='ready' AND j.manifest IS NOT NULL AND length(trim(j.manifest))>0 ORDER BY j.id")
            .bind(after.unwrap_or(0)).bind(limit as i64).bind(after.unwrap_or(0)).bind(now).bind(limit as i64).bind(after.unwrap_or(0)).bind(limit as i64).bind(limit as i64).fetch_all(&mut *c).await?;
        rows.into_iter()
            .map(|row| {
                Ok(crate::artifact_scheduler_backend::SchedulerGcRoot {
                    artifact_id: row.try_get("id")?,
                    key: ArtifactKey {
                        workspace: row.try_get("workspace")?,
                        repo: row.try_get("repo")?,
                        commit: row.try_get("commit_oid")?,
                        kind: ArtifactKind::parse(row.try_get("kind")?)?,
                        format_version: u32::try_from(row.try_get::<i64, _>("format_version")?)
                            .context("scheduler GC root format")?,
                    },
                    manifest: row.try_get("manifest")?,
                })
            })
            .collect()
    }

    pub async fn live_source_objects_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<crate::git_source_registry::SourceGcObject>> {
        if limit == 0 || limit > crate::git_source_registry::SOURCE_ROOT_PAGE_MAX {
            bail!("source GC page limit is invalid")
        }
        let (after_hash, after_owner) = after.unwrap_or(("", ""));
        let rows=sqlx::query("WITH objects(hash,len,owner) AS (SELECT root_hash,root_len,'r:'||root_hash FROM git_source_roots UNION ALL SELECT child_hash,child_len,'r:'||root_hash||':'||printf('%020d',ordinal) FROM git_source_members UNION ALL SELECT root_hash,root_len,'a:'||token FROM git_source_acquisitions WHERE state='activation_unknown' OR (state='graph_published' AND expires_at>unixepoch()) UNION ALL SELECT m.child_hash,m.child_len,'a:'||m.token||':'||printf('%020d',m.ordinal) FROM git_source_acquisition_members m JOIN git_source_acquisitions a ON a.token=m.token WHERE a.state='activation_unknown' OR (a.state='graph_published' AND a.expires_at>unixepoch())) SELECT hash,len,owner FROM objects WHERE hash>? OR (hash=? AND owner>?) ORDER BY hash,owner LIMIT ?")
            .bind(after_hash).bind(after_hash).bind(after_owner).bind(limit as i64).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(crate::git_source_registry::SourceGcObject {
                    hash: row.try_get("hash")?,
                    len: u64::try_from(row.try_get::<i64, _>("len")?)
                        .context("source GC object length")?,
                    owner: row.try_get("owner")?,
                })
            })
            .collect()
    }

    /// Atomically accept an observation and subscribe every requested kind.
    /// `expected_generation` is the durable CAS token from
    /// [`Self::observation_snapshot`]. A different-commit loser must re-resolve
    /// upstream; an identical commit with all requested kinds/formats already
    /// observed returns [`ObservationOutcome::Unchanged`] without generation
    /// churn or scheduling.
    pub async fn observe(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
        commit: &str,
        kinds: &[ArtifactKind],
        format_version: u32,
        expected_generation: Option<u64>,
    ) -> Result<ObservationOutcome> {
        validate_observation_identity(workspace, repo, branch, "write")?;
        validate_resolved_commit(commit)?;
        validate_format_version(format_version)?;
        if kinds.is_empty() {
            bail!("observation requests no artifact kinds")
        }
        let mut unique = Vec::new();
        for &k in kinds {
            if !unique.contains(&k) {
                unique.push(k)
            }
        }
        let mut c = self.immediate().await?;
        let result:Result<ObservationOutcome>=async{
   assert_gc_unfenced(&mut c).await?;
   let current:Option<(i64,String)>=sqlx::query_as("SELECT generation,desired_commit FROM branch_observations WHERE workspace=? AND repo=? AND branch=?").bind(workspace).bind(repo).bind(branch).fetch_optional(&mut *c).await?;
   let current_generation=current.as_ref().map(|(v,_)|*v as u64);
   let same_commit=current.as_ref().is_some_and(|(_,current_commit)|current_commit==commit);
   let mut fully_observed=same_commit;
   if same_commit { for kind in &unique { let present:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_observations WHERE workspace=? AND repo=? AND branch=? AND kind=? AND desired_commit=? AND format_version=?").bind(workspace).bind(repo).bind(branch).bind(kind.as_str()).bind(commit).bind(format_version as i64).fetch_one(&mut *c).await?; fully_observed &= present==1; } }
   if fully_observed {
    return Ok(ObservationOutcome::Unchanged{generation:current_generation.context("existing observation has no generation")?})
   }
   let current=current_generation;
   if current!=expected_generation{return Ok(ObservationOutcome::Stale{current_generation:current.unwrap_or(0)})}
   let generation=current.unwrap_or(0).checked_add(1).context("observation generation overflow")?;
   // Credit superseded queued work before capacity admission. The transaction
   // restores it if the replacement batch later fails.
   for kind in &unique {sqlx::query("DELETE FROM artifact_jobs WHERE state='queued' AND id IN(SELECT desired_artifact_id FROM artifact_observations WHERE workspace=? AND repo=? AND branch=? AND kind=?) AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations WHERE NOT(workspace=? AND repo=? AND branch=? AND kind=?)) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)")
    .bind(workspace).bind(repo).bind(branch).bind(kind.as_str()).bind(workspace).bind(repo).bind(branch).bind(kind.as_str()).execute(&mut *c).await?;}
   // Preflight all new jobs before inserting any, so capacity failure is atomic.
   self.preflight_batch(&mut c,workspace,repo,commit,&unique,format_version).await?;
   let mut artifacts=Vec::new();
   for kind in unique {
    let key=ArtifactKey{workspace:workspace.into(),repo:repo.into(),commit:commit.into(),kind,format_version};
    let outcome=self.schedule_in_unchecked(&mut c,&key).await?;
    let id=outcome_id(&outcome);
    let observed_at=db_now(&mut c).await?;
    sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES(?,?,?,?,?,?,?,CASE WHEN (SELECT state FROM artifact_jobs WHERE id=?)='ready' THEN ? ELSE NULL END,?,?) ON CONFLICT(workspace,repo,branch,kind) DO UPDATE SET desired_commit=excluded.desired_commit,desired_artifact_id=excluded.desired_artifact_id,desired_generation=excluded.desired_generation,published_artifact_id=CASE WHEN (SELECT state FROM artifact_jobs WHERE id=excluded.desired_artifact_id)='ready' THEN excluded.desired_artifact_id WHEN artifact_observations.format_version=excluded.format_version THEN artifact_observations.published_artifact_id ELSE NULL END,format_version=excluded.format_version,observed_at=excluded.observed_at")
      .bind(workspace).bind(repo).bind(branch).bind(kind.as_str()).bind(commit).bind(id).bind(generation as i64).bind(id).bind(id).bind(format_version as i64).bind(observed_at).execute(&mut *c).await?;
    artifacts.push((kind,outcome));
   }
   let now=db_now(&mut c).await?;
   sqlx::query("INSERT INTO branch_observations(workspace,repo,branch,generation,desired_commit,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(workspace,repo,branch) DO UPDATE SET generation=excluded.generation,desired_commit=excluded.desired_commit,updated_at=excluded.updated_at")
    .bind(workspace).bind(repo).bind(branch).bind(generation as i64).bind(commit).bind(now).execute(&mut *c).await?;
   // Superseded queued work is useless unless another branch or an explicit
   // clone consumer still subscribes to it. Prune inside the observation txn so
   // a T1/T2 flood cannot consume backlog ahead of T3.
   sqlx::query("DELETE FROM artifact_jobs WHERE workspace=? AND repo=? AND state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)")
    .bind(workspace).bind(repo).execute(&mut *c).await?;
   Ok(ObservationOutcome::Accepted{generation,artifacts})
  }.await;
        finish(c, result).await
    }

    /// Read the CAS token and commit that must bracket an upstream resolution.
    /// Passing this generation to [`Self::observe`] prevents a late fetch from
    /// overwriting a newer force-push observation; an identical commit is an
    /// atomic no-op even if another observer won first.
    pub async fn observation_snapshot(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<ObservationSnapshot> {
        validate_observation_identity(workspace, repo, branch, "snapshot")?;
        let row: Option<(i64, String)> = sqlx::query_as(
            "SELECT generation,desired_commit FROM branch_observations WHERE workspace=? AND repo=? AND branch=?",
        )
        .bind(workspace)
        .bind(repo)
        .bind(branch)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            Some((generation, commit)) => ObservationSnapshot::new(
                workspace,
                repo,
                branch,
                Some(generation as u64),
                Some(commit),
            ),
            None => ObservationSnapshot::new(workspace, repo, branch, None, None),
        })
    }

    pub async fn retry_failed(&self, key: &ArtifactKey) -> Result<RetryOutcome> {
        let mut c = self.immediate().await?;
        let result:Result<RetryOutcome>=async{
   let row:Option<(i64,String,Option<String>,i64)>=sqlx::query_as("SELECT id,state,failure_class,retry_count FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?")
    .bind(&key.workspace).bind(&key.repo).bind(&key.commit).bind(key.kind.as_str()).bind(key.format_version as i64).fetch_optional(&mut *c).await?;
   let Some((id,state,class,retries))=row else{return Ok(RetryOutcome::NotFailed)};
   if state!="failed"{return Ok(RetryOutcome::NotFailed)}
   let class=FailureClass::parse(class.as_deref().unwrap_or("permanent"))?;
   if class!=FailureClass::Retryable{return Ok(RetryOutcome::NotRetryable(class))}
   if retries as u32>=self.limits.max_manual_retries{return Ok(RetryOutcome::Exhausted)}
   self.preflight_capacity(&mut c,key.kind,&key.workspace,1).await?; let now=db_now(&mut c).await?;
   sqlx::query("UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,retry_count=retry_count+1,error=NULL,failure_class=NULL,updated_at=? WHERE id=? AND state='failed'").bind(now).bind(id).execute(&mut *c).await?;
   Ok(RetryOutcome::Requeued(id))
  }.await;
        finish(c, result).await
    }

    pub async fn quarantine_ready(
        &self,
        id: i64,
        expected_manifest: Option<&str>,
        error: &str,
    ) -> Result<QuarantineOutcome> {
        if expected_manifest.is_some_and(|manifest| manifest.trim().is_empty()) {
            bail!("expected quarantine manifest is empty")
        }
        if error.trim().is_empty() {
            bail!("artifact quarantine error is empty")
        }
        let mut c = self.immediate().await?;
        let result: Result<QuarantineOutcome> = async {
            let row: Option<(String, Option<String>, i64)> =
                sqlx::query_as("SELECT state,manifest,retry_count FROM artifact_jobs WHERE id=?")
                    .bind(id)
                    .fetch_optional(&mut *c)
                    .await?;
            let Some((state, manifest, retries)) = row else {
                return Ok(QuarantineOutcome::LostRace);
            };
            if state != "ready" || manifest.as_deref() != expected_manifest {
                return Ok(QuarantineOutcome::LostRace);
            }
            let now = db_now(&mut c).await?;
            let fenced: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM ready_publication_fence_members m
                 JOIN ready_publication_fences f ON f.token=m.token AND f.generation=m.generation
                 WHERE m.artifact_id=? AND (f.state='activation_unknown' OR f.expires_at>?)",
            )
            .bind(id)
            .bind(now)
            .fetch_one(&mut *c)
            .await?;
            if fenced != 0 {
                return Ok(QuarantineOutcome::LostRace);
            }
            sqlx::query(
                "UPDATE artifact_observations SET published_artifact_id=NULL
                 WHERE published_artifact_id=?",
            )
            .bind(id)
            .execute(&mut *c)
            .await?;
            if retries as u32 >= self.limits.max_manual_retries {
                let changed = sqlx::query(
                    "UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,
                       lease_expires_at=NULL,error=?,failure_class='retryable',updated_at=?
                     WHERE id=? AND state='ready' AND manifest IS ?",
                )
                .bind(error)
                .bind(now)
                .bind(id)
                .bind(expected_manifest)
                .execute(&mut *c)
                .await?
                .rows_affected();
                return Ok(if changed == 1 {
                    QuarantineOutcome::Exhausted
                } else {
                    QuarantineOutcome::LostRace
                });
            }
            let changed = sqlx::query(
                "UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,
                   lease_expires_at=NULL,manifest=NULL,retry_count=retry_count+1,error=?,
                   failure_class=NULL,updated_at=?
                 WHERE id=? AND state='ready' AND manifest IS ?",
            )
            .bind(error)
            .bind(now)
            .bind(id)
            .bind(expected_manifest)
            .execute(&mut *c)
            .await?
            .rows_affected();
            Ok(if changed == 1 {
                QuarantineOutcome::Requeued(id)
            } else {
                QuarantineOutcome::LostRace
            })
        }
        .await;
        finish(c, result).await
    }

    pub async fn fence_ready_publications(
        &self,
        expected: &[(i64, Option<String>)],
        provenance: &ActivationFenceProvenance,
        ttl_secs: i64,
    ) -> Result<Option<ReadyPublicationFence>> {
        let operation_id = provenance.operation_id();
        if expected.len() != 2
            || expected[0].0 == expected[1].0
            || [
                &provenance.workspace,
                &provenance.repo,
                &provenance.branch,
                &provenance.target,
                &provenance.attempt_id,
            ]
            .iter()
            .any(|value| value.trim().is_empty())
            || !(1..=3600).contains(&ttl_secs)
        {
            bail!("invalid Ready publication fence")
        }
        let token = hex::encode(rand::random::<[u8; 32]>());
        let mut c = self.immediate().await?;
        let result: Result<Option<ReadyPublicationFence>> = async {
            let now = db_now(&mut c).await?;
            let existing: Option<(String, i64, i64, String, String, String, String, String, String)> = sqlx::query_as(
                "SELECT token,generation,expires_at,state,workspace,repo,branch,target,attempt_id
                 FROM ready_publication_fences WHERE operation_id=?",
            )
            .bind(&operation_id)
            .fetch_optional(&mut *c)
            .await?;
            if let Some((_, _, expires_at, ref state, ref workspace, ref repo, ref branch, ref target, ref attempt)) = existing {
                if workspace!=&provenance.workspace || repo!=&provenance.repo || branch!=&provenance.branch
                    || target!=&provenance.target || attempt!=&provenance.attempt_id {
                    bail!("activation operation provenance mismatch")
                }
                if expires_at > now && state == "held" {
                    return Ok(None);
                }
            }
            for (id, manifest) in expected {
                let current: Option<(String, String, String, String, i64, String, Option<String>)> = sqlx::query_as(
                    "SELECT workspace,repo,commit_oid,kind,format_version,state,manifest FROM artifact_jobs WHERE id=?",
                )
                .bind(id)
                .fetch_optional(&mut *c)
                .await?;
                let Some((workspace, repo, commit, kind, format, state, current_manifest)) = current else { return Ok(None) };
                if state != "ready" || current_manifest != *manifest || manifest.as_deref().is_none_or(|m| m.trim().is_empty())
                    || workspace != provenance.workspace || repo != provenance.repo || commit != provenance.target {
                    return Ok(None);
                }
                if !matches!(kind.as_str(), "head" | "full_history") || format <= 0 { return Ok(None) }
            }
            let typed: Vec<(String, i64)> = sqlx::query_as(
                "SELECT kind,format_version FROM artifact_jobs WHERE id IN(?,?) ORDER BY kind",
            ).bind(expected[0].0).bind(expected[1].0).fetch_all(&mut *c).await?;
            if typed.len()!=2 || typed[0].0!="full_history" || typed[1].0!="head" || typed[0].1!=typed[1].1 { return Ok(None) }
            if let Some((ref old_token, old_generation, _, ref state, ..)) = existing {
                if state == "activation_unknown" {
                    let members: Vec<(i64, Option<String>)> = sqlx::query_as(
                        "SELECT artifact_id,manifest FROM ready_publication_fence_members
                         WHERE token=? AND generation=? ORDER BY artifact_id",
                    )
                    .bind(old_token)
                    .bind(old_generation)
                    .fetch_all(&mut *c)
                    .await?;
                    let mut expected_sorted = expected.to_vec();
                    expected_sorted.sort_by_key(|member| member.0);
                    if members != expected_sorted {
                        bail!("activation recovery fence membership does not match operation")
                    }
                    sqlx::query(
                        "UPDATE ready_publication_fences SET expires_at=?
                         WHERE token=? AND generation=? AND operation_id=? AND state='activation_unknown'",
                    )
                    .bind(now.saturating_add(ttl_secs))
                    .bind(old_token)
                    .bind(old_generation)
                    .bind(&operation_id)
                    .execute(&mut *c)
                    .await?;
                    return Ok(Some(ReadyPublicationFence::new(
                        old_token.clone(),
                        old_generation as u64,
                        operation_id.clone(),
                        provenance.clone(),
                        expected.to_vec(),
                    )));
                }
            }
            if let Some((old_token, old_generation, ..)) = existing {
                sqlx::query(
                    "DELETE FROM ready_publication_fence_members WHERE token=? AND generation=?",
                )
                .bind(&old_token)
                .bind(old_generation)
                .execute(&mut *c)
                .await?;
                sqlx::query(
                    "DELETE FROM ready_publication_fences WHERE token=? AND generation=? AND operation_id=?",
                )
                .bind(&old_token)
                .bind(old_generation)
                .bind(&operation_id)
                .execute(&mut *c)
                .await?;
            }
            let prior_generation: i64 = sqlx::query_scalar(
                "SELECT generation FROM ready_publication_fence_sequence WHERE id=1",
            )
            .fetch_one(&mut *c)
            .await?;
            let generation = prior_generation
                .checked_add(1)
                .context("Ready publication fence generation exhausted")?;
            sqlx::query(
                "UPDATE ready_publication_fence_sequence SET generation=? WHERE id=1 AND generation=?",
            )
            .bind(generation)
            .bind(prior_generation)
            .execute(&mut *c)
            .await?;
            let expires_at = now.saturating_add(ttl_secs);
            sqlx::query(
                "INSERT INTO ready_publication_fences(token,generation,operation_id,workspace,repo,branch,target,attempt_id,expires_at,state)
                 VALUES(?,?,?,?,?,?,?,?,?, 'held')",
            )
            .bind(&token)
            .bind(generation)
            .bind(&operation_id)
            .bind(&provenance.workspace)
            .bind(&provenance.repo)
            .bind(&provenance.branch)
            .bind(&provenance.target)
            .bind(&provenance.attempt_id)
            .bind(expires_at)
            .execute(&mut *c)
            .await?;
            for (id, manifest) in expected {
                sqlx::query(
                    "INSERT INTO ready_publication_fence_members(token,generation,artifact_id,manifest)
                     VALUES(?,?,?,?)",
                )
                .bind(&token)
                .bind(generation)
                .bind(id)
                .bind(manifest)
                .execute(&mut *c)
                .await?;
            }
            Ok(Some(ReadyPublicationFence::new(
                token,
                generation as u64,
                operation_id,
                provenance.clone(),
                expected.to_vec(),
            )))
        }
        .await;
        finish(c, result).await
    }

    pub async fn mark_activation_unknown(
        &self,
        fence: &ReadyPublicationFence,
        ttl_secs: i64,
    ) -> Result<bool> {
        if !(1..=3600).contains(&ttl_secs) {
            bail!("activation fence TTL is invalid")
        }
        let (token, generation, operation_id, expected) = fence.parts();
        let mut c = self.immediate().await?;
        let result: Result<bool> = async {
            let members: Vec<(i64, Option<String>)> = sqlx::query_as(
                "SELECT artifact_id,manifest FROM ready_publication_fence_members
                 WHERE token=? AND generation=? ORDER BY artifact_id",
            )
            .bind(token)
            .bind(generation as i64)
            .fetch_all(&mut *c)
            .await?;
            let mut expected_sorted = expected.to_vec();
            expected_sorted.sort_by_key(|member| member.0);
            if members != expected_sorted {
                return Ok(false);
            }
            let expires_at = db_now(&mut c).await?.saturating_add(ttl_secs);
            Ok(sqlx::query(
                "UPDATE ready_publication_fences SET state='activation_unknown',expires_at=?
                 WHERE token=? AND generation=? AND operation_id=?",
            )
            .bind(expires_at)
            .bind(token)
            .bind(generation as i64)
            .bind(operation_id)
            .execute(&mut *c)
            .await?
            .rows_affected()
                == 1)
        }
        .await;
        finish(c, result).await
    }

    pub async fn recover_activation_fence(
        &self,
        provenance: &ActivationFenceProvenance,
    ) -> Result<Option<ReadyPublicationFence>> {
        let operation_id = provenance.operation_id();
        let row: Option<(String, i64)> = sqlx::query_as(
            "SELECT token,generation FROM ready_publication_fences
             WHERE operation_id=? AND workspace=? AND repo=? AND branch=? AND target=? AND attempt_id=?
               AND state='activation_unknown'",
        )
        .bind(&operation_id)
        .bind(&provenance.workspace)
        .bind(&provenance.repo)
        .bind(&provenance.branch)
        .bind(&provenance.target)
        .bind(&provenance.attempt_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some((token, generation)) = row else {
            return Ok(None);
        };
        let expected = sqlx::query_as(
            "SELECT artifact_id,manifest FROM ready_publication_fence_members
             WHERE token=? AND generation=? ORDER BY artifact_id",
        )
        .bind(&token)
        .bind(generation)
        .fetch_all(&self.pool)
        .await?;
        if expected.len() != 2 {
            bail!("activation recovery fence is not an exact pair")
        }
        Ok(Some(ReadyPublicationFence::new(
            token,
            generation as u64,
            operation_id,
            provenance.clone(),
            expected,
        )))
    }

    pub async fn unknown_activation_fences_page(
        &self,
        after_generation: Option<u64>,
        limit: usize,
    ) -> Result<UnknownActivationFencePage> {
        if !(1..=128).contains(&limit) {
            bail!("unknown activation fence page limit is invalid")
        }
        let after = after_generation.unwrap_or(0);
        if after > i64::MAX as u64 {
            bail!("unknown activation fence cursor is invalid")
        }
        let rows: Vec<(String, i64, String, String, String, String, String, String)> =
            sqlx::query_as(
                "SELECT token,generation,operation_id,workspace,repo,branch,target,attempt_id
             FROM ready_publication_fences WHERE state='activation_unknown' AND generation>?
             ORDER BY generation LIMIT ?",
            )
            .bind(after as i64)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        let mut fences = Vec::with_capacity(rows.len());
        for (token, generation, operation_id, workspace, repo, branch, target, attempt_id) in rows {
            let provenance = ActivationFenceProvenance {
                workspace,
                repo,
                branch,
                target,
                attempt_id,
            };
            if provenance.operation_id() != operation_id {
                bail!("unknown activation fence operation provenance is invalid")
            }
            let expected = sqlx::query_as(
                "SELECT artifact_id,manifest FROM ready_publication_fence_members
                 WHERE token=? AND generation=? ORDER BY artifact_id",
            )
            .bind(&token)
            .bind(generation)
            .fetch_all(&self.pool)
            .await?;
            if expected.len() != 2 {
                bail!("unknown activation fence is not an exact pair")
            }
            fences.push(ReadyPublicationFence::new(
                token,
                generation as u64,
                operation_id,
                provenance,
                expected,
            ));
        }
        let next_generation = (fences.len() == limit).then(|| fences.last().unwrap().generation);
        Ok(UnknownActivationFencePage {
            fences,
            next_generation,
        })
    }

    pub async fn release_ready_publication_fence(
        &self,
        fence: ReadyPublicationFence,
    ) -> Result<()> {
        let (token, generation, operation_id, expected) = fence.parts();
        let mut c = self.immediate().await?;
        let result: Result<()> = async {
            let members: Vec<(i64, Option<String>)> = sqlx::query_as(
                "SELECT artifact_id,manifest FROM ready_publication_fence_members
                 WHERE token=? AND generation=? ORDER BY artifact_id",
            )
            .bind(token)
            .bind(generation as i64)
            .fetch_all(&mut *c)
            .await?;
            let mut expected_sorted = expected.to_vec();
            expected_sorted.sort_by_key(|member| member.0);
            if members != expected_sorted {
                return Ok(());
            }
            sqlx::query(
                "DELETE FROM ready_publication_fence_members WHERE token=? AND generation=?",
            )
            .bind(token)
            .bind(generation as i64)
            .execute(&mut *c)
            .await?;
            sqlx::query(
                "DELETE FROM ready_publication_fences
                 WHERE token=? AND generation=? AND operation_id=?",
            )
            .bind(token)
            .bind(generation as i64)
            .bind(operation_id)
            .execute(&mut *c)
            .await?;
            Ok(())
        }
        .await;
        finish(c, result).await
    }

    pub async fn claim(&self, owner: &str, lease_secs: i64) -> Result<Option<ClaimedArtifact>> {
        validate_lease(owner, lease_secs)?;
        let mut c = self.immediate().await?;
        let result:Result<Option<ClaimedArtifact>>=async{
   let total:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state='running'").fetch_one(&mut *c).await?;
   if total as usize>=self.limits.total_running{return Ok(None)}
   let (cursor,workspace_cursor):(i64,String)=sqlx::query_as("SELECT fairness_cursor,workspace_cursor FROM scheduler_state WHERE id=1").fetch_one(&mut *c).await?;
   // Durable weighted round-robin: HEAD receives two lanes, expensive kinds one each.
   let lanes=[ArtifactKind::Head,ArtifactKind::Head,ArtifactKind::FullHistory,ArtifactKind::Files];
   for offset in 0..lanes.len(){
    let pos=(cursor as usize+offset)%lanes.len(); let kind=lanes[pos];
    let running:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state='running' AND kind=?").bind(kind.as_str()).fetch_one(&mut *c).await?;
    if running as usize>=self.running_limit(kind){continue}
    let id:Option<i64>=if kind.expensive(){
     sqlx::query_scalar("SELECT q.id FROM artifact_jobs q WHERE q.state='queued' AND q.kind=? AND (SELECT count(*) FROM artifact_jobs wr WHERE wr.state='running' AND wr.workspace=q.workspace)<? AND NOT EXISTS(SELECT 1 FROM artifact_jobs r WHERE r.state='running' AND r.workspace=q.workspace AND r.repo=q.repo AND r.kind=q.kind) ORDER BY CASE WHEN q.workspace>? THEN 0 ELSE 1 END,q.workspace,q.created_at,q.id LIMIT 1").bind(kind.as_str()).bind(self.limits.workspace_running as i64).bind(&workspace_cursor).fetch_optional(&mut *c).await?
    }else{sqlx::query_scalar("SELECT q.id FROM artifact_jobs q WHERE q.state='queued' AND q.kind=? AND (SELECT count(*) FROM artifact_jobs wr WHERE wr.state='running' AND wr.workspace=q.workspace)<? ORDER BY CASE WHEN q.workspace>? THEN 0 ELSE 1 END,q.workspace,q.created_at,q.id LIMIT 1").bind(kind.as_str()).bind(self.limits.workspace_running as i64).bind(&workspace_cursor).fetch_optional(&mut *c).await?};
    let Some(id)=id else{continue}; let now=db_now(&mut c).await?;
    let won=sqlx::query("UPDATE artifact_jobs SET state='running',owner=?,heartbeat_at=?,lease_expires_at=?,lease_generation=lease_generation+1,claim_attempts=claim_attempts+1,updated_at=? WHERE id=? AND state='queued'")
     .bind(owner).bind(now).bind(now+lease_secs).bind(now).bind(id).execute(&mut *c).await?.rows_affected();
    if won==1{let record=get_conn(&mut c,id).await?.context("claimed artifact disappeared")?;sqlx::query("UPDATE scheduler_state SET fairness_cursor=?,workspace_cursor=? WHERE id=1").bind(((pos+1)%lanes.len()) as i64).bind(&record.key.workspace).execute(&mut *c).await?; return Ok(Some(ClaimedArtifact{record}))}
   }
   Ok(None)
  }.await;
        finish(c, result).await
    }

    pub async fn heartbeat(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        lease_secs: i64,
    ) -> Result<bool> {
        validate_lease(owner, lease_secs)?;
        let mut c = self.immediate().await?;
        let result:Result<bool>=async{let now=db_now(&mut c).await?; Ok(sqlx::query("UPDATE artifact_jobs SET heartbeat_at=?,lease_expires_at=?,updated_at=? WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?").bind(now).bind(now+lease_secs).bind(now).bind(claim.record.id).bind(owner).bind(claim.record.lease_generation as i64).bind(now).execute(&mut *c).await?.rows_affected()==1)}.await;
        finish(c, result).await
    }

    pub async fn owns(&self, claim: &ClaimedArtifact, owner: &str) -> Result<bool> {
        let mut c = self.pool.acquire().await?;
        let now = db_now(&mut c).await?;
        let n:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?").bind(claim.record.id).bind(owner).bind(claim.record.lease_generation as i64).bind(now).fetch_one(&mut *c).await?;
        Ok(n == 1)
    }

    #[cfg(test)]
    pub async fn complete(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        evidence: &CompletionEvidence,
    ) -> Result<bool> {
        validate_evidence(claim, evidence)?;
        self.verifier.verify(claim, evidence)?;
        let verified = self.completion_sealer.seal(claim, evidence.clone())?;
        self.complete_verified(claim, owner, &verified).await
    }

    pub(crate) async fn complete_verified(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        verified: &VerifiedCompletionEvidence,
    ) -> Result<bool> {
        let evidence = self.completion_sealer.verify(claim, verified)?;
        let mut c = self.immediate().await?;
        let result:Result<bool>=async{
   assert_gc_unfenced(&mut c).await?;
   let now=db_now(&mut c).await?; let won=sqlx::query("UPDATE artifact_jobs SET state='ready',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,manifest=?,error=NULL,failure_class=NULL,updated_at=? WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?")
    .bind(evidence.manifest()).bind(now).bind(claim.record.id).bind(owner).bind(claim.record.lease_generation as i64).bind(now).execute(&mut *c).await?.rows_affected()==1;
   if won{sqlx::query("UPDATE artifact_observations SET published_artifact_id=? WHERE desired_artifact_id=?").bind(claim.record.id).bind(claim.record.id).execute(&mut *c).await?;refresh_base_retention_conn(&mut c,&claim.record.key.workspace,&claim.record.key.repo,claim.record.key.format_version).await?;} Ok(won)
  }.await;
        finish(c, result).await
    }

    pub async fn fail(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        class: FailureClass,
        error: &str,
    ) -> Result<bool> {
        if error.trim().is_empty() {
            bail!("artifact failure reason is empty")
        };
        let mut c = self.immediate().await?;
        let result:Result<bool>=async{let now=db_now(&mut c).await?; Ok(sqlx::query("UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error=?,failure_class=?,updated_at=? WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?").bind(error).bind(class.as_str()).bind(now).bind(claim.record.id).bind(owner).bind(claim.record.lease_generation as i64).bind(now).execute(&mut *c).await?.rows_affected()==1)}.await;
        finish(c, result).await
    }

    /// Run cooperative work with internal heartbeats. On lost ownership or any
    /// failure, cancellation is signalled and every child is drained before return.
    /// Attempt-unique scratch prevents a stale child from colliding with a successor.
    #[cfg(test)]
    pub async fn run_owned(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        tasks: Vec<ArtifactTask>,
        evidence: CompletionEvidence,
        lease_secs: i64,
        scratch_root: &Path,
    ) -> Result<ExecutionOutcome> {
        crate::artifact_scheduler_backend::ArtifactSchedulerPersistence::run_owned(
            self,
            claim,
            owner,
            tasks,
            evidence,
            lease_secs,
            scratch_root,
        )
        .await
    }

    pub async fn reconcile_expired(&self) -> Result<(u64, u64)> {
        let mut c = self.immediate().await?;
        let result: Result<(u64, u64)> = async {
            let now = db_now(&mut c).await?;
            self.reconcile_at_conn(&mut c, now).await
        }
        .await;
        finish(c, result).await
    }
    async fn reconcile_at_conn(
        &self,
        c: &mut sqlx::SqliteConnection,
        now: i64,
    ) -> Result<(u64, u64)> {
        sqlx::query(
            "DELETE FROM ready_publication_fence_members WHERE token IN(
               SELECT token FROM ready_publication_fences WHERE state='held' AND expires_at<=?)",
        )
        .bind(now)
        .execute(&mut *c)
        .await?;
        sqlx::query("DELETE FROM ready_publication_fences WHERE state='held' AND expires_at<=?")
            .bind(now)
            .execute(&mut *c)
            .await?;
        sqlx::query("DELETE FROM artifact_consumers WHERE expires_at<=?")
            .bind(now)
            .execute(&mut *c)
            .await?;
        sqlx::query("DELETE FROM artifact_transport_leases WHERE rowid IN (SELECT rowid FROM artifact_transport_leases WHERE expires_at<=? ORDER BY expires_at,root_hash,session_id LIMIT 512)")
            .bind(now)
            .execute(&mut *c)
            .await?;
        sqlx::query("DELETE FROM artifact_jobs WHERE state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)").execute(&mut *c).await?;
        let failed=sqlx::query("UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error='lease expired after attempt limit',failure_class='dead_letter',updated_at=? WHERE state='running' AND lease_expires_at<=? AND claim_attempts>=?").bind(now).bind(now).bind(self.limits.max_claim_attempts as i64).execute(&mut *c).await?.rows_affected();
        let queued=sqlx::query("UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error='lease expired; reclaimed',updated_at=? WHERE state='running' AND lease_expires_at<=? AND claim_attempts<?").bind(now).bind(now).bind(self.limits.max_claim_attempts as i64).execute(&mut *c).await?.rows_affected();
        Ok((queued, failed))
    }

    pub async fn get(&self, id: i64) -> Result<Option<ArtifactRecord>> {
        let mut c = self.pool.acquire().await?;
        get_conn(&mut c, id).await
    }
    pub async fn get_by_key(&self, key: &ArtifactKey) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(SELECT)
            .bind(&key.workspace)
            .bind(&key.repo)
            .bind(&key.commit)
            .bind(key.kind.as_str())
            .bind(key.format_version as i64)
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_record).transpose()
    }
    pub async fn ready_page(&self, after_id: i64, limit: usize) -> Result<Vec<ArtifactRecord>> {
        if after_id < 0 || !(1..=1000).contains(&limit) {
            bail!("invalid ready scrub page");
        }
        sqlx::query("SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE state='ready' AND manifest IS NOT NULL AND id>? ORDER BY id LIMIT ?")
            .bind(after_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(row_record)
            .collect()
    }
    pub async fn published(
        &self,
        w: &str,
        r: &str,
        b: &str,
        k: ArtifactKind,
        format_version: u32,
    ) -> Result<Option<ArtifactRecord>> {
        let id:Option<i64>=sqlx::query_scalar("SELECT j.id FROM artifact_observations a JOIN artifact_jobs j ON j.id=a.published_artifact_id AND j.workspace=a.workspace AND j.repo=a.repo AND j.kind=a.kind AND j.format_version=a.format_version WHERE a.workspace=? AND a.repo=? AND a.branch=? AND a.kind=? AND a.format_version=? AND j.state='ready' AND j.manifest IS NOT NULL AND length(trim(j.manifest))>0").bind(w).bind(r).bind(b).bind(k.as_str()).bind(format_version as i64).fetch_optional(&self.pool).await?;
        match id {
            Some(id) => self.get(id).await,
            None => Ok(None),
        }
    }
    pub async fn ready_candidates(
        &self,
        workspace: &str,
        repo: &str,
        kind: ArtifactKind,
        format_version: u32,
        limit: u32,
    ) -> Result<Vec<ArtifactRecord>> {
        validate_format_version(format_version)?;
        if !(1..=32).contains(&limit) {
            bail!("ready candidate limit must be between 1 and 32")
        }
        let rows = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE workspace=? AND repo=? AND kind=? AND format_version=? AND state='ready' AND manifest IS NOT NULL AND length(trim(manifest))>0 ORDER BY updated_at DESC,id DESC LIMIT ?",
        )
        .bind(workspace)
        .bind(repo)
        .bind(kind.as_str())
        .bind(format_version as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_record).collect()
    }
    pub async fn complete_full_base_candidates(
        &self,
        workspace: &str,
        repo: &str,
        format_version: u32,
        limit: u32,
    ) -> Result<Vec<String>> {
        validate_format_version(format_version)?;
        if !(1..=32).contains(&limit) {
            bail!("full base candidate limit must be between 1 and 32")
        }
        let commits: Vec<String> = sqlx::query_scalar(
            "SELECT h.commit_oid FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' WHERE h.workspace=? AND h.repo=? AND h.format_version=? AND h.kind='head' AND h.state='ready' AND f.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0 AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 ORDER BY CASE WHEN h.updated_at>f.updated_at THEN h.updated_at ELSE f.updated_at END DESC, CASE WHEN h.id>f.id THEN h.id ELSE f.id END DESC LIMIT ?",
        )
        .bind(workspace)
        .bind(repo)
        .bind(format_version as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(commits)
    }
    pub async fn quarantine_publication(
        &self,
        key: &ArtifactKey,
        expected_manifest: &str,
        reason: &str,
    ) -> Result<bool> {
        validate_format_version(key.format_version)?;
        Cas::validate_artifact_id(expected_manifest)?;
        let mut c = self.immediate().await?;
        let result: Result<bool> = async {
            let now = db_now(&mut c).await?;
            let id: Option<i64> = sqlx::query_scalar(
                "UPDATE artifact_jobs SET state='failed',manifest=NULL,error=?,failure_class='retryable',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,updated_at=? WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=? AND state='ready' AND manifest=? RETURNING id",
            )
            .bind(reason)
            .bind(now)
            .bind(&key.workspace)
            .bind(&key.repo)
            .bind(&key.commit)
            .bind(key.kind.as_str())
            .bind(key.format_version as i64)
            .bind(expected_manifest)
            .fetch_optional(&mut *c)
            .await?;
            if let Some(id) = id {
                sqlx::query("UPDATE artifact_observations SET published_artifact_id=NULL WHERE published_artifact_id=?")
                    .bind(id)
                    .execute(&mut *c)
                    .await?;
                refresh_base_retention_conn(&mut c, &key.workspace, &key.repo, key.format_version)
                    .await?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        .await;
        finish(c, result).await
    }
    pub async fn counts(&self) -> Result<Vec<(ArtifactKind, ArtifactState, u64)>> {
        let rows=sqlx::query("SELECT kind,state,count(*) n FROM artifact_jobs GROUP BY kind,state ORDER BY kind,state").fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    ArtifactKind::parse(r.get("kind"))?,
                    ArtifactState::parse(r.get("state"))?,
                    r.get::<i64, _>("n") as u64,
                ))
            })
            .collect()
    }

    async fn immediate(&self) -> Result<ImmediateTransaction> {
        ImmediateTransaction::begin(&self.pool).await
    }
    async fn schedule_in(
        &self,
        c: &mut sqlx::SqliteConnection,
        key: &ArtifactKey,
    ) -> Result<ScheduleOutcome> {
        self.preflight_batch(
            c,
            &key.workspace,
            &key.repo,
            &key.commit,
            &[key.kind],
            key.format_version,
        )
        .await?;
        self.schedule_in_unchecked(c, key).await
    }
    async fn schedule_in_unchecked(
        &self,
        c: &mut sqlx::SqliteConnection,
        key: &ArtifactKey,
    ) -> Result<ScheduleOutcome> {
        if let Some(r) = get_key_conn(c, key).await? {
            return Ok(match r.state {
                ArtifactState::Ready => ScheduleOutcome::AlreadyReady(r.id),
                ArtifactState::Failed => ScheduleOutcome::Failed(
                    r.id,
                    r.failure_class.unwrap_or(FailureClass::Permanent),
                ),
                _ => ScheduleOutcome::Subscribed(r.id),
            });
        }
        let now = db_now(c).await?;
        let res=sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at)VALUES(?,?,?,?,?,'queued',?,?)").bind(&key.workspace).bind(&key.repo).bind(&key.commit).bind(key.kind.as_str()).bind(key.format_version as i64).bind(now).bind(now).execute(&mut *c).await?;
        Ok(ScheduleOutcome::Enqueued(res.last_insert_rowid()))
    }
    async fn preflight_batch(
        &self,
        c: &mut sqlx::SqliteConnection,
        w: &str,
        r: &str,
        commit: &str,
        kinds: &[ArtifactKind],
        v: u32,
    ) -> Result<()> {
        let mut additions = [0usize; 3];
        for &k in kinds {
            let key = ArtifactKey {
                workspace: w.into(),
                repo: r.into(),
                commit: commit.into(),
                kind: k,
                format_version: v,
            };
            if get_key_conn(c, &key).await?.is_none() {
                additions[kindex(k)] += 1
            }
        }
        let add_total: usize = additions.iter().sum();
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')",
        )
        .fetch_one(&mut *c)
        .await?;
        let workspace: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=?",
        )
        .bind(w)
        .fetch_one(&mut *c)
        .await?;
        let active_expensive: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind IN('full_history','files')",
        )
        .fetch_one(&mut *c)
        .await?;
        let expensive_add =
            additions[kindex(ArtifactKind::FullHistory)] + additions[kindex(ArtifactKind::Files)];
        if total as usize + add_total > self.limits.total_backlog
            || workspace as usize + add_total > self.limits.workspace_backlog
            || active_expensive as usize + expensive_add
                > self
                    .limits
                    .total_backlog
                    .saturating_sub(self.limits.head_reserved)
        {
            bail!("artifact queue capacity exhausted for atomic observation batch")
        }
        for k in [
            ArtifactKind::Head,
            ArtifactKind::FullHistory,
            ArtifactKind::Files,
        ] {
            if additions[kindex(k)] > 0 {
                self.preflight_capacity(c, k, w, additions[kindex(k)])
                    .await?
            }
        }
        Ok(())
    }
    async fn preflight_capacity(
        &self,
        c: &mut sqlx::SqliteConnection,
        kind: ArtifactKind,
        w: &str,
        add: usize,
    ) -> Result<()> {
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')",
        )
        .fetch_one(&mut *c)
        .await?;
        let workspace: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=?",
        )
        .bind(w)
        .fetch_one(&mut *c)
        .await?;
        let per: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind=?",
        )
        .bind(kind.as_str())
        .fetch_one(&mut *c)
        .await?;
        let active_expensive: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind IN('full_history','files')",
        )
        .fetch_one(&mut *c)
        .await?;
        let reserve_exhausted = kind.expensive()
            && active_expensive as usize + add
                > self
                    .limits
                    .total_backlog
                    .saturating_sub(self.limits.head_reserved);
        if total as usize + add > self.limits.total_backlog
            || workspace as usize + add > self.limits.workspace_backlog
            || per as usize + add > self.backlog_limit(kind)
            || reserve_exhausted
        {
            bail!("artifact queue capacity exhausted for {}", kind.as_str())
        }
        Ok(())
    }
    fn backlog_limit(&self, k: ArtifactKind) -> usize {
        match k {
            ArtifactKind::Head => self.limits.head_backlog,
            ArtifactKind::FullHistory => self.limits.full_history_backlog,
            ArtifactKind::Files => self.limits.files_backlog,
        }
    }
    fn running_limit(&self, k: ArtifactKind) -> usize {
        match k {
            ArtifactKind::Head => self.limits.head_running,
            ArtifactKind::FullHistory => self.limits.full_history_running,
            ArtifactKind::Files => self.limits.files_running,
        }
    }
}

const SELECT: &str = "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?";
async fn get_conn(c: &mut sqlx::SqliteConnection, id: i64) -> Result<Option<ArtifactRecord>> {
    let row=sqlx::query("SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE id=?").bind(id).fetch_optional(&mut *c).await?;
    row.map(row_record).transpose()
}
async fn get_key_conn(
    c: &mut sqlx::SqliteConnection,
    k: &ArtifactKey,
) -> Result<Option<ArtifactRecord>> {
    let row = sqlx::query(SELECT)
        .bind(&k.workspace)
        .bind(&k.repo)
        .bind(&k.commit)
        .bind(k.kind.as_str())
        .bind(k.format_version as i64)
        .fetch_optional(&mut *c)
        .await?;
    row.map(row_record).transpose()
}
fn row_record(r: SqliteRow) -> Result<ArtifactRecord> {
    Ok(ArtifactRecord {
        id: r.get("id"),
        key: ArtifactKey {
            workspace: r.get("workspace"),
            repo: r.get("repo"),
            commit: r.get("commit_oid"),
            kind: ArtifactKind::parse(r.get("kind"))?,
            format_version: r.get::<i64, _>("format_version") as u32,
        },
        state: ArtifactState::parse(r.get("state"))?,
        owner: r.get("owner"),
        lease_expires_at: r.get("lease_expires_at"),
        lease_generation: r.get::<i64, _>("lease_generation") as u64,
        claim_attempts: r.get::<i64, _>("claim_attempts") as u32,
        retry_count: r.get::<i64, _>("retry_count") as u32,
        manifest: r.get("manifest"),
        error: r.get("error"),
        failure_class: r
            .get::<Option<String>, _>("failure_class")
            .map(|s| FailureClass::parse(&s))
            .transpose()?,
    })
}
async fn db_now(c: &mut sqlx::SqliteConnection) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT unixepoch()")
        .fetch_one(&mut *c)
        .await?)
}

fn canonical_sqlite_ddl(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_whitespace() && *character != '`' && *character != '"')
        .flat_map(char::to_lowercase)
        .collect()
}

async fn preflight_sqlite_schema(
    connection: &mut sqlx::SqliteConnection,
    version: i64,
) -> Result<()> {
    if version == 0 || version == 1 {
        return Ok(());
    }
    let tables: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN('artifact_jobs','artifact_base_retention','branch_observations','artifact_observations','artifact_consumers','artifact_transport_leases','artifact_gc_sweep','scheduler_state')").fetch_one(&mut *connection).await?;
    let indexes: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='index' AND name IN('artifact_jobs_claim','artifact_jobs_lease','artifact_base_retention_repo','artifact_observations_published','artifact_transport_leases_expiry')").fetch_one(&mut *connection).await?;
    let fence_tables: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')").fetch_one(&mut *connection).await?;
    let fence_indexes: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='index' AND name='ready_publication_fences_recovery'").fetch_one(&mut *connection).await?;

    // v2 existed in two exact released lineages: the five-table admission
    // scheduler and the six-table transport scheduler. v3 diverged into an
    // admission protocol or the transport retention/GC protocol. v4 is the
    // approved transport lineage (and, briefly, a combined integration build).
    // v5/v6 are exclusively the exact union; v6 records fleet admission parity.
    let inventory_ok = match version {
        2 => {
            (tables == 5 && indexes == 3 && fence_tables == 3 && fence_indexes == 0)
                || (tables == 6 && indexes == 4 && fence_tables == 0 && fence_indexes == 0)
        }
        3 => {
            (tables == 5 && indexes == 3 && fence_tables == 3 && fence_indexes == 1)
                || (tables == 8 && indexes == 5 && fence_tables == 0 && fence_indexes == 0)
        }
        4 => {
            tables == 8
                && indexes == 5
                && ((fence_tables == 0 && fence_indexes == 0)
                    || (fence_tables == 3 && fence_indexes == 1))
        }
        5..=7 => tables == 8 && indexes == 5 && fence_tables == 3 && fence_indexes == 1,
        _ => false,
    };
    if !inventory_ok {
        bail!("sqlite artifact scheduler schema marker does not match an approved lineage")
    }
    if version == 7 {
        crate::git_source_registry::validate_sqlite_v7_in(connection).await?;
    }
    if version == 2 && fence_tables == 3 {
        let fence_ddl: Vec<(String, String)> = sqlx::query_as(
            "SELECT name,sql FROM sqlite_master WHERE type='table' AND name IN(
               'ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members') ORDER BY name",
        )
        .fetch_all(&mut *connection)
        .await?;
        let actual = fence_ddl
            .into_iter()
            .map(|(name, sql)| (name, canonical_sqlite_ddl(&sql)))
            .collect::<std::collections::BTreeMap<_, _>>();
        let expected = [
            ("ready_publication_fence_members", "createtableready_publication_fence_members(tokentextnotnull,generationintegernotnullcheck(generation>0),artifact_idintegernotnull,manifesttext,primarykey(token,artifact_id),foreignkey(token,generation)referencesready_publication_fences(token,generation)ondeletecascade,foreignkey(artifact_id)referencesartifact_jobs(id)ondeletecascade)"),
            ("ready_publication_fence_sequence", "createtableready_publication_fence_sequence(idintegerprimarykeycheck(id=1),generationintegernotnullcheck(generation>=0))"),
            ("ready_publication_fences", "createtableready_publication_fences(tokentextprimarykey,generationintegernotnulluniquecheck(generation>0),operation_idtextnotnullunique,expires_atintegernotnull,statetextnotnullcheck(statein('held','activation_unknown')),unique(token,generation))"),
        ]
        .into_iter()
        .map(|(name, sql)| (name.to_owned(), sql.to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
        if actual != expected {
            bail!("sqlite admission-v2 fence DDL differs from its released provenance")
        }
    }
    if version >= 3 && fence_tables == 3 {
        let fence_ddl: Vec<(String, String)> = sqlx::query_as(
            "SELECT name,sql FROM sqlite_master WHERE (type='table' AND name IN(
               'ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members'))
               OR (type='index' AND name='ready_publication_fences_recovery') ORDER BY name",
        )
        .fetch_all(&mut *connection)
        .await?;
        let actual = fence_ddl
            .into_iter()
            .map(|(name, sql)| (name, canonical_sqlite_ddl(&sql)))
            .collect::<std::collections::BTreeMap<_, _>>();
        let expected = [
            ("ready_publication_fence_members", "createtableready_publication_fence_members(tokentextnotnull,generationintegernotnullcheck(generation>0),artifact_idintegernotnull,manifesttextnotnullcheck(length(trim(manifest))>0),primarykey(token,artifact_id),foreignkey(token,generation)referencesready_publication_fences(token,generation)ondeletecascade,foreignkey(artifact_id)referencesartifact_jobs(id)ondeletecascade)"),
            ("ready_publication_fence_sequence", "createtableready_publication_fence_sequence(idintegerprimarykeycheck(id=1),generationintegernotnullcheck(generation>=0))"),
            ("ready_publication_fences", "createtableready_publication_fences(tokentextprimarykey,generationintegernotnulluniquecheck(generation>0),operation_idtextnotnullunique,workspacetextnotnull,repotextnotnull,branchtextnotnull,targettextnotnull,attempt_idtextnotnull,expires_atintegernotnull,statetextnotnullcheck(statein('held','activation_unknown')),unique(token,generation))"),
            ("ready_publication_fences_recovery", "createindexready_publication_fences_recoveryonready_publication_fences(state,generation,token)"),
        ]
        .into_iter()
        .map(|(name, sql)| (name.to_owned(), sql.to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
        if actual != expected {
            bail!("sqlite admission fence DDL differs from its schema marker")
        }
    }
    let additions: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN('artifact_base_retention','artifact_gc_sweep')")
        .fetch_one(&mut *connection).await?;
    if version == 2 {
        if additions != 0 {
            bail!("sqlite v2 scheduler contains unversioned v3 additions")
        }
        return Ok(());
    }
    // Admission-v3 deliberately has neither transport addition. All other
    // post-v2 lineages must have both with the exact approved DDL.
    if version == 3 && tables == 5 {
        return Ok(());
    }
    if additions != 2 {
        bail!("sqlite transport lineage is missing retention or GC state")
    }
    let base_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='artifact_base_retention'",
    )
    .fetch_one(&mut *connection)
    .await?;
    let base_index_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name='artifact_base_retention_repo'",
    )
    .fetch_one(&mut *connection)
    .await?;
    let gc_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='artifact_gc_sweep'",
    )
    .fetch_one(&mut *connection)
    .await?;
    if canonical_sqlite_ddl(&base_sql)
        != "createtableartifact_base_retention(artifact_idintegerprimarykey,workspacetextnotnull,repotextnotnull,format_versionintegernotnull,head_rankintegercheck(head_rankbetween1and8),pair_rankintegercheck(pair_rankbetween1and8),check(head_rankisnotnullorpair_rankisnotnull),foreignkey(artifact_id)referencesartifact_jobs(id)ondeletecascade)"
        || canonical_sqlite_ddl(&base_index_sql)
            != "createindexartifact_base_retention_repoonartifact_base_retention(workspace,repo,format_version,artifact_id)"
        || canonical_sqlite_ddl(&gc_sql)
            != "createtableartifact_gc_sweep(idintegerprimarykeycheck(id=1),ownertextnotnull,expires_atintegernotnull)"
    {
        bail!("sqlite scheduler retention/GC DDL differs from its schema marker")
    }
    Ok(())
}

fn validate_gc_sweep(owner: &str, ttl_secs: i64) -> Result<()> {
    if owner.trim().is_empty() || owner.len() > 200 || !(1..=600).contains(&ttl_secs) {
        bail!("GC sweep owner or TTL is invalid")
    }
    Ok(())
}

async fn assert_gc_unfenced(c: &mut sqlx::SqliteConnection) -> Result<()> {
    let now = db_now(c).await?;
    let fenced: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact_gc_sweep WHERE id=1 AND expires_at>?")
            .bind(now)
            .fetch_one(&mut *c)
            .await?;
    if fenced != 0 {
        bail!("artifact publication is temporarily fenced by remote GC")
    }
    Ok(())
}

async fn refresh_base_retention_conn(
    c: &mut sqlx::SqliteConnection,
    workspace: &str,
    repo: &str,
    format_version: u32,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM artifact_base_retention WHERE workspace=? AND repo=? AND format_version=?",
    )
    .bind(workspace)
    .bind(repo)
    .bind(format_version as i64)
    .execute(&mut *c)
    .await?;
    sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank) SELECT id,workspace,repo,format_version,row_number() OVER(ORDER BY updated_at DESC,id DESC),NULL FROM artifact_jobs WHERE workspace=? AND repo=? AND format_version=? AND kind='head' AND state='ready' AND manifest IS NOT NULL AND length(trim(manifest))>0 ORDER BY updated_at DESC,id DESC LIMIT 8")
        .bind(workspace).bind(repo).bind(format_version as i64).execute(&mut *c).await?;
    sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank) SELECT head_id,?,?,?,NULL,pair_rank FROM (SELECT h.id head_id,f.id history_id,row_number() OVER(ORDER BY CASE WHEN h.updated_at>f.updated_at THEN h.updated_at ELSE f.updated_at END DESC,CASE WHEN h.id>f.id THEN h.id ELSE f.id END DESC) pair_rank FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' AND f.state='ready' AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 WHERE h.workspace=? AND h.repo=? AND h.format_version=? AND h.kind='head' AND h.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0) WHERE pair_rank<=8 ON CONFLICT(artifact_id) DO UPDATE SET pair_rank=excluded.pair_rank")
        .bind(workspace).bind(repo).bind(format_version as i64).bind(workspace).bind(repo).bind(format_version as i64).execute(&mut *c).await?;
    sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank) SELECT history_id,?,?,?,NULL,pair_rank FROM (SELECT h.id head_id,f.id history_id,row_number() OVER(ORDER BY CASE WHEN h.updated_at>f.updated_at THEN h.updated_at ELSE f.updated_at END DESC,CASE WHEN h.id>f.id THEN h.id ELSE f.id END DESC) pair_rank FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' AND f.state='ready' AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 WHERE h.workspace=? AND h.repo=? AND h.format_version=? AND h.kind='head' AND h.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0) WHERE pair_rank<=8 ON CONFLICT(artifact_id) DO UPDATE SET pair_rank=excluded.pair_rank")
        .bind(workspace).bind(repo).bind(format_version as i64).bind(workspace).bind(repo).bind(format_version as i64).execute(&mut *c).await?;
    Ok(())
}
async fn finish<T>(mut c: ImmediateTransaction, r: Result<T>) -> Result<T> {
    match r {
        Ok(v) => match sqlx::query("COMMIT").execute(&mut *c).await {
            Ok(_) => {
                c.release();
                Ok(v)
            }
            Err(error) => {
                Err(error).context("commit artifact scheduler transaction; connection retired")
            }
        },
        Err(error) => match sqlx::query("ROLLBACK").execute(&mut *c).await {
            Ok(_) => {
                c.release();
                Err(error)
            }
            Err(rollback) => Err(error).context(format!(
                "rollback artifact scheduler transaction failed; connection retired: {rollback}"
            )),
        },
    }
}
fn outcome_id(o: &ScheduleOutcome) -> i64 {
    match o {
        ScheduleOutcome::Enqueued(i)
        | ScheduleOutcome::Subscribed(i)
        | ScheduleOutcome::AlreadyReady(i)
        | ScheduleOutcome::Failed(i, _) => *i,
    }
}
fn kindex(k: ArtifactKind) -> usize {
    match k {
        ArtifactKind::Head => 0,
        ArtifactKind::FullHistory => 1,
        ArtifactKind::Files => 2,
    }
}
pub(crate) fn validate_lease(owner: &str, secs: i64) -> Result<()> {
    if owner.trim().is_empty() {
        bail!("lease owner is empty")
    };
    if !(2..=86400).contains(&secs) {
        bail!("lease duration must be between 2 and 86400 seconds")
    };
    Ok(())
}
pub(crate) fn validate_format_version(version: u32) -> Result<()> {
    if version == 0 {
        bail!("artifact format version must be positive")
    }
    Ok(())
}
pub(crate) fn validate_observation_identity(
    workspace: &str,
    repo: &str,
    branch: &str,
    operation: &str,
) -> Result<()> {
    if workspace.trim().is_empty() || repo.trim().is_empty() || branch.trim().is_empty() {
        bail!("artifact observation {operation} has an empty workspace, repo, or branch")
    }
    Ok(())
}
pub(crate) fn validate_resolved_commit(commit: &str) -> Result<()> {
    if commit.trim().is_empty() {
        bail!("resolved artifact commit is empty")
    }
    Ok(())
}
pub(crate) fn validate_canonical_commit_oid(commit: &str) -> Result<()> {
    validate_resolved_commit(commit)?;
    crate::validation::validate_object_id(commit)
        .context("resolved artifact commit is not a canonical Git object id")
}
pub(crate) fn validate_limits(l: &SchedulerLimits) -> Result<()> {
    if l.total_backlog == 0
        || l.workspace_backlog == 0
        || l.total_running == 0
        || l.head_running == 0
        || l.full_history_running == 0
        || l.files_running == 0
        || l.workspace_running == 0
        || l.max_claim_attempts == 0
    {
        bail!("scheduler limits must be positive")
    };
    if l.head_reserved > l.total_backlog {
        bail!("HEAD reserve exceeds total backlog")
    };
    if l.head_running > l.total_running
        || l.full_history_running.saturating_add(l.files_running)
            > l.total_running.saturating_sub(l.head_running)
    {
        bail!("non-HEAD running caps consume reserved HEAD capacity")
    }
    Ok(())
}
pub(crate) fn scheduler_fingerprint(limits: &SchedulerLimits, verifier_id: &str) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        limits.total_backlog,
        limits.workspace_backlog,
        limits.head_reserved,
        limits.head_backlog,
        limits.full_history_backlog,
        limits.files_backlog,
        limits.total_running,
        limits.head_running,
        limits.full_history_running,
        limits.files_running,
        limits.workspace_running,
        limits.max_claim_attempts,
        limits.max_manual_retries,
        verifier_id
    )
}
pub(crate) fn scheduler_limits_fingerprint(limits: &SchedulerLimits) -> String {
    let mut digest = Sha256::new();
    for value in [
        limits.total_backlog,
        limits.workspace_backlog,
        limits.head_reserved,
        limits.head_backlog,
        limits.full_history_backlog,
        limits.files_backlog,
        limits.total_running,
        limits.head_running,
        limits.full_history_running,
        limits.files_running,
        limits.workspace_running,
    ] {
        digest.update((value as u64).to_be_bytes());
    }
    digest.update(limits.max_claim_attempts.to_be_bytes());
    digest.update(limits.max_manual_retries.to_be_bytes());
    hex::encode(digest.finalize())
}
pub(crate) fn validate_evidence(c: &ClaimedArtifact, e: &CompletionEvidence) -> Result<()> {
    if e.manifest().trim().is_empty() {
        bail!("artifact completion manifest is empty")
    };
    if e.key() != &c.record.key {
        bail!("completion evidence does not match claimed artifact key")
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_manifest::CasCompletionVerifier;
    use crate::cas::Cas;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    async fn scheduler(l: SchedulerLimits) -> (ArtifactScheduler, tempfile::TempDir, String) {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("a.db").to_string_lossy().to_string();
        (ArtifactScheduler::open(&p, l).await.unwrap(), d, p)
    }
    fn key(w: &str, c: &str, k: ArtifactKind) -> ArtifactKey {
        ArtifactKey {
            workspace: w.into(),
            repo: "o/r".into(),
            commit: c.into(),
            kind: k,
            format_version: 1,
        }
    }
    fn evidence(c: &ClaimedArtifact) -> CompletionEvidence {
        CompletionEvidence::new(c.record.key.clone(), "manifest-hash").unwrap()
    }
    async fn expire(s: &ArtifactScheduler, id: i64) {
        sqlx::query("UPDATE artifact_jobs SET lease_expires_at=unixepoch()-1 WHERE id=?")
            .bind(id)
            .execute(&s.pool)
            .await
            .unwrap();
    }
    async fn legacy_pool(path: &str) -> SqlitePool {
        let opts = SqliteConnectOptions::from_str(path)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE artifact_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,kind TEXT NOT NULL,format_version INTEGER NOT NULL,state TEXT NOT NULL,owner TEXT,heartbeat_at INTEGER,lease_expires_at INTEGER,attempts INTEGER NOT NULL DEFAULT 0,manifest TEXT,error TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,UNIQUE(workspace,repo,commit_oid,kind,format_version));
             CREATE TABLE artifact_observations(workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,kind TEXT NOT NULL,desired_commit TEXT NOT NULL,desired_artifact_id INTEGER NOT NULL,published_artifact_id INTEGER,observed_at INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch,kind));",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }
    async fn drop_source_v7(pool: &SqlitePool) {
        sqlx::raw_sql("DROP TABLE artifact_intents; DROP TABLE branch_source_current; DROP TABLE branch_source_generations; DROP TABLE git_source_consumers; DROP TABLE git_source_desires; DROP TABLE git_source_acquisition_members; DROP TABLE git_source_acquisitions; DROP TABLE git_source_acquisition_sequence; DROP TABLE git_source_maintenance; DROP TABLE git_source_members; DROP TABLE git_source_roots;").execute(pool).await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_processes_branches_and_head_alias_share_one_job() {
        let (a, _d, p) = scheduler(Default::default()).await;
        let b = ArtifactScheduler::open(&p, Default::default())
            .await
            .unwrap();
        let (x, y) = tokio::join!(
            a.observe("ws", "o/r", "main", "a", &[ArtifactKind::Head], 1, None),
            b.observe("ws", "o/r", "HEAD", "a", &[ArtifactKind::Head], 1, None)
        );
        let accepted = [x.unwrap(), y.unwrap()]
            .into_iter()
            .filter(|o| matches!(o, ObservationOutcome::Accepted { .. }))
            .count();
        assert_eq!(accepted, 2);
        assert_eq!(
            a.counts().await.unwrap(),
            vec![(ArtifactKind::Head, ArtifactState::Queued, 1)]
        );
        let (c1, c2) = tokio::join!(a.claim("a", 5), b.claim("b", 5));
        assert_eq!(
            [c1.unwrap().is_some(), c2.unwrap().is_some()]
                .into_iter()
                .filter(|x| *x)
                .count(),
            1
        )
    }

    #[tokio::test]
    async fn public_consumer_api_rejects_reserved_source_intent_namespace() {
        let (scheduler, _temp, _path) = scheduler(Default::default()).await;
        let reserved = format!("intent:{}", "a".repeat(48));
        assert!(
            scheduler
                .subscribe_consumer(&key("ws", "a", ArtifactKind::Head), &reserved, 60)
                .await
                .is_err()
        );
        assert!(scheduler.release_consumer(1, &reserved).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact_jobs")
                .fetch_one(&scheduler.pool)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn concurrent_same_branch_same_tip_is_one_accept_and_one_atomic_noop() {
        let (a, _d, p) = scheduler(Default::default()).await;
        let b = ArtifactScheduler::open(&p, Default::default())
            .await
            .unwrap();
        let (left, right) = tokio::join!(
            a.observe(
                "ws",
                "o/r",
                "main",
                "same-tip",
                &[ArtifactKind::Head, ArtifactKind::FullHistory],
                1,
                None,
            ),
            b.observe(
                "ws",
                "o/r",
                "main",
                "same-tip",
                &[ArtifactKind::Head, ArtifactKind::FullHistory],
                1,
                None,
            )
        );
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    ObservationOutcome::Accepted { generation: 1, .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    ObservationOutcome::Unchanged { generation: 1 }
                ))
                .count(),
            1
        );
        let snapshot = a.observation_snapshot("ws", "o/r", "main").await.unwrap();
        assert_eq!(snapshot.workspace(), "ws");
        assert_eq!(snapshot.repo(), "o/r");
        assert_eq!(snapshot.branch(), "main");
        assert_eq!(snapshot.generation(), Some(1));
        assert_eq!(snapshot.commit(), Some("same-tip"));
        assert_eq!(
            a.counts()
                .await
                .unwrap()
                .iter()
                .map(|(_, _, n)| n)
                .sum::<u64>(),
            2
        );
    }

    #[tokio::test]
    async fn stale_force_push_resolution_cannot_overwrite_newer_observation() {
        let (scheduler, _d, _p) = scheduler(Default::default()).await;
        scheduler
            .observe("ws", "o/r", "main", "t1", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        let snapshot = scheduler
            .observation_snapshot("ws", "o/r", "main")
            .await
            .unwrap();
        assert_eq!(snapshot.generation(), Some(1));
        assert!(matches!(
            crate::artifact_scheduler_backend::ArtifactSchedulerPersistence::observe_if_changed(
                &scheduler,
                &snapshot,
                "2222222222222222222222222222222222222222",
                &[ArtifactKind::Head],
                1,
            )
            .await
            .unwrap(),
            ObservationOutcome::Accepted { generation: 2, .. }
        ));
        assert_eq!(
            crate::artifact_scheduler_backend::ArtifactSchedulerPersistence::observe_if_changed(
                &scheduler,
                &snapshot,
                "3333333333333333333333333333333333333333",
                &[ArtifactKind::Head],
                1,
            )
            .await
            .unwrap(),
            ObservationOutcome::Stale {
                current_generation: 2,
            }
        );
        assert_eq!(
            scheduler
                .observation_snapshot("ws", "o/r", "main")
                .await
                .unwrap()
                .commit(),
            Some("2222222222222222222222222222222222222222")
        );
        for invalid in [
            "",
            "short",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "gggggggggggggggggggggggggggggggggggggggg",
        ] {
            assert!(
                crate::artifact_scheduler_backend::ArtifactSchedulerPersistence::observe_if_changed(
                    &scheduler,
                    &snapshot,
                    invalid,
                    &[ArtifactKind::Head],
                    1,
                )
                .await
                .is_err()
            );
        }
        assert!(validate_canonical_commit_oid(&"1".repeat(64)).is_ok());
    }

    #[tokio::test]
    async fn same_tip_only_noops_when_every_requested_kind_and_format_is_observed() {
        let (scheduler, _d, _p) = scheduler(Default::default()).await;
        scheduler
            .observe("ws", "o/r", "main", "tip", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        assert!(matches!(
            scheduler
                .observe(
                    "ws",
                    "o/r",
                    "main",
                    "tip",
                    &[ArtifactKind::Head, ArtifactKind::Files],
                    1,
                    Some(1),
                )
                .await
                .unwrap(),
            ObservationOutcome::Accepted { generation: 2, .. }
        ));
        assert!(matches!(
            scheduler
                .observe(
                    "ws",
                    "o/r",
                    "main",
                    "tip",
                    &[ArtifactKind::Head, ArtifactKind::Files],
                    2,
                    Some(2),
                )
                .await
                .unwrap(),
            ObservationOutcome::Accepted { generation: 3, .. }
        ));
        assert_eq!(
            scheduler
                .observe(
                    "ws",
                    "o/r",
                    "main",
                    "tip",
                    &[ArtifactKind::Files, ArtifactKind::Head],
                    2,
                    Some(0),
                )
                .await
                .unwrap(),
            ObservationOutcome::Unchanged { generation: 3 }
        );
    }

    #[tokio::test]
    async fn observation_snapshot_and_write_reject_ambiguous_empty_identity() {
        let (scheduler, _d, _p) = scheduler(Default::default()).await;
        for (workspace, repo, branch) in [
            ("", "o/r", "main"),
            ("ws", "\t", "main"),
            ("ws", "o/r", "\n"),
        ] {
            assert!(
                scheduler
                    .observation_snapshot(workspace, repo, branch)
                    .await
                    .is_err()
            );
            assert!(
                scheduler
                    .observe(
                        workspace,
                        repo,
                        branch,
                        "tip",
                        &[ArtifactKind::Head],
                        1,
                        None,
                    )
                    .await
                    .is_err()
            );
        }
        assert!(scheduler.counts().await.unwrap().is_empty());
        for commit in ["", " ", "\t\n"] {
            assert!(
                scheduler
                    .observe("ws", "o/r", "main", commit, &[ArtifactKind::Head], 1, None,)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn stale_observation_rejected_before_any_job_and_batch_is_atomic() {
        let (s, _d, _) = scheduler(Default::default()).await;
        let first = s
            .observe("ws", "o/r", "main", "t1", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        assert!(matches!(
            first,
            ObservationOutcome::Accepted { generation: 1, .. }
        ));
        let stale = s
            .observe(
                "ws",
                "o/r",
                "main",
                "stale",
                &[ArtifactKind::Files],
                1,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            stale,
            ObservationOutcome::Stale {
                current_generation: 1
            }
        );
        assert!(
            s.get_by_key(&key("ws", "stale", ArtifactKind::Files))
                .await
                .unwrap()
                .is_none()
        );
        let bad = SchedulerLimits {
            files_backlog: 0,
            ..Default::default()
        };
        let (s2, _d, _) = scheduler(bad).await;
        assert!(
            s2.observe(
                "ws",
                "o/r",
                "main",
                "t",
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                None
            )
            .await
            .is_err()
        );
        assert!(s2.counts().await.unwrap().is_empty())
    }

    #[tokio::test]
    async fn multi_kind_batch_cannot_consume_reserved_head_backlog() {
        let (s, _d, _) = scheduler(SchedulerLimits {
            total_backlog: 3,
            workspace_backlog: 3,
            head_reserved: 1,
            head_backlog: 3,
            full_history_backlog: 3,
            files_backlog: 3,
            ..Default::default()
        })
        .await;
        s.schedule(&key("ws", "existing", ArtifactKind::FullHistory))
            .await
            .unwrap();
        assert!(
            s.observe(
                "ws",
                "o/r",
                "main",
                "batch",
                &[ArtifactKind::FullHistory, ArtifactKind::Files],
                1,
                None
            )
            .await
            .is_err()
        );
        assert_eq!(
            s.counts().await.unwrap(),
            vec![(ArtifactKind::FullHistory, ArtifactState::Queued, 1)]
        );
    }

    #[tokio::test]
    async fn generation_cas_handles_force_push_and_same_time_without_clocks() {
        let (s, _d, _) = scheduler(Default::default()).await;
        s.observe("ws", "o/r", "main", "t1", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        let a = s
            .observe(
                "ws",
                "o/r",
                "main",
                "force",
                &[ArtifactKind::Head],
                1,
                Some(1),
            )
            .await
            .unwrap();
        assert!(matches!(
            a,
            ObservationOutcome::Accepted { generation: 2, .. }
        ));
        assert_eq!(
            s.observe(
                "ws",
                "o/r",
                "main",
                "late",
                &[ArtifactKind::Files],
                1,
                Some(1)
            )
            .await
            .unwrap(),
            ObservationOutcome::Stale {
                current_generation: 2
            }
        );
        assert!(
            s.get_by_key(&key("ws", "late", ArtifactKind::Files))
                .await
                .unwrap()
                .is_none()
        )
    }

    #[tokio::test]
    async fn superseded_exact_work_finishes_without_repointing_newer_alias() {
        let (s, _d, _) = scheduler(Default::default()).await;
        s.observe("ws", "o/r", "main", "t1", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        s.subscribe_consumer(&key("ws", "t1", ArtifactKind::Head), "clone-1", 60)
            .await
            .unwrap();
        s.observe("ws", "o/r", "main", "t2", &[ArtifactKind::Head], 1, Some(1))
            .await
            .unwrap();
        let t1 = s.claim("w", 5).await.unwrap().unwrap();
        assert_eq!(t1.record.key.commit, "t1");
        assert!(s.complete(&t1, "w", &evidence(&t1)).await.unwrap());
        assert!(
            s.published("ws", "o/r", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .is_none(),
            "superseded T1 must not repoint a T2 observation"
        );
        let t2 = s.claim("w", 5).await.unwrap().unwrap();
        assert_eq!(t2.record.key.commit, "t2");
        assert!(s.complete(&t2, "w", &evidence(&t2)).await.unwrap());
        assert_eq!(
            s.published("ws", "o/r", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .unwrap()
                .key
                .commit,
            "t2"
        );
        assert_eq!(
            s.get_by_key(&key("ws", "t1", ArtifactKind::Head))
                .await
                .unwrap()
                .unwrap()
                .state,
            ArtifactState::Ready
        );
    }

    #[tokio::test]
    async fn complete_full_base_survives_history_first_alias_advance() {
        let (s, _d, _) = scheduler(Default::default()).await;
        s.observe(
            "ws",
            "o/r",
            "main",
            "t1",
            &[ArtifactKind::Head, ArtifactKind::FullHistory],
            1,
            None,
        )
        .await
        .unwrap();
        for _ in 0..2 {
            let claim = s.claim("old", 60).await.unwrap().unwrap();
            assert!(s.complete(&claim, "old", &evidence(&claim)).await.unwrap());
        }
        assert_eq!(
            s.complete_full_base_candidates("ws", "o/r", 1, 8)
                .await
                .unwrap()
                .first()
                .map(String::as_str),
            Some("t1")
        );

        s.observe(
            "ws",
            "o/r",
            "main",
            "t2",
            &[ArtifactKind::Head, ArtifactKind::FullHistory],
            1,
            Some(1),
        )
        .await
        .unwrap();
        let head = s.claim("new-head", 60).await.unwrap().unwrap();
        assert_eq!(head.record.key.kind, ArtifactKind::Head);
        let history = s.claim("new-history", 60).await.unwrap().unwrap();
        assert_eq!(history.record.key.kind, ArtifactKind::FullHistory);
        assert!(
            s.complete(&history, "new-history", &evidence(&history))
                .await
                .unwrap()
        );
        assert_eq!(
            s.published("ws", "o/r", "main", ArtifactKind::FullHistory, 1)
                .await
                .unwrap()
                .unwrap()
                .key
                .commit,
            "t2"
        );
        assert_eq!(
            s.complete_full_base_candidates("ws", "o/r", 1, 8)
                .await
                .unwrap()
                .first()
                .map(String::as_str),
            Some("t1"),
            "the independently advanced History alias must not hide T1"
        );
        assert!(
            s.complete(&head, "new-head", &evidence(&head))
                .await
                .unwrap()
        );
        assert_eq!(
            s.complete_full_base_candidates("ws", "o/r", 1, 8)
                .await
                .unwrap()
                .first()
                .map(String::as_str),
            Some("t2")
        );
    }

    #[tokio::test]
    async fn candidate_catalog_is_bounded_and_quarantine_is_manifest_fenced() {
        let (s, _d, _) = scheduler(Default::default()).await;
        for (generation, commit) in ["old", "new"].into_iter().enumerate() {
            s.observe(
                "ws",
                "o/r",
                "main",
                commit,
                &[ArtifactKind::Head],
                1,
                (generation > 0).then_some(generation as u64),
            )
            .await
            .unwrap();
            let claim = s.claim("worker", 60).await.unwrap().unwrap();
            let manifest = if commit == "old" {
                "a".repeat(64)
            } else {
                "b".repeat(64)
            };
            let evidence = CompletionEvidence::new(claim.record.key.clone(), manifest).unwrap();
            assert!(s.complete(&claim, "worker", &evidence).await.unwrap());
        }
        let candidates = s
            .ready_candidates("ws", "o/r", ArtifactKind::Head, 1, 1)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].key.commit, "new");
        assert!(
            s.ready_candidates("ws", "o/r", ArtifactKind::Head, 1, 0)
                .await
                .is_err()
        );
        assert!(
            s.ready_candidates("ws", "o/r", ArtifactKind::Head, 1, 33)
                .await
                .is_err()
        );

        let newest = &candidates[0];
        assert!(
            !s.quarantine_publication(&newest.key, &"a".repeat(64), "stale report")
                .await
                .unwrap(),
            "a stale corruption report must not invalidate a replacement manifest"
        );
        assert_eq!(
            s.get(newest.id).await.unwrap().unwrap().state,
            ArtifactState::Ready
        );
        assert!(
            s.quarantine_publication(&newest.key, &"b".repeat(64), "verified corruption")
                .await
                .unwrap()
        );
        let quarantined = s.get(newest.id).await.unwrap().unwrap();
        assert_eq!(quarantined.state, ArtifactState::Failed);
        assert!(quarantined.manifest.is_none());
        assert!(
            s.published("ws", "o/r", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn backlog_one_credits_superseded_tips_but_preserves_clone_subscribers() {
        let limits = SchedulerLimits {
            total_backlog: 1,
            workspace_backlog: 1,
            head_backlog: 1,
            head_reserved: 0,
            ..Default::default()
        };
        let (s, _d, _) = scheduler(limits).await;
        s.observe("ws", "o/r", "main", "t1", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        s.observe("ws", "o/r", "main", "t2", &[ArtifactKind::Head], 1, Some(1))
            .await
            .unwrap();
        s.observe("ws", "o/r", "main", "t3", &[ArtifactKind::Head], 1, Some(2))
            .await
            .unwrap();
        assert!(
            s.get_by_key(&key("ws", "t1", ArtifactKind::Head))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            s.get_by_key(&key("ws", "t2", ArtifactKind::Head))
                .await
                .unwrap()
                .is_none()
        );
        let t3 = s.claim("w", 5).await.unwrap().unwrap();
        assert_eq!(t3.record.key.commit, "t3");

        let (s, _d, _) = scheduler(SchedulerLimits {
            total_backlog: 1,
            workspace_backlog: 1,
            head_backlog: 1,
            head_reserved: 0,
            ..Default::default()
        })
        .await;
        s.observe("ws", "o/r", "main", "t1", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        s.subscribe_consumer(&key("ws", "t1", ArtifactKind::Head), "clone", 60)
            .await
            .unwrap();
        assert!(
            s.observe("ws", "o/r", "main", "t2", &[ArtifactKind::Head], 1, Some(1))
                .await
                .is_err(),
            "durable clone subscriber must consume capacity rather than be pruned"
        );
        assert!(
            s.get_by_key(&key("ws", "t1", ArtifactKind::Head))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn partial_kind_observation_does_not_delete_other_kind_desired_work() {
        let (s, _d, _) = scheduler(Default::default()).await;
        s.observe(
            "ws",
            "o/r",
            "main",
            "t1",
            &[ArtifactKind::Head, ArtifactKind::Files],
            1,
            None,
        )
        .await
        .unwrap();
        s.observe("ws", "o/r", "main", "t2", &[ArtifactKind::Head], 1, Some(1))
            .await
            .unwrap();
        assert!(
            s.get_by_key(&key("ws", "t1", ArtifactKind::Head))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            s.get_by_key(&key("ws", "t1", ArtifactKind::Files))
                .await
                .unwrap()
                .unwrap()
                .state,
            ArtifactState::Queued
        );
    }
    #[tokio::test]
    async fn crashed_clone_subscription_expires_and_releases_backlog() {
        let (s, _d, _) = scheduler(Default::default()).await;
        let k = key("ws", "orphan", ArtifactKind::Head);
        let out = s.subscribe_consumer(&k, "clone", 60).await.unwrap();
        let id = outcome_id(&out);
        sqlx::query("UPDATE artifact_consumers SET expires_at=unixepoch()-1 WHERE artifact_id=?")
            .bind(id)
            .execute(&s.pool)
            .await
            .unwrap();
        s.reconcile_expired().await.unwrap();
        assert!(s.get(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn alias_never_returns_a_prior_incompatible_format() {
        let (s, _d, _) = scheduler(Default::default()).await;
        s.observe("ws", "o/r", "main", "t1", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        let c = s.claim("w", 5).await.unwrap().unwrap();
        s.complete(&c, "w", &evidence(&c)).await.unwrap();
        assert!(
            s.published("ws", "o/r", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .is_some()
        );
        s.observe("ws", "o/r", "main", "t2", &[ArtifactKind::Head], 2, Some(1))
            .await
            .unwrap();
        assert!(
            s.published("ws", "o/r", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            s.published("ws", "o/r", "main", ArtifactKind::Head, 2)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn concurrent_workers_cannot_establish_mismatched_fleet_limits() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("fleet.db").to_string_lossy().to_string();
        let a = SchedulerLimits {
            workspace_running: 2,
            ..Default::default()
        };
        let b = SchedulerLimits {
            workspace_running: 3,
            ..Default::default()
        };
        let (x, y) = tokio::join!(
            ArtifactScheduler::open(&p, a),
            ArtifactScheduler::open(&p, b)
        );
        assert_eq!([x.is_ok(), y.is_ok()].into_iter().filter(|v| *v).count(), 1);
        let pool = SqlitePool::connect(&format!("sqlite://{p}")).await.unwrap();
        let sealed: String =
            sqlx::query_scalar("SELECT limits_fingerprint FROM scheduler_state WHERE id=1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            sealed
                == scheduler_limits_fingerprint(&SchedulerLimits {
                    workspace_running: 2,
                    ..Default::default()
                })
                || sealed
                    == scheduler_limits_fingerprint(&SchedulerLimits {
                        workspace_running: 3,
                        ..Default::default()
                    })
        );
        let mut fresh = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *fresh)
            .await
            .unwrap();
        sqlx::query("ROLLBACK").execute(&mut *fresh).await.unwrap();
    }

    #[tokio::test]
    async fn legacy_scheduler_state_missing_limits_fingerprint_migrates_exactly() {
        let d = tempfile::tempdir().unwrap();
        let p = d
            .path()
            .join("legacy-limits.db")
            .to_string_lossy()
            .to_string();
        let scheduler = ArtifactScheduler::open(&p, Default::default())
            .await
            .unwrap();
        sqlx::query("ALTER TABLE scheduler_state DROP COLUMN limits_fingerprint")
            .execute(&scheduler.pool)
            .await
            .unwrap();
        drop(scheduler);
        let reopened = ArtifactScheduler::open(&p, Default::default())
            .await
            .unwrap();
        let row:(String,i64)=sqlx::query_as("SELECT limits_fingerprint,(SELECT count(*) FROM pragma_table_info('scheduler_state') WHERE name='limits_fingerprint' AND upper(type)='TEXT' AND [notnull]=1 AND dflt_value=\"''\" AND pk=0) FROM scheduler_state WHERE id=1").fetch_one(&reopened.pool).await.unwrap();
        assert_eq!(
            row.0,
            scheduler_limits_fingerprint(&SchedulerLimits::default())
        );
        assert_eq!(row.1, 1);
    }

    #[tokio::test]
    async fn malformed_scheduler_limits_column_and_state_fail_closed() {
        let d = tempfile::tempdir().unwrap();
        let p = d
            .path()
            .join("malformed-limits.db")
            .to_string_lossy()
            .to_string();
        let scheduler = ArtifactScheduler::open(&p, Default::default())
            .await
            .unwrap();
        sqlx::query("UPDATE scheduler_state SET limits_fingerprint='bad'")
            .execute(&scheduler.pool)
            .await
            .unwrap();
        drop(scheduler);
        assert!(
            ArtifactScheduler::open(&p, Default::default())
                .await
                .is_err()
        );

        let d2 = tempfile::tempdir().unwrap();
        let p2 = d2
            .path()
            .join("malformed-column.db")
            .to_string_lossy()
            .to_string();
        let scheduler = ArtifactScheduler::open(&p2, Default::default())
            .await
            .unwrap();
        sqlx::raw_sql("ALTER TABLE scheduler_state RENAME TO scheduler_state_old; CREATE TABLE scheduler_state(id INTEGER PRIMARY KEY CHECK(id=1),fairness_cursor INTEGER NOT NULL,workspace_cursor TEXT NOT NULL DEFAULT '',config_fingerprint TEXT NOT NULL DEFAULT '',limits_fingerprint INTEGER NOT NULL DEFAULT 0); INSERT INTO scheduler_state SELECT id,fairness_cursor,workspace_cursor,config_fingerprint,0 FROM scheduler_state_old; DROP TABLE scheduler_state_old;").execute(&scheduler.pool).await.unwrap();
        drop(scheduler);
        assert!(
            ArtifactScheduler::open(&p2, Default::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn concurrent_workers_cannot_establish_different_verifier_identities() {
        struct Named(&'static str);
        impl CompletionVerifier for Named {
            fn identity(&self) -> &str {
                self.0
            }
            fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
                validate_evidence(claim, evidence)
            }
        }
        let d = tempfile::tempdir().unwrap();
        let p = d
            .path()
            .join("verifier-fleet.db")
            .to_string_lossy()
            .to_string();
        let (a, b) = tokio::join!(
            ArtifactScheduler::open_with_verifier(
                &p,
                Default::default(),
                Arc::new(Named("verifier-a"))
            ),
            ArtifactScheduler::open_with_verifier(
                &p,
                Default::default(),
                Arc::new(Named("verifier-b"))
            )
        );
        assert_eq!([a.is_ok(), b.is_ok()].into_iter().filter(|v| *v).count(), 1);
    }

    #[tokio::test]
    async fn shared_database_fences_processes_with_different_proof_keys() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("proof-key-fleet.db");
        let cas = Cas::new(root.path().join("cas")).unwrap();
        let verifier_a = CasCompletionVerifier::new(cas.clone())
            .with_proof_key(&[b'a'; 32])
            .unwrap();
        let verifier_b = CasCompletionVerifier::new(cas)
            .with_proof_key(&[b'b'; 32])
            .unwrap();
        assert_ne!(verifier_a.identity(), verifier_b.identity());
        let path = path.to_string_lossy();
        let (a, b) = tokio::join!(
            ArtifactScheduler::open_with_verifier(&path, Default::default(), Arc::new(verifier_a)),
            ArtifactScheduler::open_with_verifier(&path, Default::default(), Arc::new(verifier_b))
        );
        assert_eq!(
            [a.is_ok(), b.is_ok()].into_iter().filter(|ok| *ok).count(),
            1
        );
    }

    #[tokio::test]
    async fn verified_completion_capability_is_attempt_and_verifier_instance_bound() {
        let (s, _d, _) = scheduler(Default::default()).await;
        let first_key = key("ws", "first", ArtifactKind::Head);
        let second_key = key("ws", "second", ArtifactKind::Head);
        s.schedule(&first_key).await.unwrap();
        s.schedule(&second_key).await.unwrap();
        let first = s.claim("worker-a", 30).await.unwrap().unwrap();
        let second = s.claim("worker-b", 30).await.unwrap().unwrap();
        let raw = evidence(&first);
        s.verifier.verify(&first, &raw).unwrap();
        let capability = s.completion_sealer.seal(&first, raw).unwrap();
        let cloned = capability.clone();

        assert!(s.completion_sealer.verify(&first, &cloned).is_ok());
        assert!(s.completion_sealer.verify(&second, &cloned).is_err());
        let next_lease = ClaimedArtifact {
            record: ArtifactRecord {
                lease_generation: first.record.lease_generation + 1,
                ..first.record.clone()
            },
        };
        assert!(s.completion_sealer.verify(&next_lease, &cloned).is_err());

        let same_named_different_instance =
            CompletionSealAuthority::new(s.verifier.identity()).unwrap();
        assert!(
            same_named_different_instance
                .verify(&first, &cloned)
                .is_err()
        );

        assert!(
            s.complete_verified(&first, "worker-a", &capability)
                .await
                .unwrap()
        );
        assert!(
            !s.complete_verified(&first, "worker-a", &capability)
                .await
                .unwrap(),
            "a valid capability cannot replay after its attempt settled"
        );
    }

    #[tokio::test]
    async fn raw_completion_always_runs_the_configured_verifier() {
        struct CountingVerifier(Arc<std::sync::atomic::AtomicUsize>);
        impl CompletionVerifier for CountingVerifier {
            fn identity(&self) -> &str {
                "counting-verifier-v1"
            }
            fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
                self.0.fetch_add(1, Ordering::SeqCst);
                validate_evidence(claim, evidence)
            }
        }

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let d = tempfile::tempdir().unwrap();
        let db_path = d.path().join("always-verify.db");
        let path = db_path.to_string_lossy();
        let scheduler = ArtifactScheduler::open_with_verifier(
            &path,
            Default::default(),
            Arc::new(CountingVerifier(calls.clone())),
        )
        .await
        .unwrap();
        let k = key("ws", "raw", ArtifactKind::Head);
        scheduler.schedule(&k).await.unwrap();
        let claim = scheduler.claim("worker", 30).await.unwrap().unwrap();
        assert!(
            scheduler
                .complete(&claim, "worker", &evidence(&claim))
                .await
                .unwrap()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_is_not_revived_without_explicit_bounded_retry() {
        let (s, _d, _) = scheduler(SchedulerLimits {
            max_manual_retries: 1,
            ..Default::default()
        })
        .await;
        let k = key("ws", "a", ArtifactKind::Head);
        s.schedule(&k).await.unwrap();
        let c = s.claim("w", 5).await.unwrap().unwrap();
        s.fail(&c, "w", FailureClass::Retryable, "transient")
            .await
            .unwrap();
        assert!(matches!(
            s.schedule(&k).await.unwrap(),
            ScheduleOutcome::Failed(_, FailureClass::Retryable)
        ));
        assert!(matches!(
            s.retry_failed(&k).await.unwrap(),
            RetryOutcome::Requeued(_)
        ));
        let c = s.claim("w", 5).await.unwrap().unwrap();
        s.fail(&c, "w", FailureClass::Retryable, "again")
            .await
            .unwrap();
        assert_eq!(s.retry_failed(&k).await.unwrap(), RetryOutcome::Exhausted);
        let p = key("ws", "p", ArtifactKind::Files);
        s.schedule(&p).await.unwrap();
        let c = s.claim("w", 5).await.unwrap().unwrap();
        s.fail(&c, "w", FailureClass::Permanent, "bad")
            .await
            .unwrap();
        assert_eq!(
            s.retry_failed(&p).await.unwrap(),
            RetryOutcome::NotRetryable(FailureClass::Permanent)
        )
    }

    #[tokio::test]
    async fn lease_generation_never_resets_and_fences_same_owner_aba() {
        let (s, _d, _) = scheduler(Default::default()).await;
        let k = key("ws", "a", ArtifactKind::Head);
        s.schedule(&k).await.unwrap();
        let old = s.claim("same", 5).await.unwrap().unwrap();
        expire(&s, old.record.id).await;
        assert_eq!(s.reconcile_expired().await.unwrap(), (1, 0));
        let new = s.claim("same", 5).await.unwrap().unwrap();
        assert!(new.record.lease_generation > old.record.lease_generation);
        assert!(!s.complete(&old, "same", &evidence(&old)).await.unwrap());
        assert!(s.complete(&new, "same", &evidence(&new)).await.unwrap())
    }

    #[tokio::test]
    async fn evidence_must_be_nonempty_typed_and_exact() {
        let (s, _d, _) = scheduler(Default::default()).await;
        let k = key("ws", "a", ArtifactKind::Head);
        assert!(CompletionEvidence::new(k.clone(), " ").is_err());
        s.schedule(&k).await.unwrap();
        let c = s.claim("w", 5).await.unwrap().unwrap();
        let wrong = CompletionEvidence::new(key("ws", "a", ArtifactKind::Files), "x").unwrap();
        assert!(s.complete(&c, "w", &wrong).await.is_err());
        let mut empty = evidence(&c);
        empty.artifact_count = 0;
        assert!(s.complete(&c, "w", &empty).await.is_err());
        assert!(s.complete(&c, "w", &evidence(&c)).await.unwrap())
    }

    #[tokio::test]
    async fn kind_specific_completion_verifier_is_a_mandatory_publish_gate() {
        struct RejectFiles;
        impl CompletionVerifier for RejectFiles {
            fn identity(&self) -> &str {
                "reject-files-v1"
            }
            fn verify(&self, claim: &ClaimedArtifact, e: &CompletionEvidence) -> Result<()> {
                validate_evidence(claim, e)?;
                if claim.record.key.kind == ArtifactKind::Files {
                    bail!("files manifest missing frame table")
                };
                Ok(())
            }
        }
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("verify.db").to_string_lossy().to_string();
        let s =
            ArtifactScheduler::open_with_verifier(&p, Default::default(), Arc::new(RejectFiles))
                .await
                .unwrap();
        let k = key("ws", "a", ArtifactKind::Files);
        s.schedule(&k).await.unwrap();
        let c = s.claim("w", 5).await.unwrap().unwrap();
        assert!(s.complete(&c, "w", &evidence(&c)).await.is_err());
        assert_eq!(
            s.get(c.record.id).await.unwrap().unwrap().state,
            ArtifactState::Running
        );
    }

    #[tokio::test]
    async fn lost_lease_cancels_and_drains_cooperative_and_blocking_children() {
        let (s, d, _) = scheduler(Default::default()).await;
        let k = key("ws", "a", ArtifactKind::FullHistory);
        s.schedule(&k).await.unwrap();
        let c = s.claim("w", 2).await.unwrap().unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let ca = cancelled.clone();
        let fi = finished.clone();
        let task = ArtifactTask::cooperative(move |ctx| async move {
            let token = ctx.cancelled.clone();
            tokio::task::spawn_blocking(move || {
                while !token.is_cancelled() {
                    std::thread::sleep(Duration::from_millis(10))
                }
                ca.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(40));
                fi.store(true, Ordering::SeqCst)
            })
            .await?;
            Ok(())
        });
        let runner = {
            let s = s.clone();
            let c = c.clone();
            let root = d.path().to_path_buf();
            tokio::spawn(async move {
                s.run_owned(&c, "w", vec![task], evidence(&c), 2, &root)
                    .await
                    .unwrap()
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        expire(&s, c.record.id).await;
        s.reconcile_expired().await.unwrap();
        assert_eq!(runner.await.unwrap(), ExecutionOutcome::LostLease);
        assert!(
            cancelled.load(Ordering::SeqCst) && finished.load(Ordering::SeqCst),
            "blocking child must be drained"
        )
    }

    #[tokio::test]
    async fn run_owned_preflights_db_ownership_and_drains_panics() {
        let (s, d, _) = scheduler(Default::default()).await;
        let k = key("ws", "a", ArtifactKind::Files);
        s.schedule(&k).await.unwrap();
        let c = s.claim("w", 5).await.unwrap().unwrap();
        let stale = ClaimedArtifact {
            record: ArtifactRecord {
                lease_generation: c.record.lease_generation + 1,
                ..c.record.clone()
            },
        };
        assert!(
            s.run_owned(&stale, "w", vec![], evidence(&stale), 5, d.path())
                .await
                .is_err()
        );
        let dropped = Arc::new(AtomicBool::new(false));
        struct D(Arc<AtomicBool>);
        impl Drop for D {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst)
            }
        }
        let guard = D(dropped.clone());
        let blocked = ArtifactTask::cooperative(move |ctx| async move {
            let _g = guard;
            ctx.cancelled.cancelled().await;
            Ok(())
        });
        let panic = ArtifactTask::cooperative(|_| async move { panic!("boom") });
        assert_eq!(
            s.run_owned(&c, "w", vec![blocked, panic], evidence(&c), 5, d.path())
                .await
                .unwrap(),
            ExecutionOutcome::Failed
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            s.get(c.record.id).await.unwrap().unwrap().state,
            ArtifactState::Failed
        )
    }

    #[tokio::test]
    async fn owned_build_preflights_before_start_and_completes_returned_evidence() {
        use crate::artifact_scheduler_backend::{ArtifactSchedulerPersistence, OwnedArtifactBuild};

        let (s, d, _) = scheduler(Default::default()).await;
        let k = key("ws", "owned-build", ArtifactKind::Head);
        s.schedule(&k).await.unwrap();
        let claim = s.claim("worker", 5).await.unwrap().unwrap();
        let stale = ClaimedArtifact {
            record: ArtifactRecord {
                lease_generation: claim.record.lease_generation + 1,
                ..claim.record.clone()
            },
        };
        let started = Arc::new(AtomicBool::new(false));
        let start_flag = started.clone();
        let stale_build = OwnedArtifactBuild::cooperative(move |_| async move {
            start_flag.store(true, Ordering::SeqCst);
            unreachable!("a stale build must never start")
        });
        assert!(
            ArtifactSchedulerPersistence::run_owned_build(
                &s,
                &stale,
                "worker",
                stale_build,
                5,
                d.path(),
            )
            .await
            .is_err()
        );
        assert!(!started.load(Ordering::SeqCst));

        let returned = CompletionEvidence::new(k, "returned-by-owned-build").unwrap();
        let expected = returned.clone();
        let outcome = ArtifactSchedulerPersistence::run_owned_build(
            &s,
            &claim,
            "worker",
            OwnedArtifactBuild::cooperative(move |context| async move {
                assert!(!context.cancelled.is_cancelled());
                std::fs::write(context.scratch.join("proof"), b"ok")?;
                Ok(returned)
            }),
            5,
            d.path(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ExecutionOutcome::Ready);
        let record = s.get(claim.record.id).await.unwrap().unwrap();
        assert_eq!(record.state, ArtifactState::Ready);
        assert_eq!(record.manifest.as_deref(), Some(expected.manifest.as_str()));
    }

    #[tokio::test]
    async fn owned_build_lease_loss_cancels_drains_and_never_publishes() {
        use crate::artifact_scheduler_backend::{ArtifactSchedulerPersistence, OwnedArtifactBuild};

        let (s, d, _) = scheduler(Default::default()).await;
        let k = key("ws", "owned-build-loss", ArtifactKind::FullHistory);
        s.schedule(&k).await.unwrap();
        let claim = s.claim("worker", 2).await.unwrap().unwrap();
        let child_observed_cancel = Arc::new(AtomicBool::new(false));
        let child_drained = Arc::new(AtomicBool::new(false));
        let observed = child_observed_cancel.clone();
        let drained = child_drained.clone();
        let result_evidence = CompletionEvidence::new(k, "must-not-publish").unwrap();
        let runner = {
            let scheduler = s.clone();
            let owned_claim = claim.clone();
            let scratch_root = d.path().to_path_buf();
            tokio::spawn(async move {
                ArtifactSchedulerPersistence::run_owned_build(
                    &scheduler,
                    &owned_claim,
                    "worker",
                    OwnedArtifactBuild::cooperative(move |context| async move {
                        let cancelled = context.cancelled.clone();
                        tokio::task::spawn_blocking(move || {
                            while !cancelled.is_cancelled() {
                                std::thread::sleep(Duration::from_millis(10));
                            }
                            observed.store(true, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(40));
                            drained.store(true, Ordering::SeqCst);
                        })
                        .await?;
                        Ok(result_evidence)
                    }),
                    2,
                    &scratch_root,
                )
                .await
                .unwrap()
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        expire(&s, claim.record.id).await;
        s.reconcile_expired().await.unwrap();
        assert_eq!(runner.await.unwrap(), ExecutionOutcome::LostLease);
        assert!(child_observed_cancel.load(Ordering::SeqCst));
        assert!(child_drained.load(Ordering::SeqCst));
        let record = s.get(claim.record.id).await.unwrap().unwrap();
        assert_ne!(record.state, ArtifactState::Ready);
        assert!(record.manifest.is_none());
    }

    #[tokio::test]
    async fn owned_build_rejected_evidence_fails_attempt_and_cleans_scratch() {
        use crate::artifact_scheduler_backend::{ArtifactSchedulerPersistence, OwnedArtifactBuild};

        struct RejectReturnedEvidence;
        impl CompletionVerifier for RejectReturnedEvidence {
            fn identity(&self) -> &str {
                "reject-returned-evidence-v1"
            }

            fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
                validate_evidence(claim, evidence)?;
                bail!("CAS receipt is corrupt")
            }
        }

        let d = tempfile::tempdir().unwrap();
        let db = d.path().join("rejected-owned-build.db");
        let s = ArtifactScheduler::open_with_verifier(
            db.to_string_lossy().as_ref(),
            Default::default(),
            Arc::new(RejectReturnedEvidence),
        )
        .await
        .unwrap();
        let k = key("ws", "rejected-evidence", ArtifactKind::Files);
        s.schedule(&k).await.unwrap();
        let claim = s.claim("worker", 5).await.unwrap().unwrap();
        let scratch = d.path().join(format!(
            "artifact-{}-lease-{}",
            claim.record.id, claim.record.lease_generation
        ));
        let outcome = ArtifactSchedulerPersistence::run_owned_build(
            &s,
            &claim,
            "worker",
            OwnedArtifactBuild::cooperative(move |_| async move {
                Ok(CompletionEvidence::new(k, "rejected-manifest").unwrap())
            }),
            5,
            d.path(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ExecutionOutcome::Failed);
        let record = s.get(claim.record.id).await.unwrap().unwrap();
        assert_eq!(record.state, ArtifactState::Failed);
        assert!(record.manifest.is_none());
        assert!(record.error.unwrap().contains("CAS receipt is corrupt"));
        assert!(!scratch.exists());

        let retry_key = key("ws", "wrong-evidence", ArtifactKind::Head);
        s.schedule(&retry_key).await.unwrap();
        let wrong_claim = s.claim("worker", 5).await.unwrap().unwrap();
        let wrong_key = key("other-workspace", "wrong-evidence", ArtifactKind::Head);
        assert_eq!(
            ArtifactSchedulerPersistence::run_owned_build(
                &s,
                &wrong_claim,
                "worker",
                OwnedArtifactBuild::cooperative(move |_| async move {
                    Ok(CompletionEvidence::new(wrong_key, "wrong-key").unwrap())
                }),
                5,
                d.path(),
            )
            .await
            .unwrap(),
            ExecutionOutcome::Failed
        );
        assert_eq!(
            s.get(wrong_claim.record.id).await.unwrap().unwrap().state,
            ArtifactState::Failed
        );
    }

    #[tokio::test]
    async fn owned_build_cannot_become_ready_when_durable_publication_fails() {
        use crate::artifact_scheduler_backend::{ArtifactSchedulerPersistence, OwnedArtifactBuild};

        struct PublishFails;
        impl CompletionVerifier for PublishFails {
            fn identity(&self) -> &str {
                "publish-fails-v1"
            }

            fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
                validate_evidence(claim, evidence)
            }

            fn publish_owned<'a>(
                &'a self,
                _claim: &'a ClaimedArtifact,
                _evidence: &'a CompletionEvidence,
                _context: &'a ExecutionContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>
            {
                Box::pin(async { bail!("injected durable publication failure") })
            }
        }

        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("publication-failure.db");
        let scheduler = ArtifactScheduler::open_with_verifier(
            &path.to_string_lossy(),
            Default::default(),
            Arc::new(PublishFails),
        )
        .await
        .unwrap();
        let key = key("ws", "durability", ArtifactKind::Head);
        scheduler.schedule(&key).await.unwrap();
        let claim = scheduler.claim("worker", 5).await.unwrap().unwrap();
        let returned = evidence(&claim);
        let outcome = ArtifactSchedulerPersistence::run_owned_build(
            &scheduler,
            &claim,
            "worker",
            OwnedArtifactBuild::cooperative(move |_| async move { Ok(returned) }),
            5,
            root.path(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ExecutionOutcome::Failed);
        let record = scheduler.get(claim.record.id).await.unwrap().unwrap();
        assert_eq!(record.state, ArtifactState::Failed);
        assert!(record.manifest.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_owned_build_does_not_starve_heartbeats() {
        use crate::artifact_scheduler_backend::{ArtifactSchedulerPersistence, OwnedArtifactBuild};
        use std::sync::Barrier;

        let (s, d, _) = scheduler(Default::default()).await;
        let k = key("ws", "blocking-heartbeat", ArtifactKind::Head);
        s.schedule(&k).await.unwrap();
        let claim = s.claim("worker", 2).await.unwrap().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let build_barrier = barrier.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let runner = {
            let scheduler = s.clone();
            let owned_claim = claim.clone();
            let root = d.path().to_path_buf();
            tokio::spawn(async move {
                ArtifactSchedulerPersistence::run_owned_build(
                    &scheduler,
                    &owned_claim,
                    "worker",
                    OwnedArtifactBuild::blocking(move |_| {
                        let _ = started_tx.send(());
                        build_barrier.wait();
                        std::thread::sleep(Duration::from_millis(2_200));
                        CompletionEvidence::new(k, "blocking-result")
                    }),
                    2,
                    &root,
                )
                .await
                .unwrap()
            })
        };
        // This synchronous rendezvous would deadlock a current-thread runtime
        // if the build closure were invoked directly on the Tokio worker.
        started_rx.await.unwrap();
        barrier.wait();
        assert_eq!(runner.await.unwrap(), ExecutionOutcome::Ready);
        assert_eq!(
            s.get(claim.record.id).await.unwrap().unwrap().state,
            ArtifactState::Ready
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_owned_verification_is_heartbeated_and_not_repeated_at_publish() {
        use crate::artifact_scheduler_backend::{ArtifactSchedulerPersistence, OwnedArtifactBuild};

        struct SlowOwnedVerifier {
            owned_calls: Arc<std::sync::atomic::AtomicUsize>,
            plain_calls: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl CompletionVerifier for SlowOwnedVerifier {
            fn identity(&self) -> &str {
                "slow-owned-verifier-v1"
            }
            fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
                self.plain_calls.fetch_add(1, Ordering::SeqCst);
                bail!("publication repeated production verification")
            }
            fn verify_owned(
                &self,
                claim: &ClaimedArtifact,
                evidence: &CompletionEvidence,
                context: &ExecutionContext,
            ) -> Result<()> {
                validate_evidence(claim, evidence)?;
                self.owned_calls.fetch_add(1, Ordering::SeqCst);
                for _ in 0..110 {
                    if context.cancelled.is_cancelled() {
                        bail!("slow verifier cancelled");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(())
            }
        }

        let d = tempfile::tempdir().unwrap();
        let owned_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let plain_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let s = ArtifactScheduler::open_with_verifier(
            d.path().join("slow-verify.db").to_string_lossy().as_ref(),
            Default::default(),
            Arc::new(SlowOwnedVerifier {
                owned_calls: owned_calls.clone(),
                plain_calls: plain_calls.clone(),
            }),
        )
        .await
        .unwrap();
        let k = key("ws", "slow-verify", ArtifactKind::Head);
        s.schedule(&k).await.unwrap();
        let claim = s.claim("worker", 2).await.unwrap().unwrap();
        let outcome = ArtifactSchedulerPersistence::run_owned_build(
            &s,
            &claim,
            "worker",
            OwnedArtifactBuild::cooperative(move |_| async move {
                CompletionEvidence::new(k, "verified-result")
            }),
            2,
            d.path(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ExecutionOutcome::Ready);
        assert_eq!(owned_calls.load(Ordering::SeqCst), 1);
        assert_eq!(plain_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn lease_loss_during_owned_verification_cancels_drains_and_never_publishes() {
        use crate::artifact_scheduler_backend::{ArtifactSchedulerPersistence, OwnedArtifactBuild};

        struct WaitForCancelVerifier {
            started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            drained: Arc<AtomicBool>,
        }
        impl CompletionVerifier for WaitForCancelVerifier {
            fn identity(&self) -> &str {
                "wait-for-cancel-verifier-v1"
            }
            fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
                bail!("non-owned verification must not run")
            }
            fn verify_owned(
                &self,
                _: &ClaimedArtifact,
                _: &CompletionEvidence,
                context: &ExecutionContext,
            ) -> Result<()> {
                if let Some(started) = self.started.lock().unwrap().take() {
                    let _ = started.send(());
                }
                while !context.cancelled.is_cancelled() {
                    std::thread::sleep(Duration::from_millis(10));
                }
                self.drained.store(true, Ordering::SeqCst);
                bail!("verification cancelled after lease loss")
            }
        }

        let d = tempfile::tempdir().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let drained = Arc::new(AtomicBool::new(false));
        let s = ArtifactScheduler::open_with_verifier(
            d.path().join("verify-loss.db").to_string_lossy().as_ref(),
            Default::default(),
            Arc::new(WaitForCancelVerifier {
                started: std::sync::Mutex::new(Some(started_tx)),
                drained: drained.clone(),
            }),
        )
        .await
        .unwrap();
        let k = key("ws", "verify-loss", ArtifactKind::Files);
        s.schedule(&k).await.unwrap();
        let claim = s.claim("worker", 2).await.unwrap().unwrap();
        let runner = {
            let s = s.clone();
            let claim = claim.clone();
            let root = d.path().to_owned();
            tokio::spawn(async move {
                ArtifactSchedulerPersistence::run_owned_build(
                    &s,
                    &claim,
                    "worker",
                    OwnedArtifactBuild::cooperative(move |_| async move {
                        CompletionEvidence::new(k, "never-ready")
                    }),
                    2,
                    &root,
                )
                .await
                .unwrap()
            })
        };
        started_rx.await.unwrap();
        expire(&s, claim.record.id).await;
        s.reconcile_expired().await.unwrap();
        assert_eq!(runner.await.unwrap(), ExecutionOutcome::LostLease);
        assert!(drained.load(Ordering::SeqCst));
        let record = s.get(claim.record.id).await.unwrap().unwrap();
        assert_ne!(record.state, ArtifactState::Ready);
        assert!(record.manifest.is_none());
    }

    #[tokio::test]
    async fn global_kind_and_per_repo_expensive_caps_are_fleet_wide() {
        let limits = SchedulerLimits {
            total_running: 3,
            head_running: 1,
            full_history_running: 1,
            files_running: 1,
            ..Default::default()
        };
        let (s, _d, _) = scheduler(limits).await;
        for (k, c) in [
            (ArtifactKind::Head, "h1"),
            (ArtifactKind::Head, "h2"),
            (ArtifactKind::FullHistory, "f"),
            (ArtifactKind::Files, "x"),
        ] {
            s.schedule(&key("ws", c, k)).await.unwrap();
        }
        let a = s.claim("a", 5).await.unwrap().unwrap();
        assert_eq!(a.record.key.kind, ArtifactKind::Head);
        let b = s.claim("b", 5).await.unwrap().unwrap();
        assert!(b.record.key.kind.expensive());
        // A newer generation of the running kind remains excluded, while the
        // independent expensive sibling is allowed to run concurrently.
        s.schedule(&key("ws", "same-kind-newer", b.record.key.kind))
            .await
            .unwrap();
        let c = s.claim("c", 5).await.unwrap().unwrap();
        assert!(c.record.key.kind.expensive());
        assert_ne!(b.record.key.kind, c.record.key.kind);
        assert!(s.claim("d", 5).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn durable_round_robin_and_workspace_backlog_isolation() {
        let limits = SchedulerLimits {
            workspace_backlog: 2,
            total_backlog: 5,
            head_reserved: 0,
            ..Default::default()
        };
        let (s, _d, _) = scheduler(limits).await;
        s.schedule(&key("a", "h1", ArtifactKind::Head))
            .await
            .unwrap();
        s.schedule(&key("a", "h2", ArtifactKind::Head))
            .await
            .unwrap();
        assert!(
            s.schedule(&key("a", "h3", ArtifactKind::Head))
                .await
                .is_err()
        );
        s.schedule(&key("b", "f", ArtifactKind::FullHistory))
            .await
            .unwrap();
        let first = s.claim("w", 5).await.unwrap().unwrap();
        assert_eq!(first.record.key.kind, ArtifactKind::Head);
        let second = s.claim("w2", 5).await.unwrap().unwrap();
        assert_eq!(second.record.key.kind, ArtifactKind::Head);
        let third = s.claim("w3", 5).await.unwrap().unwrap();
        assert_eq!(third.record.key.kind, ArtifactKind::FullHistory)
    }

    #[tokio::test]
    async fn invalid_lease_duration_and_dead_letter_fail_closed() {
        let (s, _d, _) = scheduler(SchedulerLimits {
            max_claim_attempts: 1,
            ..Default::default()
        })
        .await;
        let k = key("ws", "a", ArtifactKind::Head);
        s.schedule(&k).await.unwrap();
        assert!(s.claim("", 5).await.is_err());
        assert!(s.claim("w", 0).await.is_err());
        let c = s.claim("w", 2).await.unwrap().unwrap();
        expire(&s, c.record.id).await;
        assert_eq!(s.reconcile_expired().await.unwrap(), (0, 1));
        assert!(matches!(
            s.schedule(&k).await.unwrap(),
            ScheduleOutcome::Failed(_, FailureClass::DeadLetter)
        ));
        assert_eq!(
            s.retry_failed(&k).await.unwrap(),
            RetryOutcome::NotRetryable(FailureClass::DeadLetter)
        )
    }

    #[tokio::test]
    async fn initial_foundation_database_migrates_without_losing_existing_rows() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("legacy.db").to_string_lossy().to_string();
        let pool = legacy_pool(&p).await;
        sqlx::raw_sql(
            "INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('ws','o/r','legacy','head',7,'ready','unverified-legacy-manifest',1,1);
             INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,published_artifact_id,observed_at) VALUES('ws','o/r','main','head','legacy',1,1,1);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        let s = ArtifactScheduler::open(&p, Default::default())
            .await
            .unwrap();
        let legacy_key = ArtifactKey {
            workspace: "ws".into(),
            repo: "o/r".into(),
            commit: "legacy".into(),
            kind: ArtifactKind::Head,
            format_version: 7,
        };
        let legacy = s.get_by_key(&legacy_key).await.unwrap().unwrap();
        assert_eq!(legacy.lease_generation, 0);
        assert_eq!(legacy.state, ArtifactState::Queued);
        assert_eq!(legacy.manifest, None);
        let (migrated_format, published): (i64, Option<i64>) = sqlx::query_as(
            "SELECT format_version,published_artifact_id FROM artifact_observations WHERE workspace='ws' AND repo='o/r' AND branch='main' AND kind='head'",
        )
        .fetch_one(&s.pool)
        .await
        .unwrap();
        assert_eq!(migrated_format, 7);
        assert_eq!(published, None, "legacy publication evidence is untrusted");
        let claim = s.claim("worker", 5).await.unwrap().unwrap();
        assert_eq!(claim.record.lease_generation, 1);
        assert_eq!(
            s.observe(
                "ws",
                "o/r",
                "main",
                "legacy",
                &[ArtifactKind::Head],
                7,
                None
            )
            .await
            .unwrap(),
            ObservationOutcome::Unchanged { generation: 1 }
        );
        assert!(matches!(
            s.observe(
                "ws",
                "o/r",
                "main",
                "legacy",
                &[ArtifactKind::Head],
                7,
                Some(1)
            )
            .await
            .unwrap(),
            ObservationOutcome::Unchanged { generation: 1 }
        ));
    }

    #[tokio::test]
    async fn migration_rejects_mismatched_desired_identity_and_invalid_format() {
        for (name, job_commit, desired_commit, format_version) in [
            ("identity", "actual", "forged", 1_i64),
            ("format", "actual", "actual", -1_i64),
        ] {
            let d = tempfile::tempdir().unwrap();
            let p = d
                .path()
                .join(format!("{name}.db"))
                .to_string_lossy()
                .to_string();
            let pool = legacy_pool(&p).await;
            sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r',?,'head',?,'queued',1,1)")
                .bind(job_commit)
                .bind(format_version)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,published_artifact_id,observed_at) VALUES('ws','o/r','main','head',?,1,NULL,1)")
                .bind(desired_commit)
                .execute(&pool)
                .await
                .unwrap();
            pool.close().await;
            assert!(
                ArtifactScheduler::open(&p, Default::default())
                    .await
                    .is_err(),
                "migration accepted invalid {name}"
            );
        }
    }

    #[tokio::test]
    async fn migration_rejects_conflicting_latest_branch_observations() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("conflict.db").to_string_lossy().to_string();
        let pool = legacy_pool(&p).await;
        sqlx::raw_sql(
            "INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r','head-tip','head',1,'queued',1,1);
             INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r','files-tip','files',1,'queued',1,1);
             INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,published_artifact_id,observed_at) VALUES('ws','o/r','main','head','head-tip',1,NULL,10);
             INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,published_artifact_id,observed_at) VALUES('ws','o/r','main','files','files-tip',2,NULL,10);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        let error = ArtifactScheduler::open(&p, Default::default())
            .await
            .err()
            .expect("conflicting latest commits must fail migration");
        assert!(
            error
                .to_string()
                .contains("conflicting latest observations")
        );
    }

    #[tokio::test]
    async fn published_defense_rejects_queued_failed_and_empty_evidence() {
        let (s, _d, _) = scheduler(Default::default()).await;
        s.observe("ws", "o/r", "main", "t", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        let c = s.claim("w", 5).await.unwrap().unwrap();
        s.complete(&c, "w", &evidence(&c)).await.unwrap();
        assert!(
            s.published("ws", "o/r", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .is_some()
        );
        for (state, manifest) in [
            ("queued", "manifest-hash"),
            ("failed", "manifest-hash"),
            ("ready", "  "),
        ] {
            sqlx::query("UPDATE artifact_jobs SET state=?,manifest=? WHERE id=?")
                .bind(state)
                .bind(manifest)
                .bind(c.record.id)
                .execute(&s.pool)
                .await
                .unwrap();
            assert!(
                s.published("ws", "o/r", "main", ArtifactKind::Head, 1)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn future_schema_workspace_zero_and_empty_verifier_fail_closed() {
        assert!(
            ArtifactScheduler::open(
                "sqlite::memory:",
                SchedulerLimits {
                    workspace_running: 0,
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        struct Empty;
        impl CompletionVerifier for Empty {
            fn identity(&self) -> &str {
                "  "
            }
            fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
                Ok(())
            }
        }
        assert!(
            ArtifactScheduler::open_with_verifier(
                "sqlite::memory:",
                Default::default(),
                Arc::new(Empty)
            )
            .await
            .is_err()
        );
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("future.db").to_string_lossy().to_string();
        let opts = SqliteConnectOptions::from_str(&p)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
        sqlx::raw_sql("PRAGMA user_version=99")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        assert!(
            ArtifactScheduler::open(&p, Default::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn zero_format_and_blank_fingerprint_over_state_fail_closed() {
        let (s, _d, path) = scheduler(Default::default()).await;
        let mut invalid = key("ws", "zero", ArtifactKind::Head);
        invalid.format_version = 0;
        assert!(s.schedule(&invalid).await.is_err());
        assert!(
            s.observe("ws", "o/r", "zero", "zero", &[ArtifactKind::Head], 0, None)
                .await
                .is_err()
        );
        s.schedule(&key("ws", "existing", ArtifactKind::Head))
            .await
            .unwrap();
        sqlx::query("UPDATE scheduler_state SET config_fingerprint='' WHERE id=1")
            .execute(&s.pool)
            .await
            .unwrap();
        assert!(
            ArtifactScheduler::open(&path, Default::default())
                .await
                .is_err(),
            "sqlite adopted an empty fleet fingerprint over existing state"
        );

        let (ready, _ready_dir, ready_path) = scheduler(Default::default()).await;
        ready
            .observe("ws", "o/r", "main", "ready", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        let claim = ready.claim("worker", 5).await.unwrap().unwrap();
        ready
            .complete(&claim, "worker", &evidence(&claim))
            .await
            .unwrap();
        sqlx::query(
            "UPDATE scheduler_state SET config_fingerprint='__legacy_migration_pending__' WHERE id=1",
        )
        .execute(&ready.pool)
        .await
        .unwrap();
        assert!(
            ArtifactScheduler::open(&ready_path, Default::default())
                .await
                .is_err(),
            "sqlite adopted a planted migration marker over ready/published state"
        );
    }

    #[tokio::test]
    async fn transport_root_lease_is_session_repo_fenced_and_expires() {
        let (s, _d, path) = scheduler(SchedulerLimits::default()).await;
        let root = "a".repeat(64);
        let session = "b".repeat(64);
        s.register_transport_root(&root, &session, "ws", "o/r", 60)
            .await
            .unwrap();
        assert_eq!(
            s.live_transport_roots_page(None, 10).await.unwrap().len(),
            1
        );
        assert!(
            s.register_transport_root(&root, &session, "ws", "other/r", 60)
                .await
                .is_err()
        );
        let second_root = "c".repeat(64);
        s.register_transport_root(&second_root, &session, "ws", "o/r", 60)
            .await
            .unwrap();
        assert!(
            !s.renew_transport_root(&root, &session, "ws", "other/r", 60)
                .await
                .unwrap()
        );
        let pool = sqlx::SqlitePool::connect(&path).await.unwrap();
        sqlx::query("UPDATE artifact_transport_leases SET expires_at=unixepoch()-1")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            !s.renew_transport_root(&root, &session, "ws", "o/r", 60)
                .await
                .unwrap()
        );
        s.reconcile_expired().await.unwrap();
        assert!(
            s.live_transport_roots_page(None, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn gc_sweep_fences_every_normalized_root_publication_path_and_expires() {
        let (s, _d, _path) = scheduler(SchedulerLimits::default()).await;
        let pending = key("ws", "pending", ArtifactKind::Head);
        s.schedule(&pending).await.unwrap();
        let claim = s.claim("worker", 60).await.unwrap().unwrap();

        assert!(s.acquire_gc_sweep("collector-a", 60).await.unwrap());
        assert!(!s.acquire_gc_sweep("collector-b", 60).await.unwrap());
        assert!(!s.renew_gc_sweep("collector-b", 60).await.unwrap());
        assert!(
            s.register_transport_root(&"a".repeat(64), &"b".repeat(64), "ws", "o/r", 60)
                .await
                .is_err()
        );
        assert!(
            s.subscribe_consumer(&key("ws", "consumer", ArtifactKind::Files), "clone", 60)
                .await
                .is_err()
        );
        assert!(
            s.observe("ws", "o/r", "main", "tip", &[ArtifactKind::Head], 1, None)
                .await
                .is_err()
        );
        assert!(
            s.complete(&claim, "worker", &evidence(&claim))
                .await
                .is_err()
        );

        s.release_gc_sweep("collector-b").await.unwrap();
        assert!(!s.acquire_gc_sweep("collector-b", 60).await.unwrap());
        sqlx::query("UPDATE artifact_gc_sweep SET expires_at=unixepoch()-1")
            .execute(&s.pool)
            .await
            .unwrap();
        assert!(s.acquire_gc_sweep("collector-b", 60).await.unwrap());
        assert!(!s.renew_gc_sweep("collector-a", 60).await.unwrap());
        s.release_gc_sweep("collector-b").await.unwrap();
        assert!(
            s.complete(&claim, "worker", &evidence(&claim))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn divergent_lineages_migrate_to_v5_while_partial_shapes_fail_closed() {
        let (transport_v4, _dir, transport_v4_path) = scheduler(Default::default()).await;
        drop_source_v7(&transport_v4.pool).await;
        sqlx::raw_sql("DROP TABLE ready_publication_fence_members; DROP TABLE ready_publication_fences; DROP TABLE ready_publication_fence_sequence; PRAGMA user_version=4")
            .execute(&transport_v4.pool).await.unwrap();
        drop(transport_v4);
        let migrated = ArtifactScheduler::open(&transport_v4_path, Default::default())
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                .fetch_one(&migrated.pool)
                .await
                .unwrap(),
            7
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pragma_table_info('artifact_gc_sweep')"
            )
            .fetch_one(&migrated.pool)
            .await
            .unwrap(),
            3
        );

        let (combined_v4, _dir, combined_v4_path) = scheduler(Default::default()).await;
        drop_source_v7(&combined_v4.pool).await;
        sqlx::query("PRAGMA user_version=4")
            .execute(&combined_v4.pool)
            .await
            .unwrap();
        drop(combined_v4);
        let migrated_combined = ArtifactScheduler::open(&combined_v4_path, Default::default())
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                .fetch_one(&migrated_combined.pool)
                .await
                .unwrap(),
            7
        );

        let (admission_v3, _dir, admission_v3_path) = scheduler(Default::default()).await;
        drop_source_v7(&admission_v3.pool).await;
        sqlx::raw_sql("DROP TABLE artifact_base_retention; DROP TABLE artifact_gc_sweep; DROP TABLE artifact_transport_leases; PRAGMA user_version=3")
            .execute(&admission_v3.pool).await.unwrap();
        drop(admission_v3);
        let migrated_admission = ArtifactScheduler::open(&admission_v3_path, Default::default())
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                .fetch_one(&migrated_admission.pool)
                .await
                .unwrap(),
            7
        );

        let (v2, _dir, v2_path) = scheduler(Default::default()).await;
        drop_source_v7(&v2.pool).await;
        sqlx::raw_sql("DROP TABLE ready_publication_fence_members; DROP TABLE ready_publication_fences; DROP TABLE ready_publication_fence_sequence; DROP TABLE artifact_base_retention; DROP TABLE artifact_gc_sweep; PRAGMA user_version=2")
            .execute(&v2.pool).await.unwrap();
        drop(v2);
        let migrated_v2 = ArtifactScheduler::open(&v2_path, Default::default())
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                .fetch_one(&migrated_v2.pool)
                .await
                .unwrap(),
            7
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN('artifact_base_retention','artifact_gc_sweep')").fetch_one(&migrated_v2.pool).await.unwrap(),2);

        let (partial, _dir, partial_path) = scheduler(Default::default()).await;
        sqlx::query("DROP TABLE artifact_gc_sweep")
            .execute(&partial.pool)
            .await
            .unwrap();
        drop(partial);
        assert!(
            ArtifactScheduler::open(&partial_path, Default::default())
                .await
                .is_err()
        );

        let (missing_base, _dir, missing_base_path) = scheduler(Default::default()).await;
        sqlx::raw_sql("DROP TABLE artifact_base_retention; PRAGMA user_version=3")
            .execute(&missing_base.pool)
            .await
            .unwrap();
        drop(missing_base);
        assert!(
            ArtifactScheduler::open(&missing_base_path, Default::default())
                .await
                .is_err()
        );

        let (missing_index, _dir, missing_index_path) = scheduler(Default::default()).await;
        sqlx::raw_sql("DROP INDEX artifact_base_retention_repo; PRAGMA user_version=3")
            .execute(&missing_index.pool)
            .await
            .unwrap();
        drop(missing_index);
        assert!(
            ArtifactScheduler::open(&missing_index_path, Default::default())
                .await
                .is_err()
        );

        let (wrong_constraint, _dir, wrong_constraint_path) = scheduler(Default::default()).await;
        sqlx::raw_sql("DROP TABLE artifact_base_retention; CREATE TABLE artifact_base_retention(artifact_id INTEGER PRIMARY KEY,workspace TEXT NOT NULL,repo TEXT NOT NULL,format_version INTEGER NOT NULL,head_rank INTEGER,pair_rank INTEGER); CREATE INDEX artifact_base_retention_repo ON artifact_base_retention(workspace,repo,format_version,artifact_id); PRAGMA user_version=3").execute(&wrong_constraint.pool).await.unwrap();
        drop(wrong_constraint);
        assert!(
            ArtifactScheduler::open(&wrong_constraint_path, Default::default())
                .await
                .is_err()
        );

        let (future, _dir, future_path) = scheduler(Default::default()).await;
        sqlx::query("PRAGMA user_version=8")
            .execute(&future.pool)
            .await
            .unwrap();
        drop(future);
        assert!(
            ArtifactScheduler::open(&future_path, Default::default())
                .await
                .is_err()
        );

        let (concurrent, _dir, concurrent_path) = scheduler(Default::default()).await;
        drop_source_v7(&concurrent.pool).await;
        sqlx::query("PRAGMA user_version=6")
            .execute(&concurrent.pool)
            .await
            .unwrap();
        drop(concurrent);
        let (first, second) = tokio::join!(
            ArtifactScheduler::open(&concurrent_path, Default::default()),
            ArtifactScheduler::open(&concurrent_path, Default::default())
        );
        assert!(first.is_ok() && second.is_ok());
    }

    #[tokio::test]
    async fn v2_hybrids_and_forged_fences_fail_before_mutation_and_release_writer() {
        async fn rejected_without_mutation(path: &str, pool: &SqlitePool) {
            assert!(
                ArtifactScheduler::open(path, Default::default())
                    .await
                    .is_err()
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                    .fetch_one(pool)
                    .await
                    .unwrap(),
                2
            );
            tokio::time::timeout(
                Duration::from_secs(1),
                sqlx::raw_sql("BEGIN IMMEDIATE; ROLLBACK").execute(pool),
            )
            .await
            .expect("rejected migration retained sqlite writer lock")
            .unwrap();
        }

        // Unreleased admission-shaped base without its fence protocol.
        let (no_fences, _dir, no_fences_path) = scheduler(Default::default()).await;
        sqlx::raw_sql("DROP TABLE ready_publication_fence_members; DROP TABLE ready_publication_fences; DROP TABLE ready_publication_fence_sequence; DROP TABLE artifact_base_retention; DROP TABLE artifact_gc_sweep; DROP TABLE artifact_transport_leases; PRAGMA user_version=2")
            .execute(&no_fences.pool).await.unwrap();
        rejected_without_mutation(&no_fences_path, &no_fences.pool).await;

        // Unreleased transport-shaped base contaminated with admission fences.
        let (transport_with_fences, _dir, transport_with_fences_path) =
            scheduler(Default::default()).await;
        sqlx::raw_sql("DROP TABLE artifact_base_retention; DROP TABLE artifact_gc_sweep; PRAGMA user_version=2")
            .execute(&transport_with_fences.pool).await.unwrap();
        rejected_without_mutation(&transport_with_fences_path, &transport_with_fences.pool).await;

        // Exact admission inventory with a column-compatible but unreleased
        // fence DDL (manifest was never NOT NULL in the released v2 schema).
        let (forged, _dir, forged_path) = scheduler(Default::default()).await;
        sqlx::raw_sql(
            "DROP TABLE ready_publication_fence_members;
             DROP TABLE ready_publication_fences;
             DROP TABLE ready_publication_fence_sequence;
             DROP TABLE artifact_base_retention;
             DROP TABLE artifact_gc_sweep;
             DROP TABLE artifact_transport_leases;
             CREATE TABLE ready_publication_fence_sequence(id INTEGER PRIMARY KEY CHECK(id=1),generation INTEGER NOT NULL CHECK(generation>=0));
             INSERT INTO ready_publication_fence_sequence(id,generation) VALUES(1,0);
             CREATE TABLE ready_publication_fences(token TEXT PRIMARY KEY,generation INTEGER NOT NULL UNIQUE CHECK(generation>0),operation_id TEXT NOT NULL UNIQUE,expires_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('held','activation_unknown')),UNIQUE(token,generation));
             CREATE TABLE ready_publication_fence_members(token TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),artifact_id INTEGER NOT NULL,manifest TEXT NOT NULL,PRIMARY KEY(token,artifact_id),FOREIGN KEY(token,generation) REFERENCES ready_publication_fences(token,generation) ON DELETE CASCADE,FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE);
             PRAGMA user_version=2",
        )
        .execute(&forged.pool)
        .await
        .unwrap();
        rejected_without_mutation(&forged_path, &forged.pool).await;
    }

    #[tokio::test]
    async fn transactional_delete_fence_blocks_publication_past_lease_expiry() {
        let (scheduler, _d, _path) = scheduler(SchedulerLimits::default()).await;
        let scheduler = Arc::new(scheduler);
        let completing = key("ws", "complete", ArtifactKind::Head);
        scheduler.schedule(&completing).await.unwrap();
        let claim = scheduler.claim("worker", 60).await.unwrap().unwrap();
        assert!(scheduler.acquire_gc_sweep("collector", 1).await.unwrap());
        let fence = scheduler.lock_gc_delete_batch("collector").await.unwrap();
        let registration = {
            let scheduler = scheduler.clone();
            tokio::spawn(async move {
                scheduler
                    .register_transport_root(&"a".repeat(64), &"b".repeat(64), "ws", "o/r", 60)
                    .await
            })
        };
        let subscription = {
            let scheduler = scheduler.clone();
            tokio::spawn(async move {
                scheduler
                    .subscribe_consumer(&key("ws", "subscribe", ArtifactKind::Files), "clone", 60)
                    .await
            })
        };
        let completion = {
            let scheduler = scheduler.clone();
            tokio::spawn(async move {
                scheduler
                    .complete(&claim, "worker", &evidence(&claim))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            !registration.is_finished() && !subscription.is_finished() && !completion.is_finished(),
            "a publication path escaped while the external delete transaction was still live"
        );
        fence.release().await.unwrap();
        registration.await.unwrap().unwrap();
        subscription.await.unwrap().unwrap();
        assert!(completion.await.unwrap().unwrap());
        assert_eq!(
            scheduler
                .live_transport_roots_page(None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn dropped_gc_delete_fence_closes_transaction_and_does_not_lock_pool() {
        let (scheduler, _d, _path) = scheduler(SchedulerLimits::default()).await;
        assert!(scheduler.acquire_gc_sweep("collector", 60).await.unwrap());
        let fence = scheduler.lock_gc_delete_batch("collector").await.unwrap();
        drop(fence);
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            scheduler.lock_gc_delete_batch("collector"),
        )
        .await
        .expect("dropped fence retained SQLite write lock")
        .unwrap();
        replacement.release().await.unwrap();
        scheduler.release_gc_sweep("collector").await.unwrap();
    }

    #[tokio::test]
    async fn builder_consumer_roots_reused_base_through_publication_settlement() {
        let (s, _d, _path) = scheduler(SchedulerLimits::default()).await;
        let base = key("ws", "old-base", ArtifactKind::Files);
        s.subscribe_consumer(&base, "builder:output", 60)
            .await
            .unwrap();
        let base_claim = s.claim("worker", 60).await.unwrap().unwrap();
        s.complete(&base_claim, "worker", &evidence(&base_claim))
            .await
            .unwrap();

        let output = key("ws", "output", ArtifactKind::Head);
        s.schedule(&output).await.unwrap();
        let output_claim = s.claim("worker", 60).await.unwrap().unwrap();
        assert!(s.acquire_gc_sweep("collector", 60).await.unwrap());
        assert!(
            s.complete(&output_claim, "worker", &evidence(&output_claim))
                .await
                .is_err(),
            "output unexpectedly settled while GC held publication fence"
        );
        let roots = s.live_scheduler_roots_page(None, 20).await.unwrap();
        assert!(
            roots.iter().any(|root| root.key == base),
            "reused base disappeared in upload-to-completion gap"
        );
        s.release_gc_sweep("collector").await.unwrap();
        assert!(
            s.complete(&output_claim, "worker", &evidence(&output_claim))
                .await
                .unwrap()
        );
        s.release_consumer(base_claim.record.id, "builder:output")
            .await
            .unwrap();
        assert!(
            !s.live_scheduler_roots_page(None, 20)
                .await
                .unwrap()
                .iter()
                .any(|root| root.key == base)
        );
    }

    #[tokio::test]
    async fn transport_root_catalog_paginates_with_stable_composite_cursor() {
        let (s, _d, path) = scheduler(SchedulerLimits::default()).await;
        let pool = sqlx::SqlitePool::connect(&path).await.unwrap();
        for value in 0..=crate::artifact_scheduler_backend::TRANSPORT_ROOT_PAGE_MAX {
            let root = format!("{value:064x}");
            let session = format!("{:064x}", value + 10_000);
            sqlx::query("INSERT INTO artifact_transport_leases(root_hash,session_id,workspace,repo,expires_at) VALUES(?,?,?,?,unixepoch()+60)")
                .bind(root).bind(session).bind("ws").bind("o/r").execute(&pool).await.unwrap();
        }
        let first = s
            .live_transport_roots_page(
                None,
                crate::artifact_scheduler_backend::TRANSPORT_ROOT_PAGE_MAX,
            )
            .await
            .unwrap();
        assert_eq!(
            first.len(),
            crate::artifact_scheduler_backend::TRANSPORT_ROOT_PAGE_MAX as usize
        );
        let last = first.last().unwrap();
        let second = s
            .live_transport_roots_page(
                Some((&last.root_hash, &last.session_id)),
                crate::artifact_scheduler_backend::TRANSPORT_ROOT_PAGE_MAX,
            )
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
    }

    #[tokio::test]
    async fn concurrent_transport_renew_cannot_resurrect_release() {
        let (scheduler, _d, _path) = scheduler(SchedulerLimits::default()).await;
        let scheduler = Arc::new(scheduler);
        let root = "d".repeat(64);
        let session = "e".repeat(64);
        scheduler
            .register_transport_root(&root, &session, "ws", "o/r", 60)
            .await
            .unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let renew = {
            let scheduler = scheduler.clone();
            let barrier = barrier.clone();
            let root = root.clone();
            let session = session.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                scheduler
                    .renew_transport_root(&root, &session, "ws", "o/r", 60)
                    .await
                    .unwrap()
            })
        };
        let release = {
            let scheduler = scheduler.clone();
            let barrier = barrier.clone();
            let root = root.clone();
            let session = session.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                scheduler
                    .release_transport_root(&root, &session, "ws", "o/r")
                    .await
                    .unwrap()
            })
        };
        barrier.wait().await;
        let _ = renew.await.unwrap();
        assert!(release.await.unwrap());
        assert!(
            scheduler
                .live_transport_roots_page(None, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn scheduler_gc_roots_cover_published_or_live_consumed_only() {
        let (s, _d, _path) = scheduler(SchedulerLimits::default()).await;
        let published_key = key("ws", "published", ArtifactKind::Head);
        s.observe(
            "ws",
            "o/r",
            "main",
            "published",
            &[ArtifactKind::Head],
            1,
            None,
        )
        .await
        .unwrap();
        let claim = s.claim("worker", 60).await.unwrap().unwrap();
        s.complete(
            &claim,
            "worker",
            &CompletionEvidence::new(published_key.clone(), "a".repeat(64)).unwrap(),
        )
        .await
        .unwrap();

        let consumed = key("ws", "consumed", ArtifactKind::Files);
        s.subscribe_consumer(&consumed, "admission", 60)
            .await
            .unwrap();
        let claim = s.claim("worker", 60).await.unwrap().unwrap();
        s.complete(
            &claim,
            "worker",
            &CompletionEvidence::new(consumed.clone(), "b".repeat(64)).unwrap(),
        )
        .await
        .unwrap();
        let roots = s.live_scheduler_roots_page(None, 10).await.unwrap();
        assert_eq!(roots.len(), 2);

        let consumed_record = s.get_by_key(&consumed).await.unwrap().unwrap();
        sqlx::query("UPDATE artifact_consumers SET expires_at=unixepoch()-1 WHERE artifact_id=?")
            .bind(consumed_record.id)
            .execute(&s.pool)
            .await
            .unwrap();
        let roots = s.live_scheduler_roots_page(None, 10).await.unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].key, published_key);

        s.observe(
            "ws",
            "o/r",
            "main",
            "replacement",
            &[ArtifactKind::Head],
            1,
            None,
        )
        .await
        .unwrap();
        let roots = s.live_scheduler_roots_page(None, 10).await.unwrap();
        assert_eq!(
            roots.len(),
            1,
            "newest retained Head base was lost during alias advance"
        );
    }

    #[tokio::test]
    async fn base_retention_survives_head_first_pair_advance() {
        let (s, _d, _path) = scheduler(Default::default()).await;
        let keys = [
            key("ws", "old", ArtifactKind::Head),
            key("ws", "old", ArtifactKind::FullHistory),
        ];
        for k in &keys {
            s.subscribe_consumer(k, "seed", 60).await.unwrap();
        }
        for manifest in ["a".repeat(64), "b".repeat(64)] {
            let c = s.claim("worker", 60).await.unwrap().unwrap();
            s.complete(
                &c,
                "worker",
                &CompletionEvidence::new(c.record.key.clone(), manifest).unwrap(),
            )
            .await
            .unwrap();
        }
        for k in &keys {
            let r = s.get_by_key(k).await.unwrap().unwrap();
            s.release_consumer(r.id, "seed").await.unwrap();
        }
        assert_eq!(
            s.live_scheduler_roots_page(None, 20).await.unwrap().len(),
            2
        );

        let new_head = key("ws", "new", ArtifactKind::Head);
        s.subscribe_consumer(&new_head, "seed", 60).await.unwrap();
        let c = s.claim("worker", 60).await.unwrap().unwrap();
        s.complete(
            &c,
            "worker",
            &CompletionEvidence::new(c.record.key.clone(), "c".repeat(64)).unwrap(),
        )
        .await
        .unwrap();
        let r = s.get_by_key(&new_head).await.unwrap().unwrap();
        s.release_consumer(r.id, "seed").await.unwrap();
        assert_eq!(
            s.live_scheduler_roots_page(None, 20).await.unwrap().len(),
            3,
            "older complete pair was broken by Head-first advance"
        );
        let new_history = key("ws", "new", ArtifactKind::FullHistory);
        s.subscribe_consumer(&new_history, "seed", 60)
            .await
            .unwrap();
        let c = s.claim("worker", 60).await.unwrap().unwrap();
        s.complete(
            &c,
            "worker",
            &CompletionEvidence::new(c.record.key.clone(), "d".repeat(64)).unwrap(),
        )
        .await
        .unwrap();
        let r = s.get_by_key(&new_history).await.unwrap().unwrap();
        s.release_consumer(r.id, "seed").await.unwrap();
        assert_eq!(
            s.live_scheduler_roots_page(None, 20).await.unwrap().len(),
            4
        );
    }

    #[tokio::test]
    async fn base_retention_survives_history_first_pair_advance() {
        let (s, _d, _path) = scheduler(Default::default()).await;
        for kind in [ArtifactKind::Head, ArtifactKind::FullHistory] {
            let k = key("ws", "old", kind);
            s.subscribe_consumer(&k, "seed", 60).await.unwrap();
        }
        for manifest in ["a".repeat(64), "b".repeat(64)] {
            let c = s.claim("worker", 60).await.unwrap().unwrap();
            s.complete(
                &c,
                "worker",
                &CompletionEvidence::new(c.record.key.clone(), manifest).unwrap(),
            )
            .await
            .unwrap();
            s.release_consumer(c.record.id, "seed").await.unwrap();
        }
        let history = key("ws", "new", ArtifactKind::FullHistory);
        s.observe(
            "ws",
            "o/r",
            "main",
            "new",
            &[ArtifactKind::FullHistory],
            1,
            None,
        )
        .await
        .unwrap();
        s.subscribe_consumer(&history, "seed", 60).await.unwrap();
        let c = s.claim("worker", 60).await.unwrap().unwrap();
        s.complete(
            &c,
            "worker",
            &CompletionEvidence::new(c.record.key.clone(), "c".repeat(64)).unwrap(),
        )
        .await
        .unwrap();
        s.release_consumer(c.record.id, "seed").await.unwrap();
        assert_eq!(
            s.live_scheduler_roots_page(None, 20).await.unwrap().len(),
            3,
            "older complete pair was broken by History-first advance"
        );
    }

    #[tokio::test]
    async fn quarantined_newest_complete_base_falls_back_to_retained_older_pair() {
        let (s, _d, _path) = scheduler(Default::default()).await;
        for (commit, head_manifest, history_manifest) in [
            ("old", "a".repeat(64), "b".repeat(64)),
            ("new", "c".repeat(64), "d".repeat(64)),
        ] {
            for (kind, manifest) in [
                (ArtifactKind::Head, head_manifest),
                (ArtifactKind::FullHistory, history_manifest),
            ] {
                let k = key("ws", commit, kind);
                s.subscribe_consumer(&k, "seed", 60).await.unwrap();
                let c = s.claim("worker", 60).await.unwrap().unwrap();
                s.complete(
                    &c,
                    "worker",
                    &CompletionEvidence::new(c.record.key.clone(), manifest).unwrap(),
                )
                .await
                .unwrap();
                s.release_consumer(c.record.id, "seed").await.unwrap();
            }
        }
        assert_eq!(
            s.complete_full_base_candidates("ws", "o/r", 1, 8)
                .await
                .unwrap(),
            vec!["new", "old"]
        );
        assert!(
            s.quarantine_publication(
                &key("ws", "new", ArtifactKind::Head),
                &"c".repeat(64),
                "corrupt"
            )
            .await
            .unwrap()
        );
        assert_eq!(
            s.complete_full_base_candidates("ws", "o/r", 1, 8)
                .await
                .unwrap(),
            vec!["old"]
        );
        let roots = s.live_scheduler_roots_page(None, 20).await.unwrap();
        assert!(
            roots
                .iter()
                .any(|root| root.key.commit == "old" && root.key.kind == ArtifactKind::Head)
        );
        assert!(
            roots
                .iter()
                .any(|root| root.key.commit == "old" && root.key.kind == ArtifactKind::FullHistory)
        );
    }

    #[tokio::test]
    async fn base_retention_keeps_exact_newest_eight_pairs_with_tied_timestamps() {
        let (s, _d, _path) = scheduler(Default::default()).await;
        let head_times = [4_i64, 1, 4, 3, 2, 3, 1, 4, 2, 4, 3, 4];
        let history_times = [1_i64, 4, 2, 3, 4, 1, 3, 2, 4, 4, 3, 1];
        let mut pairs = Vec::new();
        for index in 0..12_i64 {
            let head_id = 100 + index * 2;
            let history_id = head_id + 1;
            for (id, kind, updated) in [
                (head_id, "head", head_times[index as usize]),
                (history_id, "full_history", history_times[index as usize]),
            ] {
                sqlx::query("INSERT INTO artifact_jobs(id,workspace,repo,commit_oid,kind,format_version,state,lease_generation,claim_attempts,retry_count,manifest,created_at,updated_at) VALUES(?,'ws','o/r',?,?,1,'ready',0,0,0,?,0,?)")
                    .bind(id).bind(format!("commit-{index}")) .bind(kind)
                    .bind(format!("{id:064x}")).bind(updated).execute(&s.pool).await.unwrap();
            }
            pairs.push((
                head_times[index as usize].max(history_times[index as usize]),
                history_id,
                head_id,
                history_id,
            ));
        }
        let mut c = s.immediate().await.unwrap();
        refresh_base_retention_conn(&mut c, "ws", "o/r", 1)
            .await
            .unwrap();
        finish(c, Ok(())).await.unwrap();

        pairs.sort_by_key(|pair| std::cmp::Reverse((pair.0, pair.1)));
        let expected = pairs
            .iter()
            .take(8)
            .flat_map(|pair| [pair.2, pair.3])
            .collect::<std::collections::HashSet<_>>();
        let retained = sqlx::query_scalar::<_, i64>(
            "SELECT artifact_id FROM artifact_base_retention WHERE pair_rank IS NOT NULL",
        )
        .fetch_all(&s.pool)
        .await
        .unwrap()
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
        assert_eq!(retained, expected);
        assert_eq!(retained.len(), 16);
    }

    #[tokio::test]
    async fn scheduler_gc_page_query_uses_materialized_indexes_not_fleet_reranking() {
        let (s, _d, _path) = scheduler(Default::default()).await;
        for id in [1_i64, 1_000_000_i64] {
            sqlx::query("INSERT INTO artifact_jobs(id,workspace,repo,commit_oid,kind,format_version,state,lease_generation,claim_attempts,retry_count,manifest,created_at,updated_at) VALUES(?, 'ws','o/r',?,'files',1,'ready',0,0,0,?,0,0)")
                .bind(id).bind(format!("commit-{id}")).bind(format!("{id:064x}")).execute(&s.pool).await.unwrap();
            sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,'live',unixepoch()+60)")
                .bind(id).execute(&s.pool).await.unwrap();
        }
        let page = s.live_scheduler_roots_page(Some(1), 1).await.unwrap();
        assert_eq!(
            page[0].artifact_id, 1_000_000,
            "sparse rooted id was not reached"
        );
        let rows=sqlx::query("EXPLAIN QUERY PLAN WITH candidates(id) AS (SELECT published_artifact_id FROM (SELECT published_artifact_id FROM artifact_observations WHERE published_artifact_id>? ORDER BY published_artifact_id LIMIT ?) UNION ALL SELECT artifact_id FROM (SELECT artifact_id FROM artifact_consumers WHERE artifact_id>? AND expires_at>? ORDER BY artifact_id LIMIT ?) UNION ALL SELECT artifact_id FROM (SELECT artifact_id FROM artifact_base_retention WHERE artifact_id>? ORDER BY artifact_id LIMIT ?)), page_ids(id) AS (SELECT DISTINCT id FROM candidates ORDER BY id LIMIT ?) SELECT j.id FROM page_ids p JOIN artifact_jobs j ON j.id=p.id WHERE j.state='ready' AND j.manifest IS NOT NULL AND length(trim(j.manifest))>0 ORDER BY j.id")
            .bind(0_i64).bind(512_i64).bind(0_i64).bind(0_i64).bind(512_i64).bind(0_i64).bind(512_i64).bind(512_i64).fetch_all(&s.pool).await.unwrap();
        let plan = rows
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("artifact_observations_published")
                && plan.contains("sqlite_autoindex_artifact_consumers_1")
                && plan.contains("INTEGER PRIMARY KEY (rowid>?)"),
            "GC root sources were not independent indexed cursor seeks: {plan}"
        );
        assert!(
            !plan.contains("SCAN j"),
            "GC page scanned sparse artifact_jobs fleet state: {plan}"
        );
    }

    #[tokio::test]
    async fn scheduler_gc_pagination_survives_more_than_page_of_duplicate_root_sources() {
        let (s, _d, _path) = scheduler(Default::default()).await;
        let mut tx = s.pool.begin().await.unwrap();
        for id in 1_i64..=600 {
            sqlx::query("INSERT INTO artifact_jobs(id,workspace,repo,commit_oid,kind,format_version,state,lease_generation,claim_attempts,retry_count,manifest,created_at,updated_at) VALUES(?,'ws','o/r',?,'files',1,'ready',0,0,0,?,0,?)")
                .bind(id).bind(format!("commit-{id}")).bind(format!("{id:064x}"))
                .bind(id).execute(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,'live',unixepoch()+60)")
                .bind(id).execute(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES('ws','o/r',?,'files',?,?,1,?,1,0)")
                .bind(format!("branch-{id}")).bind(format!("commit-{id}"))
                .bind(id).bind(id).execute(&mut *tx).await.unwrap();
        }
        tx.commit().await.unwrap();
        let first = s.live_scheduler_roots_page(None, 512).await.unwrap();
        assert_eq!(first.len(), 512);
        let second = s
            .live_scheduler_roots_page(first.last().map(|root| root.artifact_id), 512)
            .await
            .unwrap();
        assert_eq!(second.len(), 88);
        assert_eq!(second.first().unwrap().artifact_id, 513);
        assert_eq!(second.last().unwrap().artifact_id, 600);
    }
}
