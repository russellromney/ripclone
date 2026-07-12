//! Durable, commit-addressed scheduling for independently publishable artifacts.
//!
//! SQLite is both the local and cross-process implementation: all admission,
//! observation, lease, retry, fairness, and publication decisions are fenced by
//! transactions in this database. Builders may only publish through a live
//! [`ClaimedArtifact`] and typed [`CompletionEvidence`].

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
    artifact_ids: Vec<i64>,
    consumer_id: String,
}

impl ReadyPublicationFence {
    pub(crate) fn new(artifact_ids: Vec<i64>, consumer_id: String) -> Self {
        Self {
            artifact_ids,
            consumer_id,
        }
    }
    pub(crate) fn parts(&self) -> (&[i64], &str) {
        (&self.artifact_ids, &self.consumer_id)
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
CREATE TABLE IF NOT EXISTS branch_observations(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation INTEGER NOT NULL,
 desired_commit TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch));
CREATE TABLE IF NOT EXISTS artifact_observations(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,kind TEXT NOT NULL,
 desired_commit TEXT NOT NULL,desired_artifact_id INTEGER NOT NULL,desired_generation INTEGER NOT NULL,
 published_artifact_id INTEGER,format_version INTEGER NOT NULL DEFAULT 1,observed_at INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(workspace,repo,branch,kind));
CREATE TABLE IF NOT EXISTS artifact_consumers(artifact_id INTEGER NOT NULL,consumer_id TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(artifact_id,consumer_id));
CREATE TABLE IF NOT EXISTS scheduler_state(id INTEGER PRIMARY KEY CHECK(id=1),fairness_cursor INTEGER NOT NULL,workspace_cursor TEXT NOT NULL DEFAULT '',config_fingerprint TEXT NOT NULL DEFAULT '');
INSERT OR IGNORE INTO scheduler_state(id,fairness_cursor) VALUES(1,0);
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
            .busy_timeout(Duration::from_secs(10))
            .journal_mode(SqliteJournalMode::Wal);
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
        let prior_version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        if prior_version > 2 {
            bail!("artifact scheduler database is newer than this binary")
        }
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .context("initialize artifact scheduler")?;
        let mut migration = pool.begin().await?;
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
        sqlx::raw_sql("PRAGMA user_version=2")
            .execute(&mut *migration)
            .await?;
        migration.commit().await?;
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        let required:i64=sqlx::query_scalar("SELECT count(*) FROM pragma_table_info('artifact_jobs') WHERE name IN('lease_generation','claim_attempts','retry_count','failure_class')").fetch_one(&pool).await?;
        if version != 2 || required != 4 {
            bail!("artifact scheduler migration post-validation failed")
        }
        let mut config = pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *config).await?;
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
            let _ = sqlx::query("ROLLBACK").execute(&mut *config).await;
            bail!("scheduler running-limit configuration differs from existing fleet")
        }
        let stored: String =
            sqlx::query_scalar("SELECT config_fingerprint FROM scheduler_state WHERE id=1")
                .fetch_one(&mut *config)
                .await?;
        if stored != fingerprint {
            let _ = sqlx::query("ROLLBACK").execute(&mut *config).await;
            bail!("scheduler configuration CAS verification failed")
        }
        sqlx::query("COMMIT").execute(&mut *config).await?;
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

    pub async fn subscribe_consumer(
        &self,
        key: &ArtifactKey,
        consumer_id: &str,
        ttl_secs: i64,
    ) -> Result<ScheduleOutcome> {
        validate_format_version(key.format_version)?;
        if consumer_id.trim().is_empty() {
            bail!("artifact consumer id is empty")
        }
        if consumer_id.starts_with("admission-activation-") {
            bail!("artifact consumer id uses a reserved activation-fence namespace")
        }
        if !(2..=86400).contains(&ttl_secs) {
            bail!("consumer subscription TTL is invalid")
        }
        let mut c = self.immediate().await?;
        let result: Result<ScheduleOutcome> = async {
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
        if consumer_id.starts_with("admission-activation-") {
            bail!("activation fences require their opaque release capability")
        }
        let mut c = self.immediate().await?;
        let result:Result<()>=async{
            sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?").bind(artifact_id).bind(consumer_id).execute(&mut *c).await?;
            sqlx::query("DELETE FROM artifact_jobs WHERE id=? AND state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)").bind(artifact_id).execute(&mut *c).await?;Ok(())
        }.await;
        finish(c, result).await
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
                "SELECT count(*) FROM artifact_consumers
                 WHERE artifact_id=? AND consumer_id LIKE 'admission-activation-%' AND expires_at>?",
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
        consumer_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<ReadyPublicationFence>> {
        if expected.len() != 2
            || expected[0].0 == expected[1].0
            || consumer_id.trim().is_empty()
            || consumer_id.len() > 255
            || !(1..=3600).contains(&ttl_secs)
        {
            bail!("invalid Ready publication fence")
        }
        if !consumer_id.starts_with("admission-activation-") {
            bail!("Ready publication fence consumer has invalid namespace")
        }
        let mut c = self.immediate().await?;
        let result: Result<Option<ReadyPublicationFence>> = async {
            for (id, manifest) in expected {
                let current: Option<(String, Option<String>)> = sqlx::query_as(
                    "SELECT state,manifest FROM artifact_jobs WHERE id=?",
                )
                .bind(id)
                .fetch_optional(&mut *c)
                .await?;
                if !matches!(current, Some((state, current_manifest)) if state == "ready" && current_manifest == *manifest)
                {
                    return Ok(None);
                }
            }
            let expires_at = db_now(&mut c).await?.saturating_add(ttl_secs);
            for (id, _) in expected {
                sqlx::query(
                    "INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at)
                     VALUES(?,?,?) ON CONFLICT(artifact_id,consumer_id)
                     DO UPDATE SET expires_at=excluded.expires_at",
                )
                .bind(id)
                .bind(consumer_id)
                .bind(expires_at)
                .execute(&mut *c)
                .await?;
            }
            Ok(Some(ReadyPublicationFence::new(
                expected.iter().map(|(id, _)| *id).collect(),
                consumer_id.to_owned(),
            )))
        }
        .await;
        finish(c, result).await
    }

    pub async fn release_ready_publication_fence(
        &self,
        fence: ReadyPublicationFence,
    ) -> Result<()> {
        let (ids, consumer_id) = fence.parts();
        let mut c = self.immediate().await?;
        let result: Result<()> = async {
            for id in ids {
                sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?")
                    .bind(id)
                    .bind(consumer_id)
                    .execute(&mut *c)
                    .await?;
            }
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
   let now=db_now(&mut c).await?; let won=sqlx::query("UPDATE artifact_jobs SET state='ready',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,manifest=?,error=NULL,failure_class=NULL,updated_at=? WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?")
    .bind(evidence.manifest()).bind(now).bind(claim.record.id).bind(owner).bind(claim.record.lease_generation as i64).bind(now).execute(&mut *c).await?.rows_affected()==1;
   if won{sqlx::query("UPDATE artifact_observations SET published_artifact_id=? WHERE desired_artifact_id=?").bind(claim.record.id).bind(claim.record.id).execute(&mut *c).await?;} Ok(won)
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
        c: &mut PoolConnection<Sqlite>,
        now: i64,
    ) -> Result<(u64, u64)> {
        sqlx::query("DELETE FROM artifact_consumers WHERE expires_at<=?")
            .bind(now)
            .execute(&mut **c)
            .await?;
        sqlx::query("DELETE FROM artifact_jobs WHERE state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)").execute(&mut **c).await?;
        let failed=sqlx::query("UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error='lease expired after attempt limit',failure_class='dead_letter',updated_at=? WHERE state='running' AND lease_expires_at<=? AND claim_attempts>=?").bind(now).bind(now).bind(self.limits.max_claim_attempts as i64).execute(&mut **c).await?.rows_affected();
        let queued=sqlx::query("UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error='lease expired; reclaimed',updated_at=? WHERE state='running' AND lease_expires_at<=? AND claim_attempts<?").bind(now).bind(now).bind(self.limits.max_claim_attempts as i64).execute(&mut **c).await?.rows_affected();
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
    pub async fn quarantine_ready(&self, id: i64, manifest: &str, reason: &str) -> Result<bool> {
        if id <= 0 || manifest.trim().is_empty() || reason.trim().is_empty() {
            bail!("invalid ready quarantine request");
        }
        let mut tx = self.pool.begin().await?;
        let changed = sqlx::query("UPDATE artifact_jobs SET state='queued',manifest=NULL,owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error=?,failure_class=NULL,updated_at=unixepoch() WHERE id=? AND state='ready' AND manifest=?")
            .bind(reason.chars().take(4096).collect::<String>())
            .bind(id)
            .bind(manifest)
            .execute(&mut *tx)
            .await?
            .rows_affected() == 1;
        if changed {
            sqlx::query("UPDATE artifact_observations SET published_artifact_id=NULL WHERE published_artifact_id=?")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(changed)
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

    async fn immediate(&self) -> Result<PoolConnection<Sqlite>> {
        let mut c = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *c).await?;
        Ok(c)
    }
    async fn schedule_in(
        &self,
        c: &mut PoolConnection<Sqlite>,
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
        c: &mut PoolConnection<Sqlite>,
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
        let res=sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at)VALUES(?,?,?,?,?,'queued',?,?)").bind(&key.workspace).bind(&key.repo).bind(&key.commit).bind(key.kind.as_str()).bind(key.format_version as i64).bind(now).bind(now).execute(&mut **c).await?;
        Ok(ScheduleOutcome::Enqueued(res.last_insert_rowid()))
    }
    async fn preflight_batch(
        &self,
        c: &mut PoolConnection<Sqlite>,
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
        .fetch_one(&mut **c)
        .await?;
        let workspace: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=?",
        )
        .bind(w)
        .fetch_one(&mut **c)
        .await?;
        let active_expensive: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind IN('full_history','files')",
        )
        .fetch_one(&mut **c)
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
        c: &mut PoolConnection<Sqlite>,
        kind: ArtifactKind,
        w: &str,
        add: usize,
    ) -> Result<()> {
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')",
        )
        .fetch_one(&mut **c)
        .await?;
        let workspace: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=?",
        )
        .bind(w)
        .fetch_one(&mut **c)
        .await?;
        let per: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind=?",
        )
        .bind(kind.as_str())
        .fetch_one(&mut **c)
        .await?;
        let active_expensive: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind IN('full_history','files')",
        )
        .fetch_one(&mut **c)
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
async fn get_conn(c: &mut PoolConnection<Sqlite>, id: i64) -> Result<Option<ArtifactRecord>> {
    let row=sqlx::query("SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE id=?").bind(id).fetch_optional(&mut **c).await?;
    row.map(row_record).transpose()
}
async fn get_key_conn(
    c: &mut PoolConnection<Sqlite>,
    k: &ArtifactKey,
) -> Result<Option<ArtifactRecord>> {
    let row = sqlx::query(SELECT)
        .bind(&k.workspace)
        .bind(&k.repo)
        .bind(&k.commit)
        .bind(k.kind.as_str())
        .bind(k.format_version as i64)
        .fetch_optional(&mut **c)
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
async fn db_now(c: &mut PoolConnection<Sqlite>) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT unixepoch()")
        .fetch_one(&mut **c)
        .await?)
}
async fn finish<T>(mut c: PoolConnection<Sqlite>, r: Result<T>) -> Result<T> {
    match r {
        Ok(v) => {
            sqlx::query("COMMIT").execute(&mut *c).await?;
            Ok(v)
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *c).await;
            Err(e)
        }
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
    if commit.len() != 40
        || !commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("resolved artifact commit is not a canonical Git object id")
    }
    Ok(())
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
            "1111111111111111111111111111111111111111111111111111111111111111",
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
}
