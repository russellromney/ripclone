//! SQLite authority for immutable Git source graphs.
//!
//! The ordering invariant is encoded in capabilities: local preparation is
//! followed by a short transaction that publishes the complete provisional
//! graph, and only that transaction can mint a publication permit. Durable
//! upload and exact verification precede registration.

use crate::artifact_manifest::CasBlob;
use crate::artifact_scheduler::{
    ArtifactKind, FailureClass, ObservationSnapshot, SchedulerLimits, scheduler_limits_fingerprint,
};
use crate::artifact_scheduler_backend::SOURCE_INTENT_CONSUMER_PREFIX;
use crate::git_source::{
    AuthenticatedGitSource, GitObjectFormat, GitSourceLimits, GitSourceLoader,
    GitSourceMaterializer, GitSourcePackager, GitSourceRegistryView, GitSourceUploader,
    MaterializedGitSource, PreparedGitSource,
};
use crate::storage::{StorageObjectStat, StorageRef};
use crate::sync_coordinator::{
    ArtifactIntentOutcome, ArtifactObservation, ArtifactObservationOutcome, DurableSourceSnapshot,
    SyncIntent,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, SqlitePool, pool::PoolConnection};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const SOURCE_INTENT_RETENTION_EXPIRY: i64 = i64::MAX;

#[derive(Debug)]
struct AmbiguousRegistrationAck(anyhow::Error);
impl std::fmt::Display for AmbiguousRegistrationAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "source registration commit acknowledgement is ambiguous: {}",
            self.0
        )
    }
}
impl std::error::Error for AmbiguousRegistrationAck {}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RegistrationCommitFault {
    None,
    AckLostAfterCommit,
    FailWithOpenTransaction,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TransactionCommitFault {
    None,
    FailWithOpenTransaction,
}

pub const SOURCE_FORMAT_VERSION: u32 = 1;
pub const SOURCE_ROOT_PAGE_MAX: u32 = 512;

pub(crate) const SQLITE_V7_SCHEMA: &str = r#"
CREATE TABLE git_source_roots(
 root_hash TEXT PRIMARY KEY,root_len INTEGER NOT NULL CHECK(root_len>0),workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,
 source_format_version INTEGER NOT NULL CHECK(source_format_version BETWEEN 1 AND 4294967295),object_format TEXT NOT NULL CHECK(object_format IN('sha1','sha256')),
 semantic_digest TEXT NOT NULL CHECK(length(semantic_digest)=64),object_set_digest TEXT NOT NULL CHECK(length(object_set_digest)=64),
 object_count INTEGER NOT NULL CHECK(object_count>0),total_bytes INTEGER NOT NULL CHECK(total_bytes>0),registration_operation TEXT NOT NULL UNIQUE,
 registration_generation INTEGER NOT NULL UNIQUE CHECK(registration_generation>0),state TEXT NOT NULL CHECK(state IN('registered','quarantined')),
 created_at INTEGER NOT NULL,registered_at INTEGER NOT NULL,UNIQUE(workspace,repo,commit_oid,source_format_version),
 UNIQUE(root_hash,workspace,repo,commit_oid,source_format_version));
CREATE TABLE git_source_members(
 root_hash TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal>=0),child_hash TEXT NOT NULL,child_len INTEGER NOT NULL CHECK(child_len>0),
 kind TEXT NOT NULL CHECK(kind IN('pack','index')),PRIMARY KEY(root_hash,ordinal),UNIQUE(root_hash,child_hash),
 FOREIGN KEY(root_hash) REFERENCES git_source_roots(root_hash) ON DELETE RESTRICT);
CREATE INDEX git_source_members_child ON git_source_members(child_hash,root_hash);
CREATE TABLE git_source_acquisition_sequence(id INTEGER PRIMARY KEY CHECK(id=1),generation INTEGER NOT NULL CHECK(generation>=0));
INSERT INTO git_source_acquisition_sequence(id,generation) VALUES(1,0);
CREATE TABLE git_source_acquisitions(
 token TEXT PRIMARY KEY,generation INTEGER NOT NULL UNIQUE CHECK(generation>0),operation_id TEXT NOT NULL UNIQUE,
 workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,source_format_version INTEGER NOT NULL,
 owner TEXT NOT NULL,attempt_id TEXT NOT NULL,root_hash TEXT,root_len INTEGER,object_format TEXT,semantic_digest TEXT,object_set_digest TEXT,
 object_count INTEGER,total_bytes INTEGER,expires_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('held','graph_published','activation_unknown','registered','failed')),
 failure_class TEXT CHECK(failure_class IN('retryable','permanent','dead_letter')),
 CHECK((state='held' AND root_hash IS NULL AND root_len IS NULL AND object_format IS NULL AND semantic_digest IS NULL AND object_set_digest IS NULL AND object_count IS NULL AND total_bytes IS NULL AND failure_class IS NULL)
    OR (state IN('graph_published','activation_unknown','registered') AND root_hash IS NOT NULL AND root_len>0 AND object_format IN('sha1','sha256') AND semantic_digest IS NOT NULL AND object_set_digest IS NOT NULL AND object_count>0 AND total_bytes>0 AND failure_class IS NULL)
    OR (state='failed' AND failure_class IS NOT NULL)));
CREATE UNIQUE INDEX git_source_acquisitions_one_active_identity ON git_source_acquisitions(workspace,repo,commit_oid,source_format_version) WHERE state IN('held','graph_published','activation_unknown');
CREATE INDEX git_source_acquisitions_recovery ON git_source_acquisitions(state,generation,token);
CREATE TABLE git_source_acquisition_members(
 token TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal>=0),child_hash TEXT NOT NULL,child_len INTEGER NOT NULL CHECK(child_len>0),kind TEXT NOT NULL CHECK(kind IN('pack','index')),
 PRIMARY KEY(token,ordinal),UNIQUE(token,child_hash),FOREIGN KEY(token) REFERENCES git_source_acquisitions(token) ON DELETE CASCADE);
CREATE INDEX git_source_acquisition_members_child ON git_source_acquisition_members(child_hash,token);
CREATE TABLE git_source_desires(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,source_format_version INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN('acquiring','registered','failed')),root_hash TEXT,failure_class TEXT CHECK(failure_class IN('retryable','permanent','dead_letter')),
 retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count BETWEEN 0 AND 4294967295),acquisition_token TEXT,updated_at INTEGER NOT NULL,
 PRIMARY KEY(workspace,repo,commit_oid,source_format_version),
 CHECK((state='acquiring' AND acquisition_token IS NOT NULL AND root_hash IS NULL AND failure_class IS NULL)
    OR (state='registered' AND acquisition_token IS NULL AND root_hash IS NOT NULL AND failure_class IS NULL)
    OR (state='failed' AND acquisition_token IS NULL AND root_hash IS NULL AND failure_class IS NOT NULL)),
 FOREIGN KEY(root_hash) REFERENCES git_source_roots(root_hash) ON DELETE RESTRICT,FOREIGN KEY(acquisition_token) REFERENCES git_source_acquisitions(token) ON DELETE RESTRICT);
CREATE TABLE branch_source_generations(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),commit_oid TEXT NOT NULL,
 source_format_version INTEGER NOT NULL,root_hash TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch,generation),
 FOREIGN KEY(root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT);
CREATE INDEX branch_source_generations_root ON branch_source_generations(root_hash,workspace,repo);
CREATE TABLE branch_source_current(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch),
 FOREIGN KEY(workspace,repo,branch,generation) REFERENCES branch_source_generations(workspace,repo,branch,generation) ON DELETE RESTRICT);
CREATE TABLE git_source_consumers(
 root_hash TEXT NOT NULL,consumer_id TEXT NOT NULL,session_id TEXT NOT NULL UNIQUE,workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,
 source_format_version INTEGER NOT NULL,purpose TEXT NOT NULL CHECK(purpose IN('intent','builder')),expires_at INTEGER NOT NULL,
 PRIMARY KEY(root_hash,consumer_id),FOREIGN KEY(root_hash,workspace,repo,commit_oid,source_format_version)
 REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT);
CREATE INDEX git_source_consumers_expiry ON git_source_consumers(expires_at,root_hash,consumer_id);
CREATE TABLE artifact_intents(
 id INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,branch_generation INTEGER NOT NULL,
 source_root_hash TEXT NOT NULL,source_format_version INTEGER NOT NULL,commit_oid TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN('head','full_history','files')),
 format_version INTEGER NOT NULL CHECK(format_version BETWEEN 1 AND 4294967295),state TEXT NOT NULL CHECK(state IN('deferred','promoted')),
 artifact_id INTEGER,consumer_id TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,
 UNIQUE(workspace,repo,branch,branch_generation,kind,format_version),CHECK((state='deferred' AND artifact_id IS NULL) OR (state='promoted' AND artifact_id IS NOT NULL)),
 FOREIGN KEY(workspace,repo,branch,branch_generation) REFERENCES branch_source_generations(workspace,repo,branch,generation) ON DELETE RESTRICT,
 FOREIGN KEY(source_root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT,
 FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE RESTRICT);
CREATE INDEX artifact_intents_promotion ON artifact_intents(state,updated_at,id);
CREATE INDEX artifact_intents_source ON artifact_intents(source_root_hash,state,id);
CREATE TABLE git_source_maintenance(id INTEGER PRIMARY KEY CHECK(id=1),intent_cursor INTEGER NOT NULL DEFAULT 0 CHECK(intent_cursor>=0),intent_workspace_cursor TEXT NOT NULL DEFAULT '',acquisition_cursor INTEGER NOT NULL DEFAULT 0 CHECK(acquisition_cursor>=0),root_cursor TEXT NOT NULL DEFAULT '',config_fingerprint TEXT NOT NULL DEFAULT '',updated_at INTEGER NOT NULL DEFAULT 0);
INSERT INTO git_source_maintenance(id) VALUES(1);
"#;

#[derive(Debug, Clone)]
pub struct GitSourceAcquisition {
    token: String,
    generation: u64,
    operation_id: String,
    workspace: String,
    repo: String,
    commit: String,
    source_format_version: u32,
    root: CasBlob,
}

#[derive(Debug, Clone)]
pub struct GitSourcePreparePermit {
    token: String,
    generation: u64,
    operation_id: String,
    workspace: String,
    repo: String,
    commit: String,
    source_format_version: u32,
    owner: String,
    attempt_id: String,
}

#[derive(Debug, Clone)]
pub struct GitSourcePublicationPermit {
    token: String,
    generation: u64,
    workspace: String,
    repo: String,
    commit: String,
    root: CasBlob,
}

impl GitSourcePublicationPermit {
    pub(crate) fn validate(&self, prepared: &PreparedGitSource) -> Result<()> {
        if self.token.len() != 64
            || self.generation == 0
            || !prepared.matches_publication(&self.workspace, &self.repo, &self.commit, &self.root)
        {
            bail!("Git source publication permit does not match prepared graph")
        }
        Ok(())
    }
}

pub enum SourceAcquireOutcome {
    Ready(DurableSourceSnapshot),
    Acquired {
        acquisition: GitSourceAcquisition,
        permit: GitSourcePublicationPermit,
    },
    Deferred {
        token: String,
        generation: u64,
    },
    ActivationUnknown {
        token: String,
        generation: u64,
    },
    Failed {
        class: FailureClass,
        retries: u32,
    },
}

pub enum SourceBeginOutcome {
    Ready(DurableSourceSnapshot),
    PermitToPrepare(GitSourcePreparePermit),
    Deferred { token: String, generation: u64 },
    ActivationUnknown { token: String, generation: u64 },
    Failed { class: FailureClass, retries: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceGcObject {
    pub hash: String,
    pub len: u64,
    pub owner: String,
}

pub(crate) struct GitSourceRegistryRecord {
    root: CasBlob,
    workspace: String,
    repo: String,
    commit: String,
    object_format: GitObjectFormat,
    evidence_mac: [u8; 32],
}

impl GitSourceRegistryRecord {
    pub(crate) fn root(&self) -> &CasBlob {
        &self.root
    }
    pub(crate) fn workspace(&self) -> &str {
        &self.workspace
    }
    pub(crate) fn repo(&self) -> &str {
        &self.repo
    }
    pub(crate) fn commit(&self) -> &str {
        &self.commit
    }
    pub(crate) fn object_format(&self) -> GitObjectFormat {
        self.object_format
    }
    pub(crate) fn evidence_mac(&self) -> &[u8; 32] {
        &self.evidence_mac
    }
}

#[derive(Clone)]
pub struct SqliteGitSourceRegistry {
    pool: SqlitePool,
    storage: StorageRef,
    scheduler_limits: SchedulerLimits,
    source_limits: GitSourceLimits,
    seal: Arc<[u8; 32]>,
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

impl SqliteGitSourceRegistry {
    pub async fn new(
        pool: SqlitePool,
        storage: StorageRef,
        scheduler_limits: SchedulerLimits,
        source_limits: GitSourceLimits,
        seal: [u8; 32],
    ) -> Result<Self> {
        let registry = Self {
            pool,
            storage,
            scheduler_limits,
            source_limits,
            seal: Arc::new(seal),
        };
        let fingerprint = registry.source_fingerprint();
        let scheduler_fingerprint = scheduler_limits_fingerprint(&registry.scheduler_limits);
        let mut c = ImmediateTransaction::begin(&registry.pool).await?;
        let result:Result<()>=async{
            let durable_scheduler_fingerprint:String=sqlx::query_scalar("SELECT limits_fingerprint FROM scheduler_state WHERE id=1").fetch_one(&mut *c).await.context("source registry requires an initialized durable scheduler limits capability")?;
            if durable_scheduler_fingerprint!=scheduler_fingerprint{bail!("source registry scheduler limits differ from the durable scheduler capability")}
            let maintenance:Vec<(i64,String,i64,String,i64,String,i64)>=sqlx::query_as("SELECT id,config_fingerprint,intent_cursor,intent_workspace_cursor,acquisition_cursor,root_cursor,updated_at FROM git_source_maintenance").fetch_all(&mut *c).await?;
            if maintenance.len()!=1||maintenance[0].0!=1{bail!("source registry maintenance singleton is invalid")}
            let stored=&maintenance[0].1;
            if stored.is_empty(){
                let authoritative:i64=sqlx::query_scalar("SELECT (SELECT generation FROM git_source_acquisition_sequence WHERE id=1)+(SELECT count(*) FROM git_source_roots)+(SELECT count(*) FROM git_source_members)+(SELECT count(*) FROM git_source_acquisitions)+(SELECT count(*) FROM git_source_acquisition_members)+(SELECT count(*) FROM git_source_desires)+(SELECT count(*) FROM branch_source_generations)+(SELECT count(*) FROM branch_source_current)+(SELECT count(*) FROM git_source_consumers)+(SELECT count(*) FROM artifact_intents)").fetch_one(&mut *c).await?;
                if authoritative!=0||maintenance[0].2!=0||!maintenance[0].3.is_empty()||maintenance[0].4!=0||!maintenance[0].5.is_empty()||maintenance[0].6!=0{bail!("empty source registry fingerprint has authoritative state")}
                if sqlx::query("UPDATE git_source_maintenance SET config_fingerprint=? WHERE id=1 AND config_fingerprint=''").bind(&fingerprint).execute(&mut *c).await?.rows_affected()!=1{bail!("source registry configuration CAS failed")}
            }
            else if stored.as_str()!=fingerprint{bail!("source registry limits or authority seal differ from fleet configuration")}
            Ok(())
        }.await;
        finish(c, result).await?;
        Ok(registry)
    }

    pub fn fleet_seal_identity(&self) -> String {
        hex::encode(Sha256::digest(self.seal.as_ref()))
    }

    fn source_fingerprint(&self) -> String {
        let l = &self.source_limits;
        let scheduler = &self.scheduler_limits;
        let mut h = Sha256::new();
        for value in [
            l.max_manifest_bytes,
            l.max_packs as u64,
            l.max_pack_bytes,
            l.max_index_bytes,
            l.max_total_pack_bytes,
            l.max_objects as u64,
            l.max_object_bytes,
            l.max_total_object_bytes,
            l.target_pack_raw_bytes,
        ] {
            h.update(value.to_be_bytes())
        }
        h.update(self.seal.as_ref());
        h.update(SOURCE_FORMAT_VERSION.to_be_bytes());
        for value in [
            scheduler.total_backlog,
            scheduler.workspace_backlog,
            scheduler.head_reserved,
            scheduler.head_backlog,
            scheduler.full_history_backlog,
            scheduler.files_backlog,
            scheduler.total_running,
            scheduler.head_running,
            scheduler.full_history_running,
            scheduler.files_running,
            scheduler.workspace_running,
        ] {
            h.update((value as u64).to_be_bytes())
        }
        h.update(scheduler.max_claim_attempts.to_be_bytes());
        h.update(scheduler.max_manual_retries.to_be_bytes());
        hex::encode(h.finalize())
    }

    pub async fn protect_prepared(
        &self,
        prepared: &PreparedGitSource,
        owner: &str,
        attempt_id: &str,
        ttl_secs: i64,
        intent: SyncIntent,
    ) -> Result<SourceAcquireOutcome> {
        let view = prepared.registry_view(&self.source_limits)?;
        match self
            .begin_acquisition(
                &view.workspace,
                &view.repo,
                &view.commit,
                view.source_format_version,
                owner,
                attempt_id,
                ttl_secs,
                intent,
            )
            .await?
        {
            SourceBeginOutcome::Ready(snapshot) => Ok(SourceAcquireOutcome::Ready(snapshot)),
            SourceBeginOutcome::PermitToPrepare(prepare) => {
                match self.bind_prepared_graph(&prepare, prepared).await {
                    Ok((acquisition, permit)) => Ok(SourceAcquireOutcome::Acquired {
                        acquisition,
                        permit,
                    }),
                    Err(error) => {
                        let _ = self
                            .fail_preparation(&prepare, FailureClass::Retryable)
                            .await?;
                        Err(error)
                    }
                }
            }
            SourceBeginOutcome::Deferred { token, generation } => {
                Ok(SourceAcquireOutcome::Deferred { token, generation })
            }
            SourceBeginOutcome::ActivationUnknown { token, generation } => {
                Ok(SourceAcquireOutcome::ActivationUnknown { token, generation })
            }
            SourceBeginOutcome::Failed { class, retries } => {
                Ok(SourceAcquireOutcome::Failed { class, retries })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn begin_acquisition(
        &self,
        workspace: &str,
        repo: &str,
        commit: &str,
        source_format_version: u32,
        owner: &str,
        attempt_id: &str,
        ttl_secs: i64,
        intent: SyncIntent,
    ) -> Result<SourceBeginOutcome> {
        self.begin_acquisition_with_commit_fault(
            workspace,
            repo,
            commit,
            source_format_version,
            owner,
            attempt_id,
            ttl_secs,
            intent,
            TransactionCommitFault::None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn begin_acquisition_with_commit_fault(
        &self,
        workspace: &str,
        repo: &str,
        commit: &str,
        source_format_version: u32,
        owner: &str,
        attempt_id: &str,
        ttl_secs: i64,
        intent: SyncIntent,
        commit_fault: TransactionCommitFault,
    ) -> Result<SourceBeginOutcome> {
        validate_acquire_identity(
            workspace,
            repo,
            commit,
            source_format_version,
            owner,
            attempt_id,
            ttl_secs,
        )?;
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result = self
            .begin_in(
                &mut c,
                workspace,
                repo,
                commit,
                source_format_version,
                owner,
                attempt_id,
                ttl_secs,
                intent,
            )
            .await;
        finish_with_commit_fault(c, result, commit_fault).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn begin_in(
        &self,
        c: &mut sqlx::SqliteConnection,
        workspace: &str,
        repo: &str,
        commit: &str,
        source_format_version: u32,
        owner: &str,
        attempt_id: &str,
        ttl_secs: i64,
        intent: SyncIntent,
    ) -> Result<SourceBeginOutcome> {
        let now: i64 = sqlx::query_scalar("SELECT unixepoch()")
            .fetch_one(&mut *c)
            .await?;
        self.reclaim_expired_identity_in(c, workspace, repo, commit, source_format_version, now)
            .await?;
        if let Some(row) = sqlx::query("SELECT state,root_hash,failure_class,retry_count,acquisition_token FROM git_source_desires WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=?")
            .bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).fetch_optional(&mut *c).await? {
            let state: String = row.try_get("state")?;
            if state == "registered" {
                let root: String = row.try_get("root_hash")?;
                let (token, generation):(String,i64)=sqlx::query_as("SELECT token,generation FROM git_source_acquisitions WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND root_hash=? AND state='registered' ORDER BY generation DESC LIMIT 1")
                    .bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(&root).fetch_one(&mut *c).await?;
                return Ok(SourceBeginOutcome::Ready(DurableSourceSnapshot::registered(workspace.to_owned(),repo.to_owned(),commit.to_owned(),root,token,checked_u64(generation,"source generation")?)?));
            }
            if state == "acquiring" {
                let token:String=row.try_get("acquisition_token")?;
                let (generation,acq_state):(i64,String)=sqlx::query_as("SELECT generation,state FROM git_source_acquisitions WHERE token=?").bind(&token).fetch_one(&mut *c).await?;
                let generation=checked_u64(generation,"source generation")?;
                return Ok(if acq_state=="activation_unknown" { SourceBeginOutcome::ActivationUnknown{token,generation} } else { SourceBeginOutcome::Deferred{token,generation} });
            }
            let class=FailureClass::parse(row.try_get::<String,_>("failure_class")?.as_str())?;
            let retries=checked_u32(row.try_get("retry_count")?,"source retry count")?;
            if intent==SyncIntent::ObserveMovement || class!=FailureClass::Retryable || retries>=self.scheduler_limits.max_manual_retries { return Ok(SourceBeginOutcome::Failed{class,retries}); }
        }
        let prior: i64 =
            sqlx::query_scalar("SELECT generation FROM git_source_acquisition_sequence WHERE id=1")
                .fetch_one(&mut *c)
                .await?;
        let generation = prior.checked_add(1).context("source generation overflow")?;
        if sqlx::query(
            "UPDATE git_source_acquisition_sequence SET generation=? WHERE id=1 AND generation=?",
        )
        .bind(generation)
        .bind(prior)
        .execute(&mut *c)
        .await?
        .rows_affected()
            != 1
        {
            bail!("source generation CAS failed")
        }
        let token = hex::encode(rand::random::<[u8; 32]>());
        let operation_id = operation_id(workspace, repo, commit, attempt_id, generation);
        sqlx::query("INSERT INTO git_source_acquisitions(token,generation,operation_id,workspace,repo,commit_oid,source_format_version,owner,attempt_id,expires_at,state,failure_class) VALUES(?,?,?,?,?,?,?,?,?,?,'held',NULL)")
            .bind(&token).bind(generation).bind(&operation_id).bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(owner).bind(attempt_id).bind(now+ttl_secs).execute(&mut *c).await?;
        sqlx::query("INSERT INTO git_source_desires(workspace,repo,commit_oid,source_format_version,state,root_hash,failure_class,retry_count,acquisition_token,updated_at) VALUES(?,?,?,?,'acquiring',NULL,NULL,0,?,?) ON CONFLICT(workspace,repo,commit_oid,source_format_version) DO UPDATE SET state='acquiring',root_hash=NULL,failure_class=NULL,retry_count=git_source_desires.retry_count+1,acquisition_token=excluded.acquisition_token,updated_at=excluded.updated_at")
            .bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(&token).bind(now).execute(&mut *c).await?;
        Ok(SourceBeginOutcome::PermitToPrepare(
            GitSourcePreparePermit {
                token,
                generation: checked_u64(generation, "source generation")?,
                operation_id,
                workspace: workspace.to_owned(),
                repo: repo.to_owned(),
                commit: commit.to_owned(),
                source_format_version,
                owner: owner.to_owned(),
                attempt_id: attempt_id.to_owned(),
            },
        ))
    }

    pub async fn bind_prepared_graph(
        &self,
        prepare: &GitSourcePreparePermit,
        prepared: &PreparedGitSource,
    ) -> Result<(GitSourceAcquisition, GitSourcePublicationPermit)> {
        let view = prepared.registry_view(&self.source_limits)?;
        if prepare.workspace != view.workspace
            || prepare.repo != view.repo
            || prepare.commit != view.commit
            || prepare.source_format_version != view.source_format_version
        {
            bail!("prepared graph identity differs from held source acquisition")
        }
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<(GitSourceAcquisition,GitSourcePublicationPermit)>=async{
            let now:i64=sqlx::query_scalar("SELECT unixepoch()").fetch_one(&mut *c).await?;
            let sweep:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_gc_sweep WHERE expires_at>?").bind(now).fetch_one(&mut *c).await?;
            if sweep!=0{bail!("source graph publication is fenced by live GC sweep")}
            if sqlx::query("UPDATE git_source_acquisitions SET root_hash=?,root_len=?,object_format=?,semantic_digest=?,object_set_digest=?,object_count=?,total_bytes=?,state='graph_published' WHERE token=? AND generation=? AND operation_id=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND owner=? AND attempt_id=? AND state='held' AND expires_at>?")
                .bind(&view.root.hash).bind(checked_i64(view.root.len,"root length")?).bind(view.object_format).bind(&view.semantic_digest).bind(&view.object_set_digest).bind(checked_i64(view.object_count,"object count")?).bind(checked_i64(view.total_bytes,"source bytes")?)
                .bind(&prepare.token).bind(prepare.generation as i64).bind(&prepare.operation_id).bind(&prepare.workspace).bind(&prepare.repo).bind(&prepare.commit).bind(prepare.source_format_version as i64).bind(&prepare.owner).bind(&prepare.attempt_id).bind(now).execute(&mut *c).await?.rows_affected()!=1{bail!("held source preparation capability was lost")}
            for member in &view.members{sqlx::query("INSERT INTO git_source_acquisition_members(token,ordinal,child_hash,child_len,kind) VALUES(?,?,?,?,?)").bind(&prepare.token).bind(member.ordinal as i64).bind(&member.blob.hash).bind(checked_i64(member.blob.len,"member length")?).bind(member.kind).execute(&mut *c).await?;}
            let acquisition=GitSourceAcquisition{token:prepare.token.clone(),generation:prepare.generation,operation_id:prepare.operation_id.clone(),workspace:prepare.workspace.clone(),repo:prepare.repo.clone(),commit:prepare.commit.clone(),source_format_version:prepare.source_format_version,root:view.root.clone()};
            let publication=GitSourcePublicationPermit{token:prepare.token.clone(),generation:prepare.generation,workspace:prepare.workspace.clone(),repo:prepare.repo.clone(),commit:prepare.commit.clone(),root:view.root.clone()};
            Ok((acquisition,publication))
        }.await;
        finish(c, result).await
    }

    async fn reclaim_expired_identity_in(
        &self,
        c: &mut sqlx::SqliteConnection,
        workspace: &str,
        repo: &str,
        commit: &str,
        source_format_version: u32,
        now: i64,
    ) -> Result<()> {
        if let Some(token)=sqlx::query_scalar::<_,String>("SELECT token FROM git_source_acquisitions WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND state IN('held','graph_published') AND expires_at<=?")
            .bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(now).fetch_optional(&mut *c).await? {
            if sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=? WHERE acquisition_token=? AND state='acquiring'").bind(now).bind(&token).execute(&mut *c).await?.rows_affected()!=1 { bail!("expired source desire settlement lost") }
            if sqlx::query("UPDATE git_source_acquisitions SET state='failed',failure_class='retryable',expires_at=0 WHERE token=? AND state IN('held','graph_published')").bind(&token).execute(&mut *c).await?.rows_affected()!=1 { bail!("expired source acquisition settlement lost") }
        }
        Ok(())
    }

    pub async fn renew_preparation(
        &self,
        prepare: &GitSourcePreparePermit,
        ttl_secs: i64,
    ) -> Result<bool> {
        if !(1..=3600).contains(&ttl_secs) {
            bail!("source preparation TTL is invalid")
        }
        Ok(sqlx::query("UPDATE git_source_acquisitions SET expires_at=unixepoch()+? WHERE token=? AND generation=? AND operation_id=? AND owner=? AND attempt_id=? AND state='held' AND expires_at>unixepoch()")
            .bind(ttl_secs).bind(&prepare.token).bind(prepare.generation as i64).bind(&prepare.operation_id).bind(&prepare.owner).bind(&prepare.attempt_id).execute(&self.pool).await?.rows_affected()==1)
    }

    pub async fn fail_preparation(
        &self,
        prepare: &GitSourcePreparePermit,
        class: FailureClass,
    ) -> Result<bool> {
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<bool>=async{
            if sqlx::query("UPDATE git_source_acquisitions SET state='failed',failure_class=?,expires_at=0 WHERE token=? AND generation=? AND operation_id=? AND owner=? AND attempt_id=? AND state='held'")
                .bind(class.as_str()).bind(&prepare.token).bind(prepare.generation as i64).bind(&prepare.operation_id).bind(&prepare.owner).bind(&prepare.attempt_id).execute(&mut *c).await?.rows_affected()!=1{return Ok(false)}
            if sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class=?,acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'")
                .bind(class.as_str()).bind(&prepare.token).execute(&mut *c).await?.rows_affected()!=1{bail!("source preparation desire settlement lost")}
            Ok(true)
        }.await;
        finish(c, result).await
    }

    pub async fn renew(&self, acquisition: &GitSourceAcquisition, ttl_secs: i64) -> Result<bool> {
        if !(1..=3600).contains(&ttl_secs) {
            bail!("source acquisition TTL is invalid")
        }
        Ok(sqlx::query("UPDATE git_source_acquisitions SET expires_at=unixepoch()+? WHERE token=? AND generation=? AND operation_id=? AND state='graph_published' AND expires_at>unixepoch()")
            .bind(ttl_secs).bind(&acquisition.token).bind(acquisition.generation as i64).bind(&acquisition.operation_id).execute(&self.pool).await?.rows_affected()==1)
    }

    pub async fn publish_protected<U: GitSourceUploader + Clone + 'static>(
        &self,
        acquisition: &GitSourceAcquisition,
        packager: &GitSourcePackager<'_, U>,
        prepared: &PreparedGitSource,
        permit: &GitSourcePublicationPermit,
        cancelled: &CancellationToken,
    ) -> Result<()> {
        permit.validate(prepared)?;
        if acquisition.token != permit.token
            || acquisition.generation != permit.generation
            || acquisition.root != permit.root
        {
            bail!("source acquisition and publication permit differ")
        }
        let plan = packager.owned_upload_plan(prepared)?;
        let publication_cancel = cancelled.child_token();
        let heartbeat_cancel = publication_cancel.clone();
        let registry = self.clone();
        let heartbeat_acquisition = acquisition.clone();
        let mut heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {_=heartbeat_cancel.cancelled()=>return Ok(()),_=interval.tick()=>{if !registry.renew(&heartbeat_acquisition,60).await?{heartbeat_cancel.cancel();bail!("source acquisition lease was lost during upload")}}}
            }
        });
        let upload_cancel = publication_cancel.clone();
        let mut upload = tokio::task::spawn_blocking(move || plan.publish(&upload_cancel));
        tokio::select! {
            result=&mut upload=>{
                publication_cancel.cancel();
                let upload_result=result.context("source upload task did not join")?;
                let heartbeat_result=heartbeat.await.context("source upload heartbeat did not join")?;
                heartbeat_result?;upload_result
            }
            result=&mut heartbeat=>{
                publication_cancel.cancel();
                let heartbeat_result=result.context("source upload heartbeat did not join")?;
                let upload_result=upload.await.context("cancelled source upload task did not join")?;
                heartbeat_result?;upload_result
            }
        }
    }

    pub async fn fail(
        &self,
        acquisition: &GitSourceAcquisition,
        class: FailureClass,
    ) -> Result<bool> {
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<bool>=async{
            let desire=sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class=?,acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'")
                .bind(class.as_str()).bind(&acquisition.token).execute(&mut *c).await?.rows_affected();
            if desire==0{return Ok(false)}
            if sqlx::query("UPDATE git_source_acquisitions SET state='failed',failure_class=?,expires_at=0 WHERE token=? AND generation=? AND operation_id=? AND state='graph_published'")
                .bind(class.as_str()).bind(&acquisition.token).bind(acquisition.generation as i64).bind(&acquisition.operation_id).execute(&mut *c).await?.rows_affected()!=1{bail!("source failure capability lost")}
            Ok(true)
        }.await;
        finish(c, result).await
    }

    async fn register_once(
        &self,
        acquisition: &GitSourceAcquisition,
        prepared: &PreparedGitSource,
        cancelled: &CancellationToken,
        commit_fault: RegistrationCommitFault,
    ) -> Result<DurableSourceSnapshot> {
        let view = prepared.registry_view(&self.source_limits)?;
        verify_acquisition_identity(acquisition, &view)?;
        let storage = self.storage.clone();
        let blobs = view
            .members
            .iter()
            .map(|m| m.blob.clone())
            .chain(std::iter::once(view.root.clone()))
            .collect::<Vec<_>>();
        let root_bytes = view.root_bytes.clone();
        let root_hash = view.root.hash.clone();
        let verification_cancel = CancellationToken::new();
        let blocking_cancel = verification_cancel.clone();
        let mut verify = tokio::task::spawn_blocking(move || {
            verify_storage_graph(&storage, &blobs, &root_hash, &root_bytes, &blocking_cancel)
        });
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                result=&mut verify=>{result.context("Git source storage verifier did not join")??;break}
                _=cancelled.cancelled()=>{
                    verification_cancel.cancel();
                    verify.await.context("cancelled Git source verifier did not join")??;
                    bail!("Git source registration cancelled")
                }
                _=heartbeat.tick()=>{
                    if !self.renew(acquisition,60).await?{
                        verification_cancel.cancel();
                        verify.await.context("lease-lost Git source verifier did not join")??;
                        bail!("Git source acquisition lease was lost during verification")
                    }
                }
            }
        }
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<DurableSourceSnapshot>=async{
            let now:i64=sqlx::query_scalar("SELECT unixepoch()").fetch_one(&mut *c).await?;
            assert_exact_graph(&mut c,acquisition,&view,now).await?;
            sqlx::query("INSERT INTO git_source_roots(root_hash,root_len,workspace,repo,commit_oid,source_format_version,object_format,semantic_digest,object_set_digest,object_count,total_bytes,registration_operation,registration_generation,state,created_at,registered_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'registered',?,?)")
                .bind(&view.root.hash).bind(checked_i64(view.root.len,"root length")?).bind(&view.workspace).bind(&view.repo).bind(&view.commit).bind(view.source_format_version as i64).bind(view.object_format).bind(&view.semantic_digest).bind(&view.object_set_digest).bind(checked_i64(view.object_count,"object count")?).bind(checked_i64(view.total_bytes,"source bytes")?).bind(&acquisition.operation_id).bind(acquisition.generation as i64).bind(now).bind(now).execute(&mut *c).await?;
            for member in &view.members{sqlx::query("INSERT INTO git_source_members(root_hash,ordinal,child_hash,child_len,kind) VALUES(?,?,?,?,?)").bind(&view.root.hash).bind(member.ordinal as i64).bind(&member.blob.hash).bind(checked_i64(member.blob.len,"member length")?).bind(member.kind).execute(&mut *c).await?;}
            if sqlx::query("UPDATE git_source_acquisitions SET state='registered',expires_at=0 WHERE token=? AND generation=? AND state='graph_published'").bind(&acquisition.token).bind(acquisition.generation as i64).execute(&mut *c).await?.rows_affected()!=1{bail!("source registration capability lost")}
            if sqlx::query("UPDATE git_source_desires SET state='registered',root_hash=?,failure_class=NULL,acquisition_token=NULL,updated_at=? WHERE acquisition_token=? AND state='acquiring'").bind(&view.root.hash).bind(now).bind(&acquisition.token).execute(&mut *c).await?.rows_affected()!=1{bail!("source registration desire lost")}
            DurableSourceSnapshot::registered(view.workspace.clone(),view.repo.clone(),view.commit.clone(),view.root.hash.clone(),acquisition.token.clone(),acquisition.generation)
        }.await;
        match result {
            Err(error) => match sqlx::query("ROLLBACK").execute(&mut *c).await {
                Ok(_) => {
                    c.release();
                    Err(error.context("source registration failed before commit"))
                }
                Err(rollback) => Err(error).context(format!(
                    "source registration rollback failed; connection retired: {rollback}"
                )),
            },
            Ok(_snapshot) if commit_fault == RegistrationCommitFault::FailWithOpenTransaction => {
                let error = anyhow::anyhow!("injected COMMIT failure with an open transaction");
                Err(AmbiguousRegistrationAck(error).into())
            }
            Ok(snapshot) => match sqlx::query("COMMIT").execute(&mut *c).await {
                Ok(_) if commit_fault == RegistrationCommitFault::AckLostAfterCommit => {
                    c.release();
                    Err(
                        AmbiguousRegistrationAck(anyhow::anyhow!("injected lost COMMIT ACK"))
                            .into(),
                    )
                }
                Ok(_) => {
                    c.release();
                    Ok(snapshot)
                }
                Err(error) => Err(AmbiguousRegistrationAck(error.into()).into()),
            },
        }
    }

    pub async fn register(
        &self,
        acquisition: &GitSourceAcquisition,
        prepared: &PreparedGitSource,
        cancelled: &CancellationToken,
    ) -> Result<DurableSourceSnapshot> {
        self.register_or_recover_inner(
            acquisition,
            prepared,
            cancelled,
            RegistrationCommitFault::None,
        )
        .await
    }

    async fn register_or_recover_inner(
        &self,
        acquisition: &GitSourceAcquisition,
        prepared: &PreparedGitSource,
        cancelled: &CancellationToken,
        commit_fault: RegistrationCommitFault,
    ) -> Result<DurableSourceSnapshot> {
        let attempt = self
            .register_once(acquisition, prepared, cancelled, commit_fault)
            .await;
        match attempt {
            Ok(snapshot) => Ok(snapshot),
            Err(error) if error.downcast_ref::<AmbiguousRegistrationAck>().is_some() => {
                let _ = self.mark_activation_unknown(acquisition).await?;
                match self.reconcile_activation(acquisition).await? {
                    SourceAcquireOutcome::Ready(snapshot) => Ok(snapshot),
                    SourceAcquireOutcome::Failed { class, .. } => bail!(
                        "ambiguous source registration settled failed: {}",
                        class.as_str()
                    ),
                    _ => bail!("ambiguous source registration did not settle"),
                }
            }
            Err(error) => {
                let _ = self.fail(acquisition, FailureClass::Retryable).await?;
                Err(error)
            }
        }
    }

    pub async fn mark_activation_unknown(
        &self,
        acquisition: &GitSourceAcquisition,
    ) -> Result<bool> {
        Ok(sqlx::query("UPDATE git_source_acquisitions SET state='activation_unknown',expires_at=0 WHERE token=? AND generation=? AND operation_id=? AND state='graph_published'")
            .bind(&acquisition.token).bind(acquisition.generation as i64).bind(&acquisition.operation_id).execute(&self.pool).await?.rows_affected()==1)
    }

    pub async fn reconcile_activation(
        &self,
        acquisition: &GitSourceAcquisition,
    ) -> Result<SourceAcquireOutcome> {
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result: Result<SourceAcquireOutcome> = async {
            let state: String = sqlx::query_scalar("SELECT state FROM git_source_acquisitions WHERE token=? AND generation=? AND operation_id=? AND root_hash=?")
                .bind(&acquisition.token).bind(acquisition.generation as i64).bind(&acquisition.operation_id).bind(&acquisition.root.hash).fetch_one(&mut *c).await?;
            let registered:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_roots WHERE root_hash=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND registration_operation=? AND registration_generation=? AND state='registered'")
                .bind(&acquisition.root.hash).bind(&acquisition.workspace).bind(&acquisition.repo).bind(&acquisition.commit).bind(acquisition.source_format_version as i64).bind(&acquisition.operation_id).bind(acquisition.generation as i64).fetch_one(&mut *c).await?;
            if registered==1{
                if state=="activation_unknown"{
                    if sqlx::query("UPDATE git_source_acquisitions SET state='registered' WHERE token=? AND generation=? AND state='activation_unknown'").bind(&acquisition.token).bind(acquisition.generation as i64).execute(&mut *c).await?.rows_affected()!=1{bail!("unknown source activation settlement lost")}
                    if sqlx::query("UPDATE git_source_desires SET state='registered',root_hash=?,failure_class=NULL,acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'").bind(&acquisition.root.hash).bind(&acquisition.token).execute(&mut *c).await?.rows_affected()!=1{bail!("unknown source desire settlement lost")}
                }else if state!="registered"{bail!("registered source has an impossible acquisition state")}
                return Ok(SourceAcquireOutcome::Ready(DurableSourceSnapshot::registered(acquisition.workspace.clone(),acquisition.repo.clone(),acquisition.commit.clone(),acquisition.root.hash.clone(),acquisition.token.clone(),acquisition.generation)?));
            }
            if state!="activation_unknown"{bail!("source activation is not unknown")}
            if sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'").bind(&acquisition.token).execute(&mut *c).await?.rows_affected()!=1{bail!("uncommitted unknown source desire settlement lost")}
            if sqlx::query("UPDATE git_source_acquisitions SET state='failed',failure_class='retryable' WHERE token=? AND generation=? AND state='activation_unknown'").bind(&acquisition.token).bind(acquisition.generation as i64).execute(&mut *c).await?.rows_affected()!=1{bail!("uncommitted unknown source settlement lost")}
            let retries:i64=sqlx::query_scalar("SELECT retry_count FROM git_source_desires WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=?").bind(&acquisition.workspace).bind(&acquisition.repo).bind(&acquisition.commit).bind(acquisition.source_format_version as i64).fetch_one(&mut *c).await?;
            Ok(SourceAcquireOutcome::Failed{class:FailureClass::Retryable,retries:checked_u32(retries,"source retry count")?})
        }.await;
        finish(c, result).await
    }

    pub async fn source_gc_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<SourceGcObject>> {
        if limit == 0 || limit > SOURCE_ROOT_PAGE_MAX {
            bail!("source GC page limit is invalid")
        }
        let (after_hash, after_owner) = after.unwrap_or(("", ""));
        let rows=sqlx::query("WITH objects(hash,len,owner) AS (SELECT root_hash,root_len,'r:'||root_hash FROM git_source_roots UNION ALL SELECT child_hash,child_len,'r:'||root_hash||':'||printf('%020d',ordinal) FROM git_source_members UNION ALL SELECT root_hash,root_len,'a:'||token FROM git_source_acquisitions WHERE state='activation_unknown' OR (state='graph_published' AND expires_at>unixepoch()) UNION ALL SELECT m.child_hash,m.child_len,'a:'||m.token||':'||printf('%020d',m.ordinal) FROM git_source_acquisition_members m JOIN git_source_acquisitions a ON a.token=m.token WHERE a.state='activation_unknown' OR (a.state='graph_published' AND a.expires_at>unixepoch())) SELECT hash,len,owner FROM objects WHERE hash>? OR (hash=? AND owner>?) ORDER BY hash,owner LIMIT ?")
            .bind(after_hash).bind(after_hash).bind(after_owner).bind(limit as i64).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(SourceGcObject {
                    hash: row.try_get("hash")?,
                    len: checked_u64(row.try_get("len")?, "source GC object length")?,
                    owner: row.try_get("owner")?,
                })
            })
            .collect()
    }

    pub async fn claim_authenticated(
        &self,
        artifact_id: i64,
        artifact_owner: &str,
        lease_generation: u64,
        workspace: &str,
        repo: &str,
        commit: &str,
        session_id: &str,
        ttl_secs: i64,
    ) -> Result<AuthenticatedGitSource> {
        if session_id.len() != 64
            || !session_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || artifact_id <= 0
            || artifact_owner.trim().is_empty()
            || lease_generation == 0
            || !(1..=86400).contains(&ttl_secs)
        {
            bail!("builder source claim is invalid")
        }
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<AuthenticatedGitSource>=async{
            let row=sqlx::query("SELECT r.root_hash,r.root_len,r.object_format,r.registration_generation,r.registration_operation FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id JOIN git_source_roots r ON r.root_hash=i.source_root_hash WHERE i.artifact_id=? AND i.state='promoted' AND i.workspace=? AND i.repo=? AND i.commit_oid=? AND i.source_format_version=? AND j.state='running' AND j.owner=? AND j.lease_generation=? AND j.lease_expires_at>unixepoch() AND r.state='registered'").bind(artifact_id).bind(workspace).bind(repo).bind(commit).bind(SOURCE_FORMAT_VERSION as i64).bind(artifact_owner).bind(lease_generation as i64).fetch_optional(&mut *c).await?.context("promoted artifact does not own a live registered source claim")?;
            let root=CasBlob{hash:row.try_get("root_hash")?,len:checked_u64(row.try_get("root_len")?,"root length")?};
            let consumer=format!("builder:{artifact_id}:{session_id}");
            sqlx::query("INSERT INTO git_source_consumers(root_hash,consumer_id,session_id,workspace,repo,commit_oid,source_format_version,purpose,expires_at) VALUES(?,?,?,?,?,?,?,'builder',unixepoch()+?) ON CONFLICT(root_hash,consumer_id) DO UPDATE SET expires_at=excluded.expires_at WHERE git_source_consumers.session_id=excluded.session_id AND git_source_consumers.workspace=excluded.workspace AND git_source_consumers.repo=excluded.repo AND git_source_consumers.commit_oid=excluded.commit_oid")
                .bind(&root.hash).bind(&consumer).bind(session_id).bind(workspace).bind(repo).bind(commit).bind(SOURCE_FORMAT_VERSION as i64).bind(ttl_secs).execute(&mut *c).await?;
            let object_format=parse_object_format(row.try_get::<String,_>("object_format")?.as_str())?;
            let generation:i64=row.try_get("registration_generation")?;let operation:String=row.try_get("registration_operation")?;
            let mac=evidence_mac(&self.seal,&root,workspace,repo,commit,object_format,generation,&operation);
            AuthenticatedGitSource::from_registry_record(GitSourceRegistryRecord{root,workspace:workspace.into(),repo:repo.into(),commit:commit.into(),object_format,evidence_mac:mac})
        }.await;
        finish(c, result).await
    }

    pub async fn renew_builder_claim(
        &self,
        artifact_id: i64,
        artifact_owner: &str,
        lease_generation: u64,
        root_hash: &str,
        session_id: &str,
        ttl_secs: i64,
    ) -> Result<bool> {
        if artifact_id <= 0
            || artifact_owner.trim().is_empty()
            || lease_generation == 0
            || !(1..=86400).contains(&ttl_secs)
        {
            bail!("builder source claim TTL is invalid")
        }
        Ok(sqlx::query("UPDATE git_source_consumers SET expires_at=unixepoch()+? WHERE root_hash=? AND session_id=? AND purpose='builder' AND expires_at>unixepoch() AND EXISTS(SELECT 1 FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.artifact_id=? AND i.source_root_hash=git_source_consumers.root_hash AND i.state='promoted' AND j.state='running' AND j.owner=? AND j.lease_generation=? AND j.lease_expires_at>unixepoch())")
            .bind(ttl_secs).bind(root_hash).bind(session_id).bind(artifact_id).bind(artifact_owner).bind(lease_generation as i64).execute(&self.pool).await?.rows_affected()==1)
    }

    pub async fn release_builder_claim(&self, root_hash: &str, session_id: &str) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM git_source_consumers WHERE root_hash=? AND session_id=? AND purpose='builder'")
            .bind(root_hash).bind(session_id).execute(&self.pool).await?.rows_affected()==1)
    }

    pub async fn with_materialized_builder_source<L, F, T>(
        &self,
        artifact_id: i64,
        artifact_owner: &str,
        lease_generation: u64,
        workspace: &str,
        repo: &str,
        commit: &str,
        session_id: &str,
        ttl_secs: i64,
        loader: L,
        scratch: PathBuf,
        cancelled: &CancellationToken,
        work: F,
    ) -> Result<T>
    where
        L: GitSourceLoader + Send + Sync + 'static,
        F: FnOnce(&MaterializedGitSource, &CancellationToken) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let authority = self
            .claim_authenticated(
                artifact_id,
                artifact_owner,
                lease_generation,
                workspace,
                repo,
                commit,
                session_id,
                ttl_secs,
            )
            .await?;
        let root = authority.root_hash().to_owned();
        let owned_cancel = cancelled.child_token();
        let task_cancel = owned_cancel.clone();
        let limits = self.source_limits.clone();
        let mut task = tokio::task::spawn_blocking(move || {
            let materialized = GitSourceMaterializer::new(&loader, &scratch, limits)
                .materialize(&authority, &task_cancel)?;
            work(&materialized, &task_cancel)
        });
        let registry = self.clone();
        let heartbeat_root = root.clone();
        let heartbeat_session = session_id.to_owned();
        let heartbeat_owner = artifact_owner.to_owned();
        let heartbeat_cancel = owned_cancel.clone();
        let heartbeat_period = std::time::Duration::from_millis(
            ((ttl_secs as u64).saturating_mul(1000) / 3).clamp(100, 10_000),
        );
        let mut heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_period);
            loop {
                tokio::select! {_=heartbeat_cancel.cancelled()=>return Ok(()),_=interval.tick()=>{if !registry.renew_builder_claim(artifact_id,&heartbeat_owner,lease_generation,&heartbeat_root,&heartbeat_session,ttl_secs).await?{heartbeat_cancel.cancel();bail!("builder source or artifact lease was lost during materialization")}}}
            }
        });
        let (task_result, heartbeat_result) = tokio::select! {
            value=&mut task=>{
                owned_cancel.cancel();
                let task_result=value.context("builder source task did not join").and_then(|result|result);
                let heartbeat_result=heartbeat.await.context("builder source heartbeat did not join").and_then(|result|result);
                (task_result,heartbeat_result)
            },
            beat=&mut heartbeat=>{
                owned_cancel.cancel();
                let heartbeat_result=beat.context("builder source heartbeat did not join").and_then(|result|result);
                let task_result=task.await.context("cancelled builder source task did not join").and_then(|result|result);
                (task_result,heartbeat_result)
            }
        };
        let released = self.release_builder_claim(&root, session_id).await?;
        if !released {
            bail!("builder source claim disappeared before release")
        }
        heartbeat_result?;
        task_result
    }

    pub async fn promote_deferred_page(&self, limit: u32) -> Result<u32> {
        if limit == 0 || limit > 256 {
            bail!("deferred intent promotion page is invalid")
        }
        let cursor: String = sqlx::query_scalar(
            "SELECT intent_workspace_cursor FROM git_source_maintenance WHERE id=1",
        )
        .fetch_one(&self.pool)
        .await?;
        let scan_limit = (limit as i64).saturating_mul(16).clamp(64, 4096);
        let ids:Vec<(i64,String)>=sqlx::query_as("WITH candidates AS (SELECT id,workspace,row_number() OVER(PARTITION BY workspace ORDER BY updated_at,id) round FROM artifact_intents WHERE state='deferred') SELECT id,workspace FROM candidates ORDER BY round,CASE WHEN workspace>? THEN 0 ELSE 1 END,workspace,id LIMIT ?")
            .bind(&cursor).bind(scan_limit).fetch_all(&self.pool).await?;
        let mut promoted = 0;
        for (id, candidate_workspace) in ids {
            if promoted >= limit {
                break;
            }
            let mut c = ImmediateTransaction::begin(&self.pool).await?;
            let result:Result<bool>=async{
                sqlx::query("UPDATE git_source_maintenance SET intent_cursor=?,intent_workspace_cursor=?,updated_at=unixepoch() WHERE id=1").bind(id).bind(&candidate_workspace).execute(&mut *c).await?;
                let row=match sqlx::query("SELECT workspace,repo,branch,branch_generation,commit_oid,kind,format_version,consumer_id FROM artifact_intents WHERE id=? AND state='deferred'").bind(id).fetch_optional(&mut *c).await?{Some(v)=>v,None=>return Ok(false)};
                let workspace:String=row.try_get("workspace")?;let repo:String=row.try_get("repo")?;let branch:String=row.try_get("branch")?;let generation:i64=row.try_get("branch_generation")?;let commit:String=row.try_get("commit_oid")?;let kind=ArtifactKind::parse(row.try_get("kind")?)?;let format:i64=row.try_get("format_version")?;
                let existing:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?").bind(&workspace).bind(&repo).bind(&commit).bind(kind.as_str()).bind(format).fetch_one(&mut *c).await?;
                if existing==0&&!capacity_available(&mut c,&self.scheduler_limits,&workspace,kind).await?{return Ok(false)}
                let artifact_id=ensure_job(&mut c,&workspace,&repo,&commit,kind,format).await?;
                if sqlx::query("UPDATE artifact_intents SET state='promoted',artifact_id=?,updated_at=unixepoch() WHERE id=? AND state='deferred'").bind(artifact_id).bind(id).execute(&mut *c).await?.rows_affected()!=1{return Ok(false)}
                sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?) ON CONFLICT(artifact_id,consumer_id) DO UPDATE SET expires_at=excluded.expires_at").bind(artifact_id).bind(row.try_get::<String,_>("consumer_id")?).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *c).await?;
                sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES(?,?,?,?,?,?,?,CASE WHEN (SELECT state FROM artifact_jobs WHERE id=?)='ready' THEN ? ELSE NULL END,?,unixepoch()) ON CONFLICT(workspace,repo,branch,kind) DO UPDATE SET desired_commit=excluded.desired_commit,desired_artifact_id=excluded.desired_artifact_id,desired_generation=excluded.desired_generation,published_artifact_id=CASE WHEN excluded.published_artifact_id IS NOT NULL THEN excluded.published_artifact_id WHEN artifact_observations.format_version=excluded.format_version THEN artifact_observations.published_artifact_id ELSE NULL END,format_version=excluded.format_version,observed_at=excluded.observed_at")
                    .bind(&workspace).bind(&repo).bind(&branch).bind(kind.as_str()).bind(&commit).bind(artifact_id).bind(generation).bind(artifact_id).bind(artifact_id).bind(format).execute(&mut *c).await?;
                Ok(true)
            }.await;
            if finish(c, result).await? {
                promoted += 1
            }
        }
        Ok(promoted)
    }

    pub async fn reconcile_terminal_intents(&self, limit: u32) -> Result<u32> {
        self.reconcile_terminal_intents_inner(limit, None).await
    }

    async fn reconcile_terminal_intents_inner(
        &self,
        limit: u32,
        barrier: Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>,
    ) -> Result<u32> {
        if limit == 0 || limit > 512 {
            bail!("intent reconciliation page is invalid")
        }
        let rows:Vec<(i64,i64,String)>=sqlx::query_as("SELECT i.id,i.artifact_id,i.consumer_id FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.state='promoted' AND j.state IN('ready','failed') ORDER BY i.id LIMIT ?").bind(limit as i64).fetch_all(&self.pool).await?;
        if let Some((scanned, resume)) = barrier {
            scanned.wait().await;
            resume.wait().await;
        }
        let mut settled = 0;
        for (id, artifact_id, consumer) in rows {
            let mut c = ImmediateTransaction::begin(&self.pool).await?;
            let result: Result<bool> = async {
                let terminal:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.id=? AND i.artifact_id=? AND i.consumer_id=? AND i.state='promoted' AND (j.state='ready' OR (j.state='failed' AND (j.failure_class IN('permanent','dead_letter') OR (j.failure_class='retryable' AND j.retry_count>=?))))")
                    .bind(id).bind(artifact_id).bind(&consumer).bind(self.scheduler_limits.max_manual_retries as i64).fetch_one(&mut *c).await?;
                if terminal!=1{return Ok(false)}
                let deleted = sqlx::query(
                    "DELETE FROM git_source_consumers WHERE consumer_id=? AND purpose='intent'",
                )
                .bind(&consumer)
                .execute(&mut *c)
                .await?
                .rows_affected();
                let core=sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?").bind(artifact_id).bind(&consumer).execute(&mut *c).await?.rows_affected();
                if deleted != 1 || core != 1 {
                    bail!("terminal intent handoff consumers are incomplete")
                }
                if sqlx::query("DELETE FROM artifact_intents WHERE id=? AND artifact_id=? AND state='promoted'").bind(id).bind(artifact_id).execute(&mut *c).await?.rows_affected()!=1{bail!("terminal intent settlement lost")}
                Ok(true)
            }
            .await;
            if finish(c, result).await? {
                settled += 1
            }
        }
        Ok(settled)
    }

    pub async fn prune_metadata_page(&self, limit: u32) -> Result<u64> {
        if limit == 0 || limit > 512 {
            bail!("source metadata prune page is invalid")
        }
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<u64>=async{
            let mut changed=0;
            changed+=sqlx::query("DELETE FROM git_source_consumers WHERE rowid IN(SELECT rowid FROM git_source_consumers WHERE purpose='builder' AND expires_at<=unixepoch() ORDER BY expires_at,root_hash,consumer_id LIMIT ?)").bind(limit as i64).execute(&mut *c).await?.rows_affected();
            changed+=sqlx::query("DELETE FROM branch_source_generations WHERE rowid IN(SELECT g.rowid FROM branch_source_generations g LEFT JOIN branch_source_current c ON c.workspace=g.workspace AND c.repo=g.repo AND c.branch=g.branch AND c.generation=g.generation LEFT JOIN artifact_intents i ON i.workspace=g.workspace AND i.repo=g.repo AND i.branch=g.branch AND i.branch_generation=g.generation WHERE c.workspace IS NULL AND i.id IS NULL ORDER BY g.created_at,g.workspace,g.repo,g.branch,g.generation LIMIT ?)").bind(limit as i64).execute(&mut *c).await?.rows_affected();
            let cutoff:i64=sqlx::query_scalar("SELECT MAX(0,generation-1024) FROM git_source_acquisition_sequence WHERE id=1").fetch_one(&mut *c).await?;
            changed+=sqlx::query("DELETE FROM git_source_acquisitions WHERE token IN(SELECT a.token FROM git_source_acquisitions a LEFT JOIN git_source_desires d ON d.acquisition_token=a.token WHERE a.state='failed' AND a.generation<=? AND d.acquisition_token IS NULL ORDER BY a.generation LIMIT ?)").bind(cutoff).bind(limit as i64).execute(&mut *c).await?.rows_affected();
            Ok(changed)
        }.await;
        finish(c, result).await
    }

    pub async fn retire_registered_roots_page(&self, grace_secs: i64, limit: u32) -> Result<u32> {
        if !(60..=30 * 24 * 60 * 60).contains(&grace_secs) || limit == 0 || limit > 256 {
            bail!("source root retirement grace or page is invalid")
        }
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<u32>=async{
            let sweep:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_gc_sweep WHERE expires_at>unixepoch()").fetch_one(&mut *c).await?;
            if sweep!=0{bail!("source root retirement is fenced by live GC sweep")}
            let cursor:String=sqlx::query_scalar("SELECT root_cursor FROM git_source_maintenance WHERE id=1").fetch_one(&mut *c).await?;
            let roots:Vec<String>=sqlx::query_scalar("SELECT r.root_hash FROM git_source_roots r WHERE r.state='registered' AND r.registered_at<=unixepoch()-? AND r.root_hash>? AND NOT EXISTS(SELECT 1 FROM branch_source_generations g WHERE g.root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM artifact_intents i WHERE i.source_root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM git_source_consumers c WHERE c.root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM git_source_acquisitions a WHERE a.root_hash=r.root_hash AND a.state IN('held','graph_published','activation_unknown')) ORDER BY r.root_hash LIMIT ?")
                .bind(grace_secs).bind(&cursor).bind(limit as i64).fetch_all(&mut *c).await?;
            if roots.is_empty(){if !cursor.is_empty(){sqlx::query("UPDATE git_source_maintenance SET root_cursor='',updated_at=unixepoch() WHERE id=1").execute(&mut *c).await?;}return Ok(0)}
            for root in &roots{
                sqlx::query("DELETE FROM git_source_desires WHERE root_hash=? AND state='registered'").bind(root).execute(&mut *c).await?;
                sqlx::query("DELETE FROM git_source_acquisitions WHERE root_hash=? AND state IN('registered','failed')").bind(root).execute(&mut *c).await?;
                sqlx::query("DELETE FROM git_source_members WHERE root_hash=?").bind(root).execute(&mut *c).await?;
                if sqlx::query("DELETE FROM git_source_roots WHERE root_hash=? AND state='registered' AND NOT EXISTS(SELECT 1 FROM branch_source_generations WHERE root_hash=?) AND NOT EXISTS(SELECT 1 FROM artifact_intents WHERE source_root_hash=?) AND NOT EXISTS(SELECT 1 FROM git_source_consumers WHERE root_hash=?)").bind(root).bind(root).bind(root).bind(root).execute(&mut *c).await?.rows_affected()!=1{bail!("source root retirement lost its reference proof")}
            }
            sqlx::query("UPDATE git_source_maintenance SET root_cursor=?,updated_at=unixepoch() WHERE id=1").bind(roots.last().expect("nonempty retirement page")).execute(&mut *c).await?;
            Ok(roots.len() as u32)
        }.await;
        finish(c, result).await
    }
}

#[async_trait]
impl ArtifactObservation for SqliteGitSourceRegistry {
    async fn snapshot(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<ObservationSnapshot> {
        let row:Option<(i64,String)>=sqlx::query_as("SELECT generation,desired_commit FROM branch_observations WHERE workspace=? AND repo=? AND branch=?").bind(workspace).bind(repo).bind(branch).fetch_optional(&self.pool).await?;
        match row {
            Some((generation, commit)) => Ok(ObservationSnapshot::new(
                workspace,
                repo,
                branch,
                Some(checked_u64(generation, "branch generation")?),
                Some(commit),
            )),
            None => Ok(ObservationSnapshot::new(
                workspace, repo, branch, None, None,
            )),
        }
    }

    async fn record_tip_and_intents(
        &self,
        snapshot: &ObservationSnapshot,
        source: &DurableSourceSnapshot,
        kinds: &[ArtifactKind],
        format_version: u32,
        intent: SyncIntent,
    ) -> Result<ArtifactObservationOutcome> {
        if snapshot.workspace() != source.workspace()
            || snapshot.repo() != source.repo()
            || kinds.is_empty()
            || format_version == 0
        {
            bail!("source observation identity is invalid")
        }
        let mut unique = Vec::new();
        for kind in kinds {
            if !unique.contains(kind) {
                unique.push(*kind)
            }
        }
        let mut c = ImmediateTransaction::begin(&self.pool).await?;
        let result:Result<ArtifactObservationOutcome>=async{
            let registered:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions a JOIN git_source_roots r ON r.root_hash=a.root_hash WHERE a.token=? AND a.generation=? AND a.state='registered' AND a.workspace=? AND a.repo=? AND a.commit_oid=? AND a.root_hash=? AND r.state='registered'")
                .bind(source.registration_token()).bind(source.registration_generation() as i64).bind(source.workspace()).bind(source.repo()).bind(source.commit()).bind(source.manifest()).fetch_one(&mut *c).await?;
            if registered!=1{bail!("source snapshot is not an exact registered capability")}
            let current:Option<(i64,String)>=sqlx::query_as("SELECT generation,desired_commit FROM branch_observations WHERE workspace=? AND repo=? AND branch=?").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).fetch_optional(&mut *c).await?;
            let current_generation=current.as_ref().map(|v|checked_u64(v.0,"branch generation")).transpose()?;
            if current_generation!=snapshot.generation(){return Ok(ArtifactObservationOutcome::Stale{current_generation:current_generation.unwrap_or(0)})}
            let same=current.as_ref().is_some_and(|(_,commit)|commit==source.commit());
            let generation=if same{current_generation.context("same-tip branch lacks generation")?}else{current_generation.unwrap_or(0).checked_add(1).context("branch generation overflow")?};
            if !same{
                let old_deferred:Vec<String>=sqlx::query_scalar("SELECT consumer_id FROM artifact_intents WHERE workspace=? AND repo=? AND branch=? AND state='deferred'").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).fetch_all(&mut *c).await?;
                for consumer in old_deferred{sqlx::query("DELETE FROM git_source_consumers WHERE consumer_id=? AND purpose='intent'").bind(consumer).execute(&mut *c).await?;}
                sqlx::query("DELETE FROM artifact_intents WHERE workspace=? AND repo=? AND branch=? AND state='deferred'").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).execute(&mut *c).await?;
                sqlx::query("INSERT INTO branch_source_generations(workspace,repo,branch,generation,commit_oid,source_format_version,root_hash,created_at) VALUES(?,?,?,?,?,?,?,unixepoch())")
                    .bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).bind(SOURCE_FORMAT_VERSION as i64).bind(source.manifest()).execute(&mut *c).await?;
                sqlx::query("INSERT INTO branch_observations(workspace,repo,branch,generation,desired_commit,updated_at) VALUES(?,?,?,?,?,unixepoch()) ON CONFLICT(workspace,repo,branch) DO UPDATE SET generation=excluded.generation,desired_commit=excluded.desired_commit,updated_at=excluded.updated_at")
                    .bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).execute(&mut *c).await?;
                sqlx::query("INSERT INTO branch_source_current(workspace,repo,branch,generation) VALUES(?,?,?,?) ON CONFLICT(workspace,repo,branch) DO UPDATE SET generation=excluded.generation")
                    .bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).execute(&mut *c).await?;
            } else {
                let exact:i64=sqlx::query_scalar("SELECT count(*) FROM branch_source_generations g JOIN branch_source_current c USING(workspace,repo,branch,generation) WHERE g.workspace=? AND g.repo=? AND g.branch=? AND g.generation=? AND g.commit_oid=? AND g.root_hash=?")
                    .bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).bind(source.manifest()).fetch_one(&mut *c).await?;
                if exact!=1{bail!("same-tip source generation differs from registered capability")}
            }
            let mut outcomes=Vec::new();
            for kind in unique{
                if let Some((id,state,artifact_id,consumer))=sqlx::query_as::<_,(i64,String,Option<i64>,String)>("SELECT id,state,artifact_id,consumer_id FROM artifact_intents WHERE workspace=? AND repo=? AND branch=? AND branch_generation=? AND kind=? AND format_version=?")
                    .bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(kind.as_str()).bind(format_version as i64).fetch_optional(&mut *c).await?{
                    if state=="deferred"{outcomes.push((kind,ArtifactIntentOutcome::Deferred(id)));continue}
                    let artifact_id=artifact_id.context("promoted intent lacks artifact")?;
                    outcomes.push((kind,job_outcome(&mut c,artifact_id,intent,self.scheduler_limits.max_manual_retries).await?));
                    let _=consumer;continue
                }
                let consumer_id=format!("{}{}",SOURCE_INTENT_CONSUMER_PREFIX,hex::encode(rand::random::<[u8;24]>()));let session_id=hex::encode(rand::random::<[u8;32]>());
                let existing:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?").bind(snapshot.workspace()).bind(snapshot.repo()).bind(source.commit()).bind(kind.as_str()).bind(format_version as i64).fetch_one(&mut *c).await?;
                let promote=existing==1||capacity_available(&mut c,&self.scheduler_limits,snapshot.workspace(),kind).await?;
                let artifact_id=if promote{Some(ensure_job(&mut c,snapshot.workspace(),snapshot.repo(),source.commit(),kind,format_version as i64).await?)}else{None};
                let state=if promote{"promoted"}else{"deferred"};
                let inserted=sqlx::query("INSERT INTO artifact_intents(workspace,repo,branch,branch_generation,source_root_hash,source_format_version,commit_oid,kind,format_version,state,artifact_id,consumer_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,unixepoch(),unixepoch())")
                    .bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.manifest()).bind(SOURCE_FORMAT_VERSION as i64).bind(source.commit()).bind(kind.as_str()).bind(format_version as i64).bind(state).bind(artifact_id).bind(&consumer_id).execute(&mut *c).await?.last_insert_rowid();
                sqlx::query("INSERT INTO git_source_consumers(root_hash,consumer_id,session_id,workspace,repo,commit_oid,source_format_version,purpose,expires_at) VALUES(?,?,?,?,?,?,?,'intent',?)")
                    .bind(source.manifest()).bind(&consumer_id).bind(session_id).bind(source.workspace()).bind(source.repo()).bind(source.commit()).bind(SOURCE_FORMAT_VERSION as i64).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *c).await?;
                if let Some(artifact_id)=artifact_id{
                    sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?)").bind(artifact_id).bind(&consumer_id).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *c).await?;
                    sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES(?,?,?,?,?,?,?,CASE WHEN (SELECT state FROM artifact_jobs WHERE id=?)='ready' THEN ? ELSE NULL END,?,unixepoch()) ON CONFLICT(workspace,repo,branch,kind) DO UPDATE SET desired_commit=excluded.desired_commit,desired_artifact_id=excluded.desired_artifact_id,desired_generation=excluded.desired_generation,published_artifact_id=CASE WHEN excluded.published_artifact_id IS NOT NULL THEN excluded.published_artifact_id WHEN artifact_observations.format_version=excluded.format_version THEN artifact_observations.published_artifact_id ELSE NULL END,format_version=excluded.format_version,observed_at=excluded.observed_at")
                        .bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(kind.as_str()).bind(source.commit()).bind(artifact_id).bind(generation as i64).bind(artifact_id).bind(artifact_id).bind(format_version as i64).execute(&mut *c).await?;
                    outcomes.push((kind,job_outcome(&mut c,artifact_id,intent,self.scheduler_limits.max_manual_retries).await?));
                }else{outcomes.push((kind,ArtifactIntentOutcome::Deferred(inserted)))}
            }
            Ok(ArtifactObservationOutcome::Recorded{generation,advanced:!same,artifacts:outcomes})
        }.await;
        finish(c, result).await
    }
}

async fn capacity_available(
    c: &mut sqlx::SqliteConnection,
    limits: &SchedulerLimits,
    workspace: &str,
    kind: ArtifactKind,
) -> Result<bool> {
    let total: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')")
            .fetch_one(&mut *c)
            .await?;
    let local: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=?",
    )
    .bind(workspace)
    .fetch_one(&mut *c)
    .await?;
    let lane: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind=?",
    )
    .bind(kind.as_str())
    .fetch_one(&mut *c)
    .await?;
    let expensive:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind IN('full_history','files')").fetch_one(&mut *c).await?;
    let lane_limit = match kind {
        ArtifactKind::Head => limits.head_backlog,
        ArtifactKind::FullHistory => limits.full_history_backlog,
        ArtifactKind::Files => limits.files_backlog,
    };
    Ok(total < limits.total_backlog as i64
        && local < limits.workspace_backlog as i64
        && lane < lane_limit as i64
        && (!matches!(kind, ArtifactKind::FullHistory | ArtifactKind::Files)
            || expensive < limits.total_backlog.saturating_sub(limits.head_reserved) as i64))
}

async fn ensure_job(
    c: &mut sqlx::SqliteConnection,
    workspace: &str,
    repo: &str,
    commit: &str,
    kind: ArtifactKind,
    format: i64,
) -> Result<i64> {
    if let Some(id)=sqlx::query_scalar::<_,i64>("SELECT id FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?").bind(workspace).bind(repo).bind(commit).bind(kind.as_str()).bind(format).fetch_optional(&mut *c).await?{return Ok(id)}
    Ok(sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES(?,?,?,?,?,'queued',unixepoch(),unixepoch())").bind(workspace).bind(repo).bind(commit).bind(kind.as_str()).bind(format).execute(&mut *c).await?.last_insert_rowid())
}

async fn job_outcome(
    c: &mut sqlx::SqliteConnection,
    id: i64,
    intent: SyncIntent,
    max_retries: u32,
) -> Result<ArtifactIntentOutcome> {
    let row = sqlx::query("SELECT state,failure_class,retry_count FROM artifact_jobs WHERE id=?")
        .bind(id)
        .fetch_one(&mut *c)
        .await?;
    let mut state: String = row.try_get("state")?;
    let class = row
        .try_get::<Option<String>, _>("failure_class")?
        .map(|v| FailureClass::parse(&v))
        .transpose()?;
    let retries = checked_u32(row.try_get("retry_count")?, "artifact retry count")?;
    if state == "failed"
        && intent == SyncIntent::EnsureCurrent
        && class == Some(FailureClass::Retryable)
        && retries < max_retries
        && sqlx::query("UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,manifest=NULL,error=NULL,failure_class=NULL,retry_count=retry_count+1,updated_at=unixepoch() WHERE id=? AND state='failed' AND failure_class='retryable' AND retry_count=?")
            .bind(id).bind(retries as i64).execute(&mut *c).await?.rows_affected()==1
    {
        state = "queued".into()
    }
    Ok(match state.as_str() {
        "ready" => ArtifactIntentOutcome::Ready(id),
        "failed" => ArtifactIntentOutcome::Failed(id, class.unwrap_or(FailureClass::Permanent)),
        "queued" | "running" => ArtifactIntentOutcome::Subscribed(id),
        _ => bail!("artifact job state is invalid"),
    })
}

pub(crate) async fn migrate_sqlite_v7_in(c: &mut sqlx::SqliteConnection) -> Result<()> {
    let planted:i64=sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name LIKE 'git_source_%' OR name LIKE 'branch_source_%' OR name LIKE 'artifact_intents%' OR tbl_name IN('git_source_roots','git_source_members','git_source_acquisition_sequence','git_source_acquisitions','git_source_acquisition_members','git_source_desires','branch_source_generations','branch_source_current','git_source_consumers','artifact_intents','git_source_maintenance')").fetch_one(&mut *c).await?;
    if planted != 0 {
        bail!("partial or planted Git source registry schema detected")
    }
    sqlx::raw_sql(SQLITE_V7_SCHEMA).execute(&mut *c).await?;
    validate_sqlite_v7_in(c).await
}

pub(crate) async fn validate_sqlite_v7_in(c: &mut sqlx::SqliteConnection) -> Result<()> {
    let expected = expected_sqlite_v7_ddl()?;
    let actual_rows:Vec<(String,String)>=sqlx::query_as("SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL AND (name LIKE 'git_source_%' OR name LIKE 'branch_source_%' OR name LIKE 'artifact_intents%' OR tbl_name IN('git_source_roots','git_source_members','git_source_acquisition_sequence','git_source_acquisitions','git_source_acquisition_members','git_source_desires','branch_source_generations','branch_source_current','git_source_consumers','artifact_intents','git_source_maintenance')) ORDER BY name").fetch_all(&mut *c).await?;
    let actual = actual_rows
        .into_iter()
        .map(|(name, sql)| (name, canonical_ddl(&sql)))
        .collect::<BTreeMap<_, _>>();
    if actual != expected {
        bail!("Git source registry DDL inventory or definition is not exact")
    }
    let fk = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut *c)
        .await?;
    if fk.iter().any(|row| {
        let table: String = row.get("table");
        table.starts_with("git_source_")
            || table.starts_with("branch_source_")
            || table == "artifact_intents"
    }) {
        bail!("Git source registry foreign keys are invalid")
    }
    let singleton: Vec<(i64, i64)> =
        sqlx::query_as("SELECT id,generation FROM git_source_acquisition_sequence")
            .fetch_all(&mut *c)
            .await?;
    let max: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(generation),0) FROM git_source_acquisitions")
            .fetch_one(&mut *c)
            .await?;
    if singleton.len() != 1 || singleton[0].0 != 1 || singleton[0].1 < max {
        bail!("Git source acquisition sequence is invalid")
    }
    let operation_rows:Vec<(i64,String,String,String,String,String)>=sqlx::query_as("SELECT generation,workspace,repo,commit_oid,attempt_id,operation_id FROM git_source_acquisitions").fetch_all(&mut *c).await?;
    if operation_rows
        .iter()
        .any(|(generation, workspace, repo, commit, attempt, stored)| {
            stored != &operation_id(workspace, repo, commit, attempt, *generation)
        })
    {
        bail!("Git source acquisition operation provenance is invalid")
    }
    let bad:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions a WHERE (a.state='held' AND EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token)) OR (a.state IN('graph_published','activation_unknown','registered') AND (a.root_hash IS NULL OR NOT EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token))) OR EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token GROUP BY m.token HAVING MIN(m.ordinal)<>0 OR MAX(m.ordinal)+1<>count(*) OR count(*)%2<>0 OR SUM(CASE WHEN (m.ordinal%2=0 AND m.kind='pack') OR (m.ordinal%2=1 AND m.kind='index') THEN 0 ELSE 1 END)<>0 OR SUM(m.child_len)<>a.total_bytes)").fetch_one(&mut *c).await?;
    if bad != 0 {
        bail!("Git source provisional graphs are invalid")
    }
    let invalid_roots:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_roots r WHERE length(r.root_hash)<>64 OR r.root_hash GLOB '*[^0-9a-f]*' OR length(r.semantic_digest)<>64 OR r.semantic_digest GLOB '*[^0-9a-f]*' OR length(r.object_set_digest)<>64 OR r.object_set_digest GLOB '*[^0-9a-f]*' OR NOT EXISTS(SELECT 1 FROM git_source_members m WHERE m.root_hash=r.root_hash GROUP BY m.root_hash HAVING MIN(m.ordinal)=0 AND MAX(m.ordinal)+1=count(*) AND count(*)%2=0 AND SUM(m.child_len)=r.total_bytes AND SUM(CASE WHEN (m.ordinal%2=0 AND m.kind='pack') OR (m.ordinal%2=1 AND m.kind='index') THEN 0 ELSE 1 END)=0) OR EXISTS(SELECT 1 FROM git_source_members m WHERE m.root_hash=r.root_hash AND (length(m.child_hash)<>64 OR m.child_hash GLOB '*[^0-9a-f]*'))").fetch_one(&mut *c).await?;
    let invalid_provisional:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions a WHERE length(a.token)<>64 OR a.token GLOB '*[^0-9a-f]*' OR (a.root_hash IS NOT NULL AND (length(a.root_hash)<>64 OR a.root_hash GLOB '*[^0-9a-f]*')) OR (a.semantic_digest IS NOT NULL AND (length(a.semantic_digest)<>64 OR a.semantic_digest GLOB '*[^0-9a-f]*')) OR (a.object_set_digest IS NOT NULL AND (length(a.object_set_digest)<>64 OR a.object_set_digest GLOB '*[^0-9a-f]*')) OR EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token AND (length(m.child_hash)<>64 OR m.child_hash GLOB '*[^0-9a-f]*'))").fetch_one(&mut *c).await?;
    let invalid_registered_acquisitions:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions a LEFT JOIN git_source_roots r ON r.root_hash=a.root_hash WHERE a.state='registered' AND (r.root_hash IS NULL OR r.state<>'registered' OR r.root_len<>a.root_len OR r.workspace<>a.workspace OR r.repo<>a.repo OR r.commit_oid<>a.commit_oid OR r.source_format_version<>a.source_format_version OR r.object_format<>a.object_format OR r.semantic_digest<>a.semantic_digest OR r.object_set_digest<>a.object_set_digest OR r.object_count<>a.object_count OR r.total_bytes<>a.total_bytes OR r.registration_operation<>a.operation_id OR r.registration_generation<>a.generation OR EXISTS(SELECT 1 FROM git_source_acquisition_members am LEFT JOIN git_source_members m ON m.root_hash=r.root_hash AND m.ordinal=am.ordinal WHERE am.token=a.token AND (m.ordinal IS NULL OR m.child_hash<>am.child_hash OR m.child_len<>am.child_len OR m.kind<>am.kind)) OR EXISTS(SELECT 1 FROM git_source_members m LEFT JOIN git_source_acquisition_members am ON am.token=a.token AND am.ordinal=m.ordinal WHERE m.root_hash=r.root_hash AND am.ordinal IS NULL))").fetch_one(&mut *c).await?;
    let invalid_registered_roots:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_roots r WHERE r.state='registered' AND NOT EXISTS(SELECT 1 FROM git_source_acquisitions a WHERE a.state='registered' AND a.root_hash=r.root_hash AND a.root_len=r.root_len AND a.workspace=r.workspace AND a.repo=r.repo AND a.commit_oid=r.commit_oid AND a.source_format_version=r.source_format_version AND a.object_format=r.object_format AND a.semantic_digest=r.semantic_digest AND a.object_set_digest=r.object_set_digest AND a.object_count=r.object_count AND a.total_bytes=r.total_bytes AND a.operation_id=r.registration_operation AND a.generation=r.registration_generation)").fetch_one(&mut *c).await?;
    let conflicting_graph_descriptors:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT hash,count(DISTINCT len||':'||kind) variants FROM (SELECT root_hash hash,root_len len,'root' kind FROM git_source_roots UNION ALL SELECT root_hash,root_len,'root' FROM git_source_acquisitions WHERE root_hash IS NOT NULL UNION ALL SELECT child_hash,child_len,kind FROM git_source_members UNION ALL SELECT child_hash,child_len,kind FROM git_source_acquisition_members) GROUP BY hash HAVING variants<>1)").fetch_one(&mut *c).await?;
    let root_child_aliases:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT root_hash hash FROM git_source_roots UNION SELECT root_hash FROM git_source_acquisitions WHERE root_hash IS NOT NULL) r JOIN (SELECT child_hash hash FROM git_source_members UNION SELECT child_hash FROM git_source_acquisition_members) m USING(hash)").fetch_one(&mut *c).await?;
    let invalid_desires:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_desires d LEFT JOIN git_source_acquisitions a ON a.token=d.acquisition_token LEFT JOIN git_source_roots r ON r.root_hash=d.root_hash WHERE d.source_format_version<>1 OR (d.state='acquiring' AND (a.token IS NULL OR a.workspace<>d.workspace OR a.repo<>d.repo OR a.commit_oid<>d.commit_oid OR a.source_format_version<>d.source_format_version OR a.state NOT IN('held','graph_published','activation_unknown'))) OR (d.state='registered' AND (r.root_hash IS NULL OR r.workspace<>d.workspace OR r.repo<>d.repo OR r.commit_oid<>d.commit_oid OR r.source_format_version<>d.source_format_version OR r.state<>'registered'))").fetch_one(&mut *c).await?;
    let orphan_acquisition_desires:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions a WHERE (a.state IN('held','graph_published','activation_unknown') AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='acquiring' AND d.acquisition_token=a.token AND d.workspace=a.workspace AND d.repo=a.repo AND d.commit_oid=a.commit_oid AND d.source_format_version=a.source_format_version)) OR (a.state='registered' AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='registered' AND d.acquisition_token IS NULL AND d.root_hash=a.root_hash AND d.workspace=a.workspace AND d.repo=a.repo AND d.commit_oid=a.commit_oid AND d.source_format_version=a.source_format_version))").fetch_one(&mut *c).await?;
    let orphan_registered_root_desires:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_roots r WHERE r.state='registered' AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='registered' AND d.root_hash=r.root_hash AND d.workspace=r.workspace AND d.repo=r.repo AND d.commit_oid=r.commit_oid AND d.source_format_version=r.source_format_version)").fetch_one(&mut *c).await?;
    let invalid_branches:i64=sqlx::query_scalar("SELECT count(*) FROM branch_source_current c JOIN branch_source_generations g USING(workspace,repo,branch,generation) LEFT JOIN branch_observations b USING(workspace,repo,branch) WHERE b.workspace IS NULL OR b.generation<>g.generation OR b.desired_commit<>g.commit_oid OR NOT EXISTS(SELECT 1 FROM git_source_roots r JOIN git_source_desires d ON d.root_hash=r.root_hash AND d.workspace=r.workspace AND d.repo=r.repo AND d.commit_oid=r.commit_oid AND d.source_format_version=r.source_format_version WHERE r.root_hash=g.root_hash AND r.workspace=g.workspace AND r.repo=g.repo AND r.commit_oid=g.commit_oid AND r.source_format_version=g.source_format_version AND r.state='registered' AND d.state='registered')").fetch_one(&mut *c).await?;
    let invalid_intents:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_intents i JOIN branch_source_generations g ON g.workspace=i.workspace AND g.repo=i.repo AND g.branch=i.branch AND g.generation=i.branch_generation LEFT JOIN git_source_consumers c ON c.root_hash=i.source_root_hash AND c.consumer_id=i.consumer_id LEFT JOIN artifact_jobs j ON j.id=i.artifact_id LEFT JOIN git_source_desires d ON d.workspace=i.workspace AND d.repo=i.repo AND d.commit_oid=i.commit_oid WHERE length(i.consumer_id)<>55 OR substr(i.consumer_id,1,7)<>'intent:' OR substr(i.consumer_id,8) GLOB '*[^0-9a-f]*' OR (SELECT count(*) FROM artifact_intents sibling WHERE sibling.consumer_id=i.consumer_id)<>1 OR g.root_hash<>i.source_root_hash OR g.commit_oid<>i.commit_oid OR d.workspace IS NULL OR d.source_format_version<>i.source_format_version OR d.state<>'registered' OR d.root_hash<>i.source_root_hash OR c.consumer_id IS NULL OR length(c.session_id)<>64 OR c.session_id GLOB '*[^0-9a-f]*' OR c.workspace<>i.workspace OR c.repo<>i.repo OR c.commit_oid<>i.commit_oid OR c.source_format_version<>i.source_format_version OR c.purpose<>'intent' OR c.expires_at<>9223372036854775807 OR (i.state='promoted' AND (j.id IS NULL OR j.workspace<>i.workspace OR j.repo<>i.repo OR j.commit_oid<>i.commit_oid OR j.kind<>i.kind OR j.format_version<>i.format_version))").fetch_one(&mut *c).await?;
    // Source intents are durable lifecycle roots, not expiring client sessions.
    // Their source and artifact claims therefore remain paired at i64::MAX
    // until terminal intent settlement deletes both in one transaction.
    let invalid_intent_artifact_consumers:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_intents i WHERE (i.state='deferred' AND EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id)) OR (i.state='promoted' AND ((SELECT count(*) FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id)<>1 OR NOT EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id AND ac.artifact_id=i.artifact_id AND ac.expires_at=9223372036854775807)))").fetch_one(&mut *c).await?;
    let orphan_intent_consumers:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_consumers c WHERE c.purpose='intent' AND (length(c.consumer_id)<>55 OR substr(c.consumer_id,1,7)<>'intent:' OR substr(c.consumer_id,8) GLOB '*[^0-9a-f]*' OR length(c.session_id)<>64 OR c.session_id GLOB '*[^0-9a-f]*' OR c.expires_at<>9223372036854775807 OR NOT EXISTS(SELECT 1 FROM artifact_intents i WHERE i.consumer_id=c.consumer_id AND i.source_root_hash=c.root_hash AND i.workspace=c.workspace AND i.repo=c.repo AND i.commit_oid=c.commit_oid AND i.source_format_version=c.source_format_version))").fetch_one(&mut *c).await?;
    // Non-prefixed rows are valid scheduler consumers. The reserved namespace
    // gives us enough provenance to reject a completely orphaned intent claim.
    let orphan_intent_artifact_consumers:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_consumers ac WHERE substr(ac.consumer_id,1,7)='intent:' AND (ac.expires_at<>9223372036854775807 OR (SELECT count(*) FROM artifact_intents i WHERE i.state='promoted' AND i.consumer_id=ac.consumer_id AND i.artifact_id=ac.artifact_id)<>1)").fetch_one(&mut *c).await?;
    let invalid_maintenance:i64=sqlx::query_scalar("SELECT CASE WHEN count(*)<>1 THEN 1 ELSE COALESCE(MAX(CASE WHEN id<>1 OR intent_cursor<0 OR acquisition_cursor<0 OR (root_cursor<>'' AND (length(root_cursor)<>64 OR root_cursor GLOB '*[^0-9a-f]*')) OR (config_fingerprint<>'' AND (length(config_fingerprint)<>64 OR config_fingerprint GLOB '*[^0-9a-f]*')) THEN 1 ELSE 0 END),1) END FROM git_source_maintenance").fetch_one(&mut *c).await?;
    if invalid_roots
        + invalid_provisional
        + invalid_registered_acquisitions
        + invalid_registered_roots
        + conflicting_graph_descriptors
        + root_child_aliases
        + invalid_desires
        + orphan_acquisition_desires
        + orphan_registered_root_desires
        + invalid_branches
        + invalid_intents
        + invalid_intent_artifact_consumers
        + orphan_intent_consumers
        + orphan_intent_artifact_consumers
        + invalid_maintenance
        != 0
    {
        bail!("Git source registry relational state is invalid")
    }
    Ok(())
}

fn expected_sqlite_v7_ddl() -> Result<BTreeMap<String, String>> {
    let mut expected = BTreeMap::new();
    for statement in SQLITE_V7_SCHEMA.split(';').map(str::trim) {
        let upper = statement.to_ascii_uppercase();
        if !(upper.starts_with("CREATE TABLE ")
            || upper.starts_with("CREATE INDEX ")
            || upper.starts_with("CREATE UNIQUE INDEX "))
        {
            continue;
        }
        let prefix = if upper.starts_with("CREATE UNIQUE INDEX ") {
            "CREATE UNIQUE INDEX ".len()
        } else if upper.starts_with("CREATE INDEX ") {
            "CREATE INDEX ".len()
        } else {
            "CREATE TABLE ".len()
        };
        let rest = &statement[prefix..];
        let end = rest
            .find(|c: char| c == '(' || c.is_whitespace())
            .context("canonical v7 DDL object lacks name")?;
        let name = rest[..end].to_owned();
        if expected.insert(name, canonical_ddl(statement)).is_some() {
            bail!("canonical v7 DDL repeats an object")
        }
    }
    Ok(expected)
}

fn canonical_ddl(sql: &str) -> String {
    sql.chars()
        .filter(|c| !c.is_whitespace() && *c != '`' && *c != '\"')
        .flat_map(char::to_lowercase)
        .collect()
}

async fn assert_exact_graph(
    c: &mut sqlx::SqliteConnection,
    a: &GitSourceAcquisition,
    v: &GitSourceRegistryView,
    now: i64,
) -> Result<()> {
    let found:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions WHERE token=? AND generation=? AND operation_id=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND root_hash=? AND root_len=? AND state='graph_published' AND expires_at>?")
        .bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).bind(&v.workspace).bind(&v.repo).bind(&v.commit).bind(v.source_format_version as i64).bind(&v.root.hash).bind(checked_i64(v.root.len,"root length")?).bind(now).fetch_one(&mut *c).await?;
    if found != 1 {
        bail!("source acquisition is stale or mismatched")
    }
    let members:Vec<(i64,String,i64,String)>=sqlx::query_as("SELECT ordinal,child_hash,child_len,kind FROM git_source_acquisition_members WHERE token=? ORDER BY ordinal").bind(&a.token).fetch_all(&mut *c).await?;
    if members.len() != v.members.len()
        || members.iter().zip(&v.members).any(|(x, y)| {
            x.0 != y.ordinal as i64
                || x.1 != y.blob.hash
                || x.2 != y.blob.len as i64
                || x.3 != y.kind
        })
    {
        bail!("source acquisition graph changed")
    }
    Ok(())
}

fn verify_acquisition_identity(a: &GitSourceAcquisition, v: &GitSourceRegistryView) -> Result<()> {
    if a.workspace != v.workspace
        || a.repo != v.repo
        || a.commit != v.commit
        || a.source_format_version != v.source_format_version
        || a.root != v.root
    {
        bail!("prepared source does not match acquisition")
    };
    Ok(())
}
#[allow(clippy::too_many_arguments)]
fn validate_acquire_identity(
    workspace: &str,
    repo: &str,
    commit: &str,
    source_format_version: u32,
    owner: &str,
    attempt: &str,
    ttl: i64,
) -> Result<()> {
    if workspace.trim().is_empty()
        || repo.trim().is_empty()
        || workspace.len() > 1024
        || repo.len() > 1024
        || workspace.chars().any(char::is_control)
        || repo.chars().any(char::is_control)
        || source_format_version != SOURCE_FORMAT_VERSION
        || owner.trim().is_empty()
        || attempt.trim().is_empty()
        || !(1..=3600).contains(&ttl)
    {
        bail!("source acquisition identity or TTL is invalid")
    };
    crate::artifact_scheduler::validate_canonical_commit_oid(commit)
}
async fn finish<T>(c: ImmediateTransaction, result: Result<T>) -> Result<T> {
    finish_with_commit_fault(c, result, TransactionCommitFault::None).await
}

async fn finish_with_commit_fault<T>(
    mut c: ImmediateTransaction,
    result: Result<T>,
    commit_fault: TransactionCommitFault,
) -> Result<T> {
    match result {
        Ok(v) => {
            if commit_fault == TransactionCommitFault::FailWithOpenTransaction {
                bail!("injected transaction COMMIT failure with an open transaction")
            }
            match sqlx::query("COMMIT").execute(&mut *c).await {
                Ok(_) => {
                    c.release();
                    Ok(v)
                }
                Err(error) => Err(error).context("commit registry transaction; connection retired"),
            }
        }
        Err(error) => match sqlx::query("ROLLBACK").execute(&mut *c).await {
            Ok(_) => {
                c.release();
                Err(error)
            }
            Err(rollback) => Err(error).context(format!(
                "rollback registry transaction failed; connection retired: {rollback}"
            )),
        },
    }
}
fn checked_i64(v: u64, label: &str) -> Result<i64> {
    i64::try_from(v).with_context(|| format!("{label} exceeds database range"))
}
fn checked_u64(v: i64, label: &str) -> Result<u64> {
    u64::try_from(v).with_context(|| format!("{label} is negative"))
}
fn checked_u32(v: i64, label: &str) -> Result<u32> {
    u32::try_from(v).with_context(|| format!("{label} exceeds range"))
}
fn operation_id(
    workspace: &str,
    repo: &str,
    commit: &str,
    attempt: &str,
    generation: i64,
) -> String {
    let mut h = Sha256::new();
    for v in [workspace, repo, commit, attempt] {
        h.update((v.len() as u64).to_be_bytes());
        h.update(v.as_bytes());
    }
    h.update(generation.to_be_bytes());
    hex::encode(h.finalize())
}
fn parse_object_format(v: &str) -> Result<GitObjectFormat> {
    match v {
        "sha1" => Ok(GitObjectFormat::Sha1),
        "sha256" => Ok(GitObjectFormat::Sha256),
        _ => bail!("registered object format is invalid"),
    }
}
fn evidence_mac(
    seal: &[u8; 32],
    root: &CasBlob,
    workspace: &str,
    repo: &str,
    commit: &str,
    format: GitObjectFormat,
    generation: i64,
    operation: &str,
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(seal);
    for v in [root.hash.as_str(), workspace, repo, commit, operation] {
        h.update((v.len() as u64).to_be_bytes());
        h.update(v.as_bytes());
    }
    let format = match format {
        GitObjectFormat::Sha1 => "sha1",
        GitObjectFormat::Sha256 => "sha256",
    };
    h.update((format.len() as u64).to_be_bytes());
    h.update(format.as_bytes());
    h.update(root.len.to_be_bytes());
    h.update(generation.to_be_bytes());
    h.finalize().into()
}
fn verify_storage_graph(
    storage: &StorageRef,
    blobs: &[CasBlob],
    root_hash: &str,
    root_bytes: &[u8],
    cancelled: &CancellationToken,
) -> Result<()> {
    for blob in blobs {
        if cancelled.is_cancelled() {
            bail!("source object verification cancelled")
        }
        match storage.stat_object(&blob.hash)? {
            StorageObjectStat::Present(len) if len == blob.len => {}
            StorageObjectStat::Present(_) => bail!("source object length mismatch"),
            StorageObjectStat::Missing => bail!("source object missing"),
        };
        let mut digest = Sha256::new();
        let mut offset = 0u64;
        while offset < blob.len {
            if cancelled.is_cancelled() {
                bail!("source object verification cancelled")
            }
            let length = (blob.len - offset).min(1024 * 1024);
            let bytes = storage.get_range(&blob.hash, offset, length)?;
            if bytes.len() as u64 != length {
                bail!("source object range read was short")
            }
            digest.update(&bytes);
            offset += length;
        }
        if hex::encode(digest.finalize()) != blob.hash {
            bail!("source object content mismatch")
        }
    }
    let mut durable_root = Vec::with_capacity(root_bytes.len());
    let mut offset = 0u64;
    while offset < root_bytes.len() as u64 {
        if cancelled.is_cancelled() {
            bail!("source root verification cancelled")
        }
        let length = (root_bytes.len() as u64 - offset).min(1024 * 1024);
        let bytes = storage.get_range(root_hash, offset, length)?;
        if bytes.len() as u64 != length {
            bail!("source root range read was short")
        }
        durable_root.extend_from_slice(&bytes);
        offset += length;
    }
    if hex::encode(Sha256::digest(root_bytes)) != root_hash || durable_root != root_bytes {
        bail!("source root is not canonical")
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::ArtifactScheduler;
    use crate::git_source::{
        CasGitSourceStore, GitSourceLoader, GitSourceMaterializer, GitSourcePackager,
        GitSourceUploader, prepared_source_for_registry_test,
    };
    use crate::storage::LocalStorage;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone, Default)]
    struct SlowUploader {
        cancelled: Arc<AtomicBool>,
    }
    impl GitSourceUploader for SlowUploader {
        fn put_file(
            &self,
            blob: &CasBlob,
            source: &Path,
            cancel: &CancellationToken,
        ) -> Result<()> {
            for _ in 0..100 {
                if cancel.is_cancelled() {
                    self.cancelled.store(true, Ordering::SeqCst);
                    bail!("cancelled slow upload")
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            let bytes = std::fs::read(source)?;
            if bytes.len() as u64 != blob.len || hex::encode(Sha256::digest(&bytes)) != blob.hash {
                bail!("slow upload input mismatch")
            }
            Ok(())
        }
        fn put_bytes(
            &self,
            blob: &CasBlob,
            bytes: &[u8],
            cancel: &CancellationToken,
        ) -> Result<()> {
            if cancel.is_cancelled() {
                self.cancelled.store(true, Ordering::SeqCst);
                bail!("cancelled slow root upload")
            }
            if bytes.len() as u64 != blob.len || hex::encode(Sha256::digest(bytes)) != blob.hash {
                bail!("slow root mismatch")
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    struct BlockingLoader {
        inner: CasGitSourceStore,
        started: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
    }

    impl GitSourceLoader for BlockingLoader {
        fn load_file(
            &self,
            _blob: &CasBlob,
            _destination: &Path,
            cancel: &CancellationToken,
        ) -> Result<()> {
            self.started.store(true, Ordering::SeqCst);
            loop {
                if cancel.is_cancelled() {
                    self.cancelled.store(true, Ordering::SeqCst);
                    bail!("cancelled blocking source load")
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }

        fn load_bytes(
            &self,
            blob: &CasBlob,
            maximum: u64,
            cancel: &CancellationToken,
        ) -> Result<Vec<u8>> {
            self.inner.load_bytes(blob, maximum, cancel)
        }
    }

    async fn fixture() -> (
        ArtifactScheduler,
        SqliteGitSourceRegistry,
        SqlitePool,
        tempfile::TempDir,
    ) {
        fixture_with_limits(SchedulerLimits::default()).await
    }

    async fn fixture_with_limits(
        limits: SchedulerLimits,
    ) -> (
        ArtifactScheduler,
        SqliteGitSourceRegistry,
        SqlitePool,
        tempfile::TempDir,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("registry.db");
        let scheduler = ArtifactScheduler::open(database.to_str().unwrap(), limits.clone())
            .await
            .unwrap();
        let pool = SqlitePool::connect(&format!("sqlite://{}", database.display()))
            .await
            .unwrap();
        let storage: StorageRef = Arc::new(LocalStorage::new(temp.path().join("objects")).unwrap());
        let registry = SqliteGitSourceRegistry::new(
            pool.clone(),
            storage,
            limits,
            GitSourceLimits::default(),
            [7; 32],
        )
        .await
        .unwrap();
        (scheduler, registry, pool, temp)
    }

    fn prepared(registry: &SqliteGitSourceRegistry, commit: &str) -> PreparedGitSource {
        prepared_in(registry, "ws", "o/r", commit)
    }

    fn prepared_in(
        registry: &SqliteGitSourceRegistry,
        workspace: &str,
        repo: &str,
        commit: &str,
    ) -> PreparedGitSource {
        let pack_bytes = b"pack";
        let index_bytes = b"index";
        let pack = CasBlob {
            hash: hex::encode(Sha256::digest(pack_bytes)),
            len: pack_bytes.len() as u64,
        };
        let index = CasBlob {
            hash: hex::encode(Sha256::digest(index_bytes)),
            len: index_bytes.len() as u64,
        };
        registry.storage.put(&pack.hash, pack_bytes).unwrap();
        registry.storage.put(&index.hash, index_bytes).unwrap();
        let prepared =
            prepared_source_for_registry_test(workspace, repo, commit, pack, index).unwrap();
        let view = prepared.registry_view(&GitSourceLimits::default()).unwrap();
        registry
            .storage
            .put(&view.root.hash, &view.root_bytes)
            .unwrap();
        prepared
    }

    async fn registered_source(
        registry: &SqliteGitSourceRegistry,
        commit: &str,
    ) -> DurableSourceSnapshot {
        let source = prepared(registry, commit);
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected source acquisition"),
        };
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap()
    }

    async fn promoted_head_intent(
        registry: &SqliteGitSourceRegistry,
        pool: &SqlitePool,
        commit: &str,
    ) -> (i64, String) {
        let source = registered_source(registry, commit).await;
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let outcome = registry
            .record_tip_and_intents(
                &before,
                &source,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            ArtifactObservationOutcome::Recorded { ref artifacts, .. }
                if matches!(artifacts[0].1, ArtifactIntentOutcome::Subscribed(_))
        ));
        sqlx::query_as(
            "SELECT artifact_id,consumer_id FROM artifact_intents WHERE state='promoted'",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn assert_registry_validation_passes(pool: &SqlitePool) {
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        validate_sqlite_v7_in(&mut connection).await.unwrap();
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
    }

    async fn assert_registry_validation_fails(pool: &SqlitePool) {
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert!(validate_sqlite_v7_in(&mut connection).await.is_err());
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn provisional_graph_coalesces_and_fences_gc_then_registers() {
        let (scheduler, registry, _pool, _temp) = fixture().await;
        let source = prepared(&registry, &"a".repeat(40));
        let (acquisition, _permit) = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::ObserveMovement)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired {
                acquisition,
                permit,
            } => (acquisition, permit),
            _ => panic!("expected acquisition"),
        };
        assert!(matches!(
            registry
                .protect_prepared(
                    &source,
                    "other",
                    "attempt-2",
                    60,
                    SyncIntent::ObserveMovement
                )
                .await
                .unwrap(),
            SourceAcquireOutcome::Deferred { .. }
        ));
        assert!(scheduler.acquire_gc_sweep("gc", 60).await.unwrap());
        let page = scheduler.live_source_objects_page(None, 16).await.unwrap();
        assert_eq!(page.len(), 3);
        scheduler.release_gc_sweep("gc").await.unwrap();
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(snapshot.commit(), "a".repeat(40));
        assert!(matches!(
            registry
                .protect_prepared(
                    &source,
                    "owner",
                    "attempt-3",
                    60,
                    SyncIntent::ObserveMovement
                )
                .await
                .unwrap(),
            SourceAcquireOutcome::Ready(_)
        ));
    }

    #[tokio::test]
    async fn identity_first_ready_path_requires_no_prepared_graph() {
        let (_scheduler, registry, _pool, _temp) = fixture().await;
        let commit = "c".repeat(40);
        let source = prepared(&registry, &commit);
        let acquisition = match registry
            .protect_prepared(&source, "owner", "initial", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    &commit,
                    SOURCE_FORMAT_VERSION,
                    "other",
                    "no-prepare",
                    60,
                    SyncIntent::EnsureCurrent,
                )
                .await
                .unwrap(),
            SourceBeginOutcome::Ready(_)
        ));
    }

    #[tokio::test]
    async fn failed_begin_commit_retires_connection_and_rolls_back_all_writes() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "8".repeat(40);
        assert!(
            registry
                .begin_acquisition_with_commit_fault(
                    "ws",
                    "o/r",
                    &commit,
                    SOURCE_FORMAT_VERSION,
                    "owner",
                    "attempt",
                    60,
                    SyncIntent::EnsureCurrent,
                    TransactionCommitFault::FailWithOpenTransaction,
                )
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT generation FROM git_source_acquisition_sequence WHERE id=1"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_acquisitions")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_desires")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    &commit,
                    SOURCE_FORMAT_VERSION,
                    "owner",
                    "attempt",
                    60,
                    SyncIntent::EnsureCurrent,
                )
                .await
                .unwrap(),
            SourceBeginOutcome::PermitToPrepare(_)
        ));
        let mut fresh = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *fresh)
            .await
            .unwrap();
        sqlx::query("ROLLBACK").execute(&mut *fresh).await.unwrap();
    }

    #[tokio::test]
    async fn dropped_inflight_begin_bind_and_settlement_never_contaminate_pool() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "7".repeat(40);
        let mut begin = ImmediateTransaction::begin(&pool).await.unwrap();
        assert!(matches!(
            registry
                .begin_in(
                    &mut begin,
                    "ws",
                    "o/r",
                    &commit,
                    1,
                    "owner",
                    "attempt",
                    60,
                    SyncIntent::EnsureCurrent
                )
                .await
                .unwrap(),
            SourceBeginOutcome::PermitToPrepare(_)
        ));
        drop(begin);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_acquisitions")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );

        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                1,
                "owner",
                "attempt",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(v) => v,
            _ => panic!(),
        };
        let source = prepared(&registry, &commit);
        let view = source.registry_view(&GitSourceLimits::default()).unwrap();
        let mut bind = ImmediateTransaction::begin(&pool).await.unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET root_hash=?,root_len=?,object_format=?,semantic_digest=?,object_set_digest=?,object_count=?,total_bytes=?,state='graph_published' WHERE token=? AND state='held'")
            .bind(&view.root.hash).bind(view.root.len as i64).bind(view.object_format).bind(&view.semantic_digest).bind(&view.object_set_digest).bind(view.object_count as i64).bind(view.total_bytes as i64).bind(&permit.token).execute(&mut *bind).await.unwrap();
        sqlx::query("INSERT INTO git_source_acquisition_members(token,ordinal,child_hash,child_len,kind) VALUES(?,0,?,?,'pack')")
            .bind(&permit.token).bind(&view.members[0].blob.hash).bind(view.members[0].blob.len as i64).execute(&mut *bind).await.unwrap();
        drop(bind);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM git_source_acquisitions WHERE token=?"
            )
            .bind(&permit.token)
            .fetch_one(&pool)
            .await
            .unwrap(),
            "held"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM git_source_acquisition_members WHERE token=?"
            )
            .bind(&permit.token)
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );
        let (acquisition, _) = registry
            .bind_prepared_graph(&permit, &source)
            .await
            .unwrap();

        let mut settlement = ImmediateTransaction::begin(&pool).await.unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET state='failed',failure_class='retryable' WHERE token=?")
            .bind(&permit.token).execute(&mut *settlement).await.unwrap();
        sqlx::query("UPDATE git_source_desires SET state='failed',failure_class='retryable',acquisition_token=NULL WHERE acquisition_token=?")
            .bind(&permit.token).execute(&mut *settlement).await.unwrap();
        drop(settlement);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM git_source_acquisitions WHERE token=?"
            )
            .bind(&permit.token)
            .fetch_one(&pool)
            .await
            .unwrap(),
            "graph_published"
        );
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let mut fresh = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *fresh)
            .await
            .unwrap();
        sqlx::query("ROLLBACK").execute(&mut *fresh).await.unwrap();
    }

    #[tokio::test]
    async fn independent_registries_dedupe_before_preparation() {
        let (_scheduler, first, _pool, temp) = fixture().await;
        let second_pool = SqlitePool::connect(&format!(
            "sqlite://{}",
            temp.path().join("registry.db").display()
        ))
        .await
        .unwrap();
        let second = SqliteGitSourceRegistry::new(
            second_pool,
            Arc::new(LocalStorage::new(temp.path().join("objects")).unwrap()),
            SchedulerLimits::default(),
            GitSourceLimits::default(),
            [7; 32],
        )
        .await
        .unwrap();
        let commit = "d".repeat(40);
        let first_begin = first.begin_acquisition(
            "ws",
            "o/r",
            &commit,
            SOURCE_FORMAT_VERSION,
            "one",
            "one",
            60,
            SyncIntent::EnsureCurrent,
        );
        let second_begin = second.begin_acquisition(
            "ws",
            "o/r",
            &commit,
            SOURCE_FORMAT_VERSION,
            "two",
            "two",
            60,
            SyncIntent::EnsureCurrent,
        );
        let (first_outcome, second_outcome) = tokio::join!(first_begin, second_begin);
        let outcomes = [first_outcome.unwrap(), second_outcome.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, SourceBeginOutcome::PermitToPrepare(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, SourceBeginOutcome::Deferred { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn expired_prepare_lease_is_taken_over_and_stale_owner_cannot_bind() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "e".repeat(40);
        let stale = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                SOURCE_FORMAT_VERSION,
                "old",
                "old",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected prepare permit"),
        };
        sqlx::query("UPDATE git_source_acquisitions SET expires_at=unixepoch()-1 WHERE token=?")
            .bind(&stale.token)
            .execute(&pool)
            .await
            .unwrap();
        let replacement = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                SOURCE_FORMAT_VERSION,
                "new",
                "old",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected takeover"),
        };
        assert!(replacement.generation > stale.generation);
        let source = prepared(&registry, &commit);
        assert!(registry.bind_prepared_graph(&stale, &source).await.is_err());
        assert!(
            registry
                .bind_prepared_graph(&replacement, &source)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn prepare_failure_is_typed_without_provisional_graph_leak() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "f".repeat(40);
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                SOURCE_FORMAT_VERSION,
                "owner",
                "attempt",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected prepare permit"),
        };
        assert!(
            registry
                .fail_preparation(&permit, FailureClass::Permanent)
                .await
                .unwrap()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_acquisition_members")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    &commit,
                    SOURCE_FORMAT_VERSION,
                    "owner",
                    "retry",
                    60,
                    SyncIntent::EnsureCurrent,
                )
                .await
                .unwrap(),
            SourceBeginOutcome::Failed {
                class: FailureClass::Permanent,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn retryable_prepare_failure_allows_same_attempt_id_with_generation_fencing() {
        let (_scheduler, registry, _pool, _temp) = fixture().await;
        let commit = "0".repeat(40);
        let first = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                SOURCE_FORMAT_VERSION,
                "owner",
                "same",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected first permit"),
        };
        assert!(
            registry
                .fail_preparation(&first, FailureClass::Retryable)
                .await
                .unwrap()
        );
        let second = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                SOURCE_FORMAT_VERSION,
                "owner",
                "same",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected same-attempt retry permit"),
        };
        assert!(second.generation > first.generation);
        assert_ne!(second.operation_id, first.operation_id);
        let source = prepared(&registry, &commit);
        assert!(registry.bind_prepared_graph(&first, &source).await.is_err());
        assert!(registry.bind_prepared_graph(&second, &source).await.is_ok());
    }

    #[tokio::test]
    async fn prepare_capability_cannot_alias_identity_or_source_format() {
        let (_scheduler, registry, _pool, _temp) = fixture().await;
        let commit = "9".repeat(40);
        assert!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    &commit,
                    SOURCE_FORMAT_VERSION + 1,
                    "owner",
                    "bad-format",
                    60,
                    SyncIntent::EnsureCurrent,
                )
                .await
                .is_err()
        );
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                SOURCE_FORMAT_VERSION,
                "owner",
                "valid",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected permit"),
        };
        let foreign = prepared_in(&registry, "other", "o/r", &commit);
        assert!(
            registry
                .bind_prepared_graph(&permit, &foreign)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn committed_registration_with_lost_ack_reconciles_ready() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"1".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot = registry
            .register_or_recover_inner(
                &acquisition,
                &source,
                &CancellationToken::new(),
                RegistrationCommitFault::AckLostAfterCommit,
            )
            .await
            .unwrap();
        assert_eq!(snapshot.manifest(), source.root().hash);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM git_source_acquisitions WHERE token=?"
            )
            .bind(&acquisition.token)
            .fetch_one(&pool)
            .await
            .unwrap(),
            "registered"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM git_source_desires WHERE acquisition_token IS NULL"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            "registered"
        );
    }

    #[tokio::test]
    async fn failed_commit_with_open_transaction_retires_connection_and_settles_failed() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"0".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        assert!(
            registry
                .register_or_recover_inner(
                    &acquisition,
                    &source,
                    &CancellationToken::new(),
                    RegistrationCommitFault::FailWithOpenTransaction,
                )
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM git_source_acquisitions WHERE token=?"
            )
            .bind(&acquisition.token)
            .fetch_one(&pool)
            .await
            .unwrap(),
            "failed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_roots")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        let mut fresh = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *fresh)
            .await
            .unwrap();
        sqlx::query("ROLLBACK").execute(&mut *fresh).await.unwrap();
    }

    #[tokio::test]
    async fn unknown_uncommitted_settles_failed_and_ensure_retries() {
        let (_scheduler, registry, _pool, _temp) = fixture().await;
        let source = prepared(&registry, &"b".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        assert!(
            registry
                .mark_activation_unknown(&acquisition)
                .await
                .unwrap()
        );
        assert!(matches!(
            registry.reconcile_activation(&acquisition).await.unwrap(),
            SourceAcquireOutcome::Failed {
                class: FailureClass::Retryable,
                ..
            }
        ));
        assert!(matches!(
            registry
                .protect_prepared(&source, "owner", "retry", 60, SyncIntent::ObserveMovement)
                .await
                .unwrap(),
            SourceAcquireOutcome::Failed { .. }
        ));
        assert!(matches!(
            registry
                .protect_prepared(&source, "owner", "retry", 60, SyncIntent::EnsureCurrent)
                .await
                .unwrap(),
            SourceAcquireOutcome::Acquired { .. }
        ));
    }

    #[tokio::test]
    async fn atomic_intent_handoff_retains_then_releases_both_consumers() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"c".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let outcome = registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        let artifact = match outcome {
            ArtifactObservationOutcome::Recorded { artifacts, .. } => match artifacts[0].1 {
                ArtifactIntentOutcome::Subscribed(id) => id,
                _ => panic!("expected runnable"),
            },
            _ => panic!("expected record"),
        };
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM git_source_consumers WHERE purpose='intent'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM artifact_consumers WHERE artifact_id=?"
            )
            .bind(artifact)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        sqlx::query("UPDATE artifact_jobs SET state='failed',failure_class='permanent',error='test' WHERE id=?").bind(artifact).execute(&pool).await.unwrap();
        assert_eq!(registry.reconcile_terminal_intents(10).await.unwrap(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact_intents")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_consumers")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact_consumers")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn terminal_reconciliation_rechecks_after_retry_requeue() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"2".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let artifact = match registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            ArtifactObservationOutcome::Recorded { artifacts, .. } => match artifacts[0].1 {
                ArtifactIntentOutcome::Subscribed(id) => id,
                _ => panic!("expected subscription"),
            },
            _ => panic!("expected observation"),
        };
        sqlx::query("UPDATE artifact_jobs SET state='failed',failure_class='retryable',retry_count=0,error='retry' WHERE id=?")
            .bind(artifact).execute(&pool).await.unwrap();
        let scanned = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let worker_registry = registry.clone();
        let worker_scanned = scanned.clone();
        let worker_resume = resume.clone();
        let worker = tokio::spawn(async move {
            worker_registry
                .reconcile_terminal_intents_inner(10, Some((worker_scanned, worker_resume)))
                .await
        });
        scanned.wait().await;
        let current = registry.snapshot("ws", "o/r", "main").await.unwrap();
        assert!(matches!(
            registry
                .record_tip_and_intents(
                    &current,
                    &snapshot,
                    &[ArtifactKind::Head],
                    1,
                    SyncIntent::EnsureCurrent,
                )
                .await
                .unwrap(),
            ArtifactObservationOutcome::Recorded { .. }
        ));
        resume.wait().await;
        assert_eq!(worker.await.unwrap().unwrap(), 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact_intents")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_consumers")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM artifact_jobs WHERE id=?")
                .bind(artifact)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "queued"
        );
    }

    #[tokio::test]
    async fn full_capacity_reuses_exact_job_and_preserves_prior_publication() {
        let limits = SchedulerLimits {
            total_backlog: 1,
            workspace_backlog: 1,
            head_reserved: 1,
            head_backlog: 1,
            ..Default::default()
        };
        let (_scheduler, registry, pool, _temp) = fixture_with_limits(limits).await;
        let commit = "d".repeat(40);
        let source = prepared(&registry, &commit);
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let old=sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('ws','o/r',?,'head',1,'ready','old-root',unixepoch(),unixepoch())").bind("e".repeat(40)).execute(&pool).await.unwrap().last_insert_rowid();
        sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES('ws','o/r','main','head',?,?,1,?,1,unixepoch())").bind("e".repeat(40)).bind(old).bind(old).execute(&pool).await.unwrap();
        let exact=sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r',?,'head',1,'queued',unixepoch(),unixepoch())").bind(&commit).execute(&pool).await.unwrap().last_insert_rowid();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let outcome = registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome,ArtifactObservationOutcome::Recorded{ref artifacts,..} if matches!(artifacts[0].1,ArtifactIntentOutcome::Subscribed(id) if id==exact))
        );
        assert_eq!(sqlx::query_scalar::<_,Option<i64>>("SELECT published_artifact_id FROM artifact_observations WHERE workspace='ws' AND repo='o/r' AND branch='main' AND kind='head'").fetch_one(&pool).await.unwrap(),Some(old));
    }

    #[tokio::test]
    async fn advancing_coalesces_deferred_but_retains_old_promoted_generation() {
        let limits = SchedulerLimits {
            total_backlog: 1,
            workspace_backlog: 1,
            head_reserved: 1,
            head_backlog: 1,
            ..Default::default()
        };
        let (_scheduler, registry, pool, _temp) = fixture_with_limits(limits).await;
        let first = prepared(&registry, &"1".repeat(40));
        let a1 = match registry
            .protect_prepared(&first, "owner", "a1", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        let s1 = registry
            .register(&a1, &first, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        registry
            .record_tip_and_intents(
                &before,
                &s1,
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        let second = prepared(&registry, &"2".repeat(40));
        let a2 = match registry
            .protect_prepared(&second, "owner", "a2", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        let s2 = registry
            .register(&a2, &second, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        registry
            .record_tip_and_intents(
                &before,
                &s2,
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM artifact_intents WHERE branch_generation=1 AND state='promoted'").fetch_one(&pool).await.unwrap(),1);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM artifact_intents WHERE branch_generation=1 AND state='deferred'").fetch_one(&pool).await.unwrap(),0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM git_source_consumers WHERE commit_oid=?"
            )
            .bind("1".repeat(40))
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn gc_fences_new_graph_and_composite_paging_keeps_shared_children() {
        let (scheduler, registry, _pool, _temp) = fixture().await;
        assert!(scheduler.acquire_gc_sweep("gc", 60).await.unwrap());
        let first = prepared(&registry, &"3".repeat(40));
        assert!(
            registry
                .protect_prepared(&first, "owner", "blocked", 60, SyncIntent::EnsureCurrent)
                .await
                .is_err()
        );
        scheduler.release_gc_sweep("gc").await.unwrap();
        assert!(matches!(
            registry
                .protect_prepared(&first, "owner", "first", 60, SyncIntent::EnsureCurrent)
                .await
                .unwrap(),
            SourceAcquireOutcome::Acquired { .. }
        ));
        let second = prepared(&registry, &"4".repeat(40));
        assert!(matches!(
            registry
                .protect_prepared(&second, "owner", "second", 60, SyncIntent::EnsureCurrent)
                .await
                .unwrap(),
            SourceAcquireOutcome::Acquired { .. }
        ));
        let mut cursor: Option<(String, String)> = None;
        let mut objects = Vec::new();
        loop {
            let page = registry
                .source_gc_page(cursor.as_ref().map(|(h, o)| (h.as_str(), o.as_str())), 2)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            let last = page.last().unwrap();
            cursor = Some((last.hash.clone(), last.owner.clone()));
            objects.extend(page);
        }
        assert_eq!(objects.len(), 6);
        let shared = objects
            .iter()
            .fold(std::collections::HashMap::new(), |mut map, object| {
                *map.entry(object.hash.clone()).or_insert(0) += 1;
                map
            });
        assert!(shared.values().any(|count| *count == 2));
    }

    #[tokio::test]
    async fn committed_unknown_reconciles_and_retirement_is_grace_and_sweep_fenced() {
        let (scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"5".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET state='activation_unknown' WHERE token=?")
            .bind(&acquisition.token)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_desires SET state='acquiring',root_hash=NULL,acquisition_token=? WHERE workspace='ws' AND repo='o/r' AND commit_oid=?").bind(&acquisition.token).bind("5".repeat(40)).execute(&pool).await.unwrap();
        assert!(matches!(
            registry.reconcile_activation(&acquisition).await.unwrap(),
            SourceAcquireOutcome::Ready(_)
        ));
        sqlx::query("UPDATE git_source_roots SET registered_at=unixepoch()-3600")
            .execute(&pool)
            .await
            .unwrap();
        assert!(scheduler.acquire_gc_sweep("gc", 60).await.unwrap());
        assert!(registry.retire_registered_roots_page(60, 10).await.is_err());
        scheduler.release_gc_sweep("gc").await.unwrap();
        assert_eq!(
            registry.retire_registered_roots_page(60, 10).await.unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM git_source_roots")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn fair_promotion_skips_saturated_lane() {
        let limits = SchedulerLimits {
            total_backlog: 10,
            workspace_backlog: 10,
            head_reserved: 1,
            head_backlog: 1,
            files_backlog: 1,
            ..Default::default()
        };
        let (_scheduler, registry, pool, _temp) = fixture_with_limits(limits).await;
        let source = prepared(&registry, &"6".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        for (kind, commit) in [("head", "7".repeat(40)), ("files", "8".repeat(40))] {
            sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r',?,?,1,'queued',unixepoch(),unixepoch())").bind(commit).bind(kind).execute(&pool).await.unwrap();
        }
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        sqlx::query("DELETE FROM artifact_jobs WHERE kind='files' AND commit_oid<>?")
            .bind("6".repeat(40))
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(registry.promote_deferred_page(1).await.unwrap(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT kind FROM artifact_intents WHERE state='promoted'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            "files"
        );
    }

    #[tokio::test]
    async fn fair_promotion_reaches_workspace_beyond_long_saturated_prefix() {
        let limits = SchedulerLimits {
            total_backlog: 2,
            workspace_backlog: 1,
            head_reserved: 2,
            head_backlog: 2,
            ..Default::default()
        };
        let (_scheduler, registry, pool, _temp) = fixture_with_limits(limits).await;
        let source_a = prepared_in(&registry, "a", "o/r", &"4".repeat(40));
        let acquisition_a = match registry
            .protect_prepared(
                &source_a,
                "owner-a",
                "attempt-a",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot_a = registry
            .register(&acquisition_a, &source_a, &CancellationToken::new())
            .await
            .unwrap();
        let source_z = prepared_in(&registry, "z", "o/r", &"5".repeat(40));
        let acquisition_z = match registry
            .protect_prepared(
                &source_z,
                "owner-z",
                "attempt-z",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot_z = registry
            .register(&acquisition_z, &source_z, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('a','o/r',?,'head',1,'queued',unixepoch(),unixepoch()),('z','o/r',?,'head',1,'queued',unixepoch(),unixepoch())")
            .bind("6".repeat(40)).bind("7".repeat(40)).execute(&pool).await.unwrap();
        for index in 0..65 {
            let branch = format!("branch-{index:03}");
            let before = registry.snapshot("a", "o/r", &branch).await.unwrap();
            registry
                .record_tip_and_intents(
                    &before,
                    &snapshot_a,
                    &[ArtifactKind::Head],
                    1,
                    SyncIntent::EnsureCurrent,
                )
                .await
                .unwrap();
        }
        let before_z = registry.snapshot("z", "o/r", "main").await.unwrap();
        registry
            .record_tip_and_intents(
                &before_z,
                &snapshot_z,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM artifact_intents WHERE workspace='a' AND state='deferred'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            65
        );
        sqlx::query("DELETE FROM artifact_jobs WHERE workspace='z' AND commit_oid=?")
            .bind("7".repeat(40))
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(registry.promote_deferred_page(1).await.unwrap(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT workspace FROM artifact_intents WHERE state='promoted'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            "z"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT intent_workspace_cursor FROM git_source_maintenance WHERE id=1"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            "z"
        );
    }

    #[tokio::test]
    async fn builder_authority_is_exact_lease_bound_and_quarantine_fails_closed() {
        let (_scheduler, registry, pool, temp) = fixture().await;
        let source = prepared(&registry, &"9".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let outcome = registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        let artifact = match outcome {
            ArtifactObservationOutcome::Recorded { artifacts, .. } => match artifacts[0].1 {
                ArtifactIntentOutcome::Subscribed(id) => id,
                _ => panic!(),
            },
            _ => panic!(),
        };
        sqlx::query("UPDATE artifact_jobs SET state='running',owner='worker',lease_generation=3,lease_expires_at=unixepoch()+60 WHERE id=?").bind(artifact).execute(&pool).await.unwrap();
        let session = "a".repeat(64);
        assert!(
            registry
                .claim_authenticated(
                    artifact,
                    "wrong",
                    3,
                    "ws",
                    "o/r",
                    &"9".repeat(40),
                    &session,
                    60
                )
                .await
                .is_err()
        );
        let authority = registry
            .claim_authenticated(
                artifact,
                "worker",
                3,
                "ws",
                "o/r",
                &"9".repeat(40),
                &session,
                60,
            )
            .await
            .unwrap();
        let loader =
            CasGitSourceStore::new(&crate::cas::Cas::new(temp.path().join("objects")).unwrap())
                .unwrap();
        let materialize_scratch = temp.path().join("materialize");
        std::fs::create_dir(&materialize_scratch).unwrap();
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            GitSourceMaterializer::new(&loader, &materialize_scratch, GitSourceLimits::default())
                .materialize(&authority, &cancelled)
                .is_err()
        );
        assert!(
            registry
                .renew_builder_claim(artifact, "worker", 3, snapshot.manifest(), &session, 60)
                .await
                .unwrap()
        );
        sqlx::query("UPDATE artifact_jobs SET lease_expires_at=unixepoch()-1 WHERE id=?")
            .bind(artifact)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            !registry
                .renew_builder_claim(artifact, "worker", 3, snapshot.manifest(), &session, 60)
                .await
                .unwrap()
        );
        assert!(
            registry
                .release_builder_claim(snapshot.manifest(), &session)
                .await
                .unwrap()
        );
        sqlx::query("UPDATE git_source_roots SET state='quarantined' WHERE root_hash=?")
            .bind(snapshot.manifest())
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            registry
                .claim_authenticated(
                    artifact,
                    "worker",
                    3,
                    "ws",
                    "o/r",
                    &"9".repeat(40),
                    &session,
                    60
                )
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn builder_lease_loss_cancels_drains_and_releases_claim() {
        let (_scheduler, registry, pool, temp) = fixture().await;
        let source = prepared(&registry, &"3".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let artifact = match registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            ArtifactObservationOutcome::Recorded { artifacts, .. } => match artifacts[0].1 {
                ArtifactIntentOutcome::Subscribed(id) => id,
                _ => panic!("expected subscription"),
            },
            _ => panic!("expected observation"),
        };
        sqlx::query("UPDATE artifact_jobs SET state='running',owner='worker',lease_generation=4,lease_expires_at=unixepoch()+60 WHERE id=?")
            .bind(artifact).execute(&pool).await.unwrap();
        let started = Arc::new(AtomicBool::new(false));
        let child_cancelled = Arc::new(AtomicBool::new(false));
        let cas = crate::cas::Cas::new(temp.path().join("objects")).unwrap();
        let loader = BlockingLoader {
            inner: CasGitSourceStore::new(&cas).unwrap(),
            started: started.clone(),
            cancelled: child_cancelled.clone(),
        };
        let task_registry = registry.clone();
        let scratch = temp.path().join("builder-materialize");
        std::fs::create_dir(&scratch).unwrap();
        let session = "b".repeat(64);
        let task_session = session.clone();
        let task = tokio::spawn(async move {
            task_registry
                .with_materialized_builder_source(
                    artifact,
                    "worker",
                    4,
                    "ws",
                    "o/r",
                    &"3".repeat(40),
                    &task_session,
                    1,
                    loader,
                    scratch,
                    &CancellationToken::new(),
                    |_source, _cancel| Ok(()),
                )
                .await
        });
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        sqlx::query("UPDATE artifact_jobs SET lease_expires_at=unixepoch()-1 WHERE id=?")
            .bind(artifact)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(child_cancelled.load(Ordering::SeqCst));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM git_source_consumers WHERE session_id=?"
            )
            .bind(session)
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn upload_lease_loss_cancels_and_drains_child_copy() {
        let (_scheduler, registry, _pool, temp) = fixture().await;
        let source = prepared(&registry, &"0".repeat(40));
        let (acquisition, permit) = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired {
                acquisition,
                permit,
            } => (acquisition, permit),
            _ => panic!(),
        };
        let local = crate::cas::Cas::new(temp.path().join("local")).unwrap();
        local
            .put_with_hash(&hex::encode(Sha256::digest(b"pack")), b"pack")
            .unwrap();
        local
            .put_with_hash(&hex::encode(Sha256::digest(b"index")), b"index")
            .unwrap();
        let uploader = SlowUploader::default();
        let scratch = temp.path().join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        let packager =
            GitSourcePackager::new(&local, &uploader, &scratch, GitSourceLimits::default());
        assert!(
            registry
                .fail(&acquisition, FailureClass::Retryable)
                .await
                .unwrap()
        );
        assert!(
            registry
                .publish_protected(
                    &acquisition,
                    &packager,
                    &source,
                    &permit,
                    &CancellationToken::new()
                )
                .await
                .is_err()
        );
        assert!(uploader.cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn intent_outcomes_derive_requeue_ready_and_deadletter_from_job() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"f".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let first = registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        let id = match first {
            ArtifactObservationOutcome::Recorded { artifacts, .. } => match artifacts[0].1 {
                ArtifactIntentOutcome::Subscribed(id) => id,
                _ => panic!(),
            },
            _ => panic!(),
        };
        sqlx::query("UPDATE artifact_jobs SET state='ready',manifest='ready-root' WHERE id=?")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        let current = registry.snapshot("ws", "o/r", "main").await.unwrap();
        assert!(
            matches!(registry.record_tip_and_intents(&current,&snapshot,&[ArtifactKind::Head],1,SyncIntent::EnsureCurrent).await.unwrap(),ArtifactObservationOutcome::Recorded{ref artifacts,..} if matches!(artifacts[0].1,ArtifactIntentOutcome::Ready(v) if v==id))
        );
        sqlx::query("UPDATE artifact_jobs SET state='failed',manifest=NULL,failure_class='dead_letter',error='dead' WHERE id=?").bind(id).execute(&pool).await.unwrap();
        let current = registry.snapshot("ws", "o/r", "main").await.unwrap();
        assert!(
            matches!(registry.record_tip_and_intents(&current,&snapshot,&[ArtifactKind::Head],1,SyncIntent::EnsureCurrent).await.unwrap(),ArtifactObservationOutcome::Recorded{ref artifacts,..} if matches!(artifacts[0].1,ArtifactIntentOutcome::Failed(v,FailureClass::DeadLetter) if v==id))
        );
        sqlx::query(
            "UPDATE artifact_jobs SET state='queued',failure_class=NULL,error=NULL WHERE id=?",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
        let current = registry.snapshot("ws", "o/r", "main").await.unwrap();
        assert!(
            matches!(registry.record_tip_and_intents(&current,&snapshot,&[ArtifactKind::Head],1,SyncIntent::ObserveMovement).await.unwrap(),ArtifactObservationOutcome::Recorded{ref artifacts,..} if matches!(artifacts[0].1,ArtifactIntentOutcome::Subscribed(v) if v==id))
        );
    }

    #[tokio::test]
    async fn validator_rejects_arbitrary_ddl_singletons_hashes_and_descriptor_conflicts() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        sqlx::query("CREATE INDEX harmless_name ON git_source_maintenance(updated_at)")
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("DROP INDEX harmless_name")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TRIGGER harmless_trigger AFTER UPDATE ON git_source_maintenance BEGIN SELECT 1; END")
            .execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("DROP TRIGGER harmless_trigger")
            .execute(&pool)
            .await
            .unwrap();
        let fingerprint = registry.source_fingerprint();
        sqlx::query("DELETE FROM git_source_maintenance")
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("INSERT INTO git_source_maintenance(id,config_fingerprint) VALUES(1,?)")
            .bind(fingerprint)
            .execute(&pool)
            .await
            .unwrap();

        let first = prepared(&registry, &"a".repeat(40));
        let first_acquisition = match registry
            .protect_prepared(&first, "owner", "first", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        registry
            .register(&first_acquisition, &first, &CancellationToken::new())
            .await
            .unwrap();
        let child_hash: String =
            sqlx::query_scalar("SELECT child_hash FROM git_source_members WHERE ordinal=0")
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query("UPDATE git_source_members SET child_hash=upper(child_hash) WHERE ordinal=0")
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("UPDATE git_source_members SET child_hash=? WHERE ordinal=0")
            .bind(&child_hash)
            .execute(&pool)
            .await
            .unwrap();

        let second = prepared(&registry, &"b".repeat(40));
        let second_acquisition = match registry
            .protect_prepared(&second, "owner", "second", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        sqlx::query("UPDATE git_source_acquisitions SET semantic_digest=upper(semantic_digest) WHERE token=?")
            .bind(&second_acquisition.token).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("UPDATE git_source_acquisitions SET semantic_digest=lower(semantic_digest) WHERE token=?")
            .bind(&second_acquisition.token).execute(&pool).await.unwrap();
        sqlx::query("UPDATE git_source_acquisition_members SET child_len=child_len+1 WHERE token=? AND ordinal=0")
            .bind(&second_acquisition.token).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_provisional_member_shape_and_exact_byte_sum() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"1".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        sqlx::query(
            "UPDATE git_source_acquisition_members SET ordinal=2 WHERE token=? AND ordinal=0",
        )
        .bind(&acquisition.token)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query(
            "UPDATE git_source_acquisition_members SET ordinal=0 WHERE token=? AND ordinal=2",
        )
        .bind(&acquisition.token)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET total_bytes=total_bytes+1 WHERE token=?")
            .bind(&acquisition.token)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_members_attached_to_held_acquisition() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &"5".repeat(40),
                SOURCE_FORMAT_VERSION,
                "owner",
                "attempt",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected held acquisition"),
        };
        sqlx::query("INSERT INTO git_source_acquisition_members(token,ordinal,child_hash,child_len,kind) VALUES(?,0,?,1,'pack')")
            .bind(&permit.token).bind("a".repeat(64)).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_recomputes_operation_provenance_for_held_and_bound_graphs() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "b".repeat(40);
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                SOURCE_FORMAT_VERSION,
                "owner",
                "attempt",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected held acquisition"),
        };
        sqlx::query("UPDATE git_source_acquisitions SET operation_id=? WHERE token=?")
            .bind("c".repeat(64))
            .bind(&permit.token)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("UPDATE git_source_acquisitions SET operation_id=? WHERE token=?")
            .bind(&permit.operation_id)
            .bind(&permit.token)
            .execute(&pool)
            .await
            .unwrap();
        let source = prepared(&registry, &commit);
        registry
            .bind_prepared_graph(&permit, &source)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET operation_id=? WHERE token=?")
            .bind("d".repeat(64))
            .bind(&permit.token)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_noncanonical_acquisition_token() {
        let (_scheduler, _registry, pool, _temp) = fixture().await;
        let commit = "c".repeat(40);
        let operation = operation_id("ws", "o/r", &commit, "attempt", 1);
        sqlx::query("UPDATE git_source_acquisition_sequence SET generation=1 WHERE id=1")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO git_source_acquisitions(token,generation,operation_id,workspace,repo,commit_oid,source_format_version,owner,attempt_id,expires_at,state) VALUES('bad',1,?,'ws','o/r',?,1,'owner','attempt',unixepoch()+60,'held')")
            .bind(operation).bind(&commit).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO git_source_desires(workspace,repo,commit_oid,source_format_version,state,retry_count,acquisition_token,updated_at) VALUES('ws','o/r',?,1,'acquiring',0,'bad',unixepoch())")
            .bind(commit).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_orphan_held_acquisition() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &"d".repeat(40),
                1,
                "owner",
                "attempt",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(v) => v,
            _ => panic!(),
        };
        sqlx::query("DELETE FROM git_source_desires WHERE acquisition_token=?")
            .bind(&permit.token)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_orphan_bound_graph_acquisition() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "e".repeat(40);
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &commit,
                1,
                "owner",
                "attempt",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(v) => v,
            _ => panic!(),
        };
        registry
            .bind_prepared_graph(&permit, &prepared(&registry, &commit))
            .await
            .unwrap();
        sqlx::query("DELETE FROM git_source_desires WHERE acquisition_token=?")
            .bind(&permit.token)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_registered_graph_without_reverse_desire() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"f".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("DELETE FROM git_source_desires WHERE state='registered'")
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_current_branch_without_core_observation() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "a".repeat(40);
        let source = prepared(&registry, &commit);
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("INSERT INTO branch_source_generations(workspace,repo,branch,generation,commit_oid,source_format_version,root_hash,created_at) VALUES('ws','o/r','main',1,?,1,?,unixepoch())").bind(&commit).bind(snapshot.manifest()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO branch_source_current(workspace,repo,branch,generation) VALUES('ws','o/r','main',1)").execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_orphan_intent_source_consumer() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let commit = "b".repeat(40);
        let source = prepared(&registry, &commit);
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("INSERT INTO git_source_consumers(root_hash,consumer_id,session_id,workspace,repo,commit_oid,source_format_version,purpose,expires_at) VALUES(?,'orphan','orphan-session','ws','o/r',?,1,'intent',?)")
            .bind(snapshot.manifest()).bind(commit).bind(i64::MAX).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_malformed_intent_consumer_capability() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let (_artifact, consumer) = promoted_head_intent(&registry, &pool, &"c".repeat(40)).await;
        assert_registry_validation_passes(&pool).await;
        let malformed = format!("intent:{}", "d".repeat(47));
        sqlx::query("UPDATE artifact_intents SET consumer_id=? WHERE consumer_id=?")
            .bind(&malformed)
            .bind(&consumer)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_consumers SET consumer_id=? WHERE consumer_id=?")
            .bind(&malformed)
            .bind(&consumer)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE artifact_consumers SET consumer_id=? WHERE consumer_id=?")
            .bind(&malformed)
            .bind(&consumer)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_malformed_intent_source_session_capability() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let (_artifact, consumer) = promoted_head_intent(&registry, &pool, &"d".repeat(40)).await;
        assert_registry_validation_passes(&pool).await;
        sqlx::query(
            "UPDATE git_source_consumers SET session_id='not-a-capability' WHERE consumer_id=?",
        )
        .bind(consumer)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_deferred_intent_with_artifact_consumer() {
        let limits = SchedulerLimits {
            total_backlog: 1,
            workspace_backlog: 1,
            head_reserved: 1,
            ..Default::default()
        };
        let (_scheduler, registry, pool, _temp) = fixture_with_limits(limits).await;
        let blocker = sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r',?,'head',1,'queued',unixepoch(),unixepoch())")
            .bind("e".repeat(40)).execute(&pool).await.unwrap().last_insert_rowid();
        let source = registered_source(&registry, &"f".repeat(40)).await;
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let outcome = registry
            .record_tip_and_intents(
                &before,
                &source,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            ArtifactObservationOutcome::Recorded { ref artifacts, .. }
                if matches!(artifacts[0].1, ArtifactIntentOutcome::Deferred(_))
        ));
        let consumer: String =
            sqlx::query_scalar("SELECT consumer_id FROM artifact_intents WHERE state='deferred'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_registry_validation_passes(&pool).await;
        sqlx::query(
            "INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?)",
        )
        .bind(blocker)
        .bind(consumer)
        .bind(SOURCE_INTENT_RETENTION_EXPIRY)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_promoted_intent_with_extra_wrong_artifact_consumer() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let (_artifact, consumer) = promoted_head_intent(&registry, &pool, &"1".repeat(40)).await;
        let wrong = sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r',?,'files',1,'queued',unixepoch(),unixepoch())")
            .bind("2".repeat(40)).execute(&pool).await.unwrap().last_insert_rowid();
        assert_registry_validation_passes(&pool).await;
        sqlx::query(
            "INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?)",
        )
        .bind(wrong)
        .bind(consumer)
        .bind(SOURCE_INTENT_RETENTION_EXPIRY)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_promoted_intent_with_only_wrong_artifact_consumer() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let (artifact, consumer) = promoted_head_intent(&registry, &pool, &"3".repeat(40)).await;
        let wrong = sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES('ws','o/r',?,'files',1,'queued',unixepoch(),unixepoch())")
            .bind("4".repeat(40)).execute(&pool).await.unwrap().last_insert_rowid();
        assert_registry_validation_passes(&pool).await;
        sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?")
            .bind(artifact)
            .bind(&consumer)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?)",
        )
        .bind(wrong)
        .bind(consumer)
        .bind(SOURCE_INTENT_RETENTION_EXPIRY)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_fully_orphaned_reserved_artifact_consumer() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let (artifact, _consumer) = promoted_head_intent(&registry, &pool, &"5".repeat(40)).await;
        assert_registry_validation_passes(&pool).await;
        sqlx::query(
            "INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?)",
        )
        .bind(artifact)
        .bind(format!("intent:{}", "6".repeat(48)))
        .bind(SOURCE_INTENT_RETENTION_EXPIRY)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_nonpermanent_or_unpaired_intent_retention() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let (artifact, consumer) = promoted_head_intent(&registry, &pool, &"7".repeat(40)).await;
        assert_registry_validation_passes(&pool).await;
        sqlx::query("UPDATE git_source_consumers SET expires_at=? WHERE consumer_id=?")
            .bind(SOURCE_INTENT_RETENTION_EXPIRY - 1)
            .bind(&consumer)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("UPDATE git_source_consumers SET expires_at=? WHERE consumer_id=?")
            .bind(SOURCE_INTENT_RETENTION_EXPIRY)
            .bind(&consumer)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_passes(&pool).await;
        sqlx::query(
            "UPDATE artifact_consumers SET expires_at=? WHERE artifact_id=? AND consumer_id=?",
        )
        .bind(SOURCE_INTENT_RETENTION_EXPIRY - 1)
        .bind(artifact)
        .bind(consumer)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_registered_acquisition_descriptor_and_registration_mismatch() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"2".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET operation_id=? WHERE token=?")
            .bind("0".repeat(64))
            .bind(&acquisition.token)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
        sqlx::query("UPDATE git_source_acquisitions SET operation_id=? WHERE token=?")
            .bind(&acquisition.operation_id)
            .bind(&acquisition.token)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_roots SET registration_generation=registration_generation+1 WHERE root_hash=?")
            .bind(&acquisition.root.hash).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_registered_root_without_exact_registered_acquisition() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"7".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET state='graph_published',expires_at=unixepoch()+60 WHERE token=?")
            .bind(&acquisition.token).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_root_descriptor_conflicts_and_root_child_aliases() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"3".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let (child_hash,child_len):(String,i64)=sqlx::query_as("SELECT child_hash,child_len FROM git_source_acquisition_members WHERE token=? AND ordinal=0").bind(&acquisition.token).fetch_one(&pool).await.unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET root_hash=?,root_len=? WHERE token=?")
            .bind(child_hash)
            .bind(child_len)
            .bind(&acquisition.token)
            .execute(&pool)
            .await
            .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_desire_source_format_divergence_from_intent() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let source = prepared(&registry, &"4".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!("expected acquisition"),
        };
        let snapshot = registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_desires SET source_format_version=2 WHERE workspace='ws' AND repo='o/r' AND commit_oid=?")
            .bind("4".repeat(40)).execute(&pool).await.unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn validator_rejects_desire_source_format_divergence_from_acquisition() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &"6".repeat(40),
                SOURCE_FORMAT_VERSION,
                "owner",
                "attempt",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
            _ => panic!("expected held acquisition"),
        };
        sqlx::query(
            "UPDATE git_source_desires SET source_format_version=2 WHERE acquisition_token=?",
        )
        .bind(&permit.token)
        .execute(&pool)
        .await
        .unwrap();
        assert_registry_validation_fails(&pool).await;
    }

    #[tokio::test]
    async fn exact_startup_rejects_planted_ddl_and_corrupt_digest_state() {
        let (_scheduler, registry, pool, _temp) = fixture().await;
        sqlx::raw_sql("DROP INDEX artifact_intents_source; CREATE INDEX artifact_intents_source ON artifact_intents(state,source_root_hash)").execute(&pool).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert!(validate_sqlite_v7_in(&mut connection).await.is_err());
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::raw_sql("DROP INDEX artifact_intents_source; CREATE INDEX artifact_intents_source ON artifact_intents(source_root_hash,state,id)").execute(&pool).await.unwrap();
        sqlx::raw_sql("CREATE TRIGGER git_source_planted AFTER INSERT ON git_source_maintenance BEGIN SELECT 1; END").execute(&pool).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert!(validate_sqlite_v7_in(&mut connection).await.is_err());
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER git_source_planted")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_maintenance SET config_fingerprint='bad'")
            .execute(&pool)
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert!(validate_sqlite_v7_in(&mut connection).await.is_err());
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_maintenance SET config_fingerprint=?")
            .bind(registry.source_fingerprint())
            .execute(&pool)
            .await
            .unwrap();
        let source = prepared(&registry, &"a".repeat(40));
        let acquisition = match registry
            .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
            .await
            .unwrap()
        {
            SourceAcquireOutcome::Acquired { acquisition, .. } => acquisition,
            _ => panic!(),
        };
        registry
            .register(&acquisition, &source, &CancellationToken::new())
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_roots SET semantic_digest=upper(semantic_digest)")
            .execute(&pool)
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert!(validate_sqlite_v7_in(&mut connection).await.is_err());
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn partial_source_namespace_is_rejected_without_mutation() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE git_source_roots(planted TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert!(migrate_sqlite_v7_in(&mut connection).await.is_err());
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='git_source_roots'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pragma_table_info('git_source_roots')"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn fingerprint_is_sealed_and_empty_adoption_requires_pristine_state() {
        let (_scheduler, registry, pool, temp) = fixture().await;
        let different_limits = SchedulerLimits {
            files_backlog: registry.scheduler_limits.files_backlog + 1,
            ..registry.scheduler_limits.clone()
        };
        let storage: StorageRef = Arc::new(LocalStorage::new(temp.path().join("objects")).unwrap());
        assert!(
            SqliteGitSourceRegistry::new(
                pool.clone(),
                storage.clone(),
                different_limits,
                GitSourceLimits::default(),
                [7; 32],
            )
            .await
            .is_err()
        );
        let source = prepared(&registry, &"8".repeat(40));
        assert!(matches!(
            registry
                .protect_prepared(&source, "owner", "attempt", 60, SyncIntent::EnsureCurrent)
                .await
                .unwrap(),
            SourceAcquireOutcome::Acquired { .. }
        ));
        sqlx::query("UPDATE git_source_maintenance SET config_fingerprint='' WHERE id=1")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            SqliteGitSourceRegistry::new(
                pool,
                storage,
                registry.scheduler_limits.clone(),
                GitSourceLimits::default(),
                [7; 32],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn pristine_source_registry_rejects_limits_divergent_from_durable_scheduler() {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("limits.db");
        let limits_a = SchedulerLimits::default();
        let _scheduler = ArtifactScheduler::open(database.to_str().unwrap(), limits_a.clone())
            .await
            .unwrap();
        let pool = SqlitePool::connect(&format!("sqlite://{}", database.display()))
            .await
            .unwrap();
        let limits_b = SchedulerLimits {
            files_backlog: limits_a.files_backlog + 1,
            ..limits_a
        };
        assert!(
            SqliteGitSourceRegistry::new(
                pool,
                Arc::new(LocalStorage::new(temp.path().join("objects")).unwrap()),
                limits_b,
                GitSourceLimits::default(),
                [7; 32],
            )
            .await
            .is_err()
        );
    }
}
