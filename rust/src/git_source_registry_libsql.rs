//! libSQL/Turso authority for immutable Git source graphs.
//!
//! The libSQL driver gives each operation an owned transaction. Dropping an
//! in-flight future drops that transaction, so cancellation cannot return an
//! open transaction to a shared connection. Schema publication is performed
//! in the scheduler's fleet-wide immediate migration transaction.

use super::{
    GitSourceAcquisition, GitSourcePreparePermit, GitSourcePublicationPermit,
    SOURCE_FORMAT_VERSION, SOURCE_ROOT_PAGE_MAX, SQLITE_V7_SCHEMA, SourceAcquireOutcome,
    SourceBeginOutcome, SourceGcObject, canonical_ddl, checked_i64, checked_u32, checked_u64,
    expected_sqlite_v7_ddl, operation_id, validate_acquire_identity, verify_acquisition_identity,
    verify_storage_graph,
};
use ::libsql::{Connection, Value};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::artifact_scheduler::{FailureClass, SchedulerLimits, scheduler_limits_fingerprint};
use crate::git_source::{GitSourceLimits, GitSourcePackager, GitSourceUploader, PreparedGitSource};
use crate::storage::StorageRef;
use crate::sync_coordinator::{DurableSourceSnapshot, SyncIntent};
use tokio_util::sync::CancellationToken;

macro_rules! values {
    ($($value:expr),* $(,)?) => {{
        let values: Vec<Value> = vec![$($value),*];
        values
    }};
}

/// Placeholder authority handle. Runtime lifecycle methods are added in the
/// same module so the schema and state machine cannot drift into sidecars.
#[derive(Clone)]
pub struct LibsqlGitSourceRegistry {
    database: std::sync::Arc<::libsql::Database>,
    storage: StorageRef,
    scheduler_limits: SchedulerLimits,
    source_limits: GitSourceLimits,
    seal: std::sync::Arc<[u8; 32]>,
}

impl LibsqlGitSourceRegistry {
    pub async fn new(
        database: std::sync::Arc<::libsql::Database>,
        storage: StorageRef,
        scheduler_limits: SchedulerLimits,
        source_limits: GitSourceLimits,
        seal: [u8; 32],
    ) -> Result<Self> {
        let registry = Self {
            database,
            storage,
            scheduler_limits,
            source_limits,
            seal: std::sync::Arc::new(seal),
        };
        let connection = registry.database.connect()?;
        let transaction = connection
            .transaction_with_behavior(::libsql::TransactionBehavior::Immediate)
            .await?;
        let result: Result<()> = async {
            validate_v7_schema(&transaction).await?;
            let durable_limits = one_string(
                &transaction,
                "SELECT limits_fingerprint FROM scheduler_state WHERE id=1",
            )
            .await
            .context("libsql source registry requires initialized scheduler limits")?;
            if durable_limits != scheduler_limits_fingerprint(&registry.scheduler_limits) {
                bail!("libsql source registry scheduler limits differ from durable scheduler")
            }
            let row = one_maintenance(&transaction).await?;
            let fingerprint = registry.source_fingerprint();
            if row.1.is_empty() {
                let authoritative = one_i64(&transaction, "SELECT (SELECT generation FROM git_source_acquisition_sequence WHERE id=1)+(SELECT count(*) FROM git_source_roots)+(SELECT count(*) FROM git_source_members)+(SELECT count(*) FROM git_source_acquisitions)+(SELECT count(*) FROM git_source_acquisition_members)+(SELECT count(*) FROM git_source_desires)+(SELECT count(*) FROM branch_source_generations)+(SELECT count(*) FROM branch_source_current)+(SELECT count(*) FROM git_source_consumers)+(SELECT count(*) FROM artifact_intents)").await?;
                if authoritative != 0
                    || row.2 != 0
                    || !row.3.is_empty()
                    || row.4 != 0
                    || !row.5.is_empty()
                    || row.6 != 0
                {
                    bail!("empty libsql source fingerprint has authoritative state")
                }
                let changed = transaction
                    .execute(
                        "UPDATE git_source_maintenance SET config_fingerprint=? WHERE id=1 AND config_fingerprint=''",
                        [fingerprint.clone()],
                    )
                    .await?;
                if changed != 1 {
                    bail!("libsql source registry configuration CAS failed")
                }
            } else if row.1 != fingerprint {
                bail!("libsql source limits or authority seal differ from fleet configuration")
            }
            Ok(())
        }
        .await;
        finish(transaction, result).await?;
        Ok(registry)
    }

    pub fn fleet_seal_identity(&self) -> String {
        hex::encode(Sha256::digest(self.seal.as_ref()))
    }

    pub fn storage(&self) -> &StorageRef {
        &self.storage
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
        validate_acquire_identity(
            workspace,
            repo,
            commit,
            source_format_version,
            owner,
            attempt_id,
            ttl_secs,
        )?;
        let connection = self.database.connect()?;
        let transaction = connection
            .transaction_with_behavior(::libsql::TransactionBehavior::Immediate)
            .await?;
        let result: Result<SourceBeginOutcome> = async {
            let now = one_i64(&transaction, "SELECT unixepoch()").await?;
            self.reclaim_expired_identity(
                &transaction,
                workspace,
                repo,
                commit,
                source_format_version,
                now,
            )
            .await?;
            let mut desires = transaction.query("SELECT state,root_hash,failure_class,retry_count,acquisition_token FROM git_source_desires WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=?",values![workspace.into(),repo.into(),commit.into(),(source_format_version as i64).into()]).await?;
            if let Some(row) = desires.next().await? {
                let state: String = row.get(0)?;
                if state == "registered" {
                    let root: String = row.get(1)?;
                    drop(desires);
                    let mut acquisitions = transaction.query("SELECT token,generation FROM git_source_acquisitions WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND root_hash=? AND state='registered' ORDER BY generation DESC LIMIT 1",values![workspace.into(),repo.into(),commit.into(),(source_format_version as i64).into(),root.clone().into()]).await?;
                    let registered = acquisitions
                        .next()
                        .await?
                        .context("registered libsql source acquisition missing")?;
                    let token: String = registered.get(0)?;
                    let generation: i64 = registered.get(1)?;
                    return Ok(SourceBeginOutcome::Ready(DurableSourceSnapshot::registered(
                        workspace.to_owned(),
                        repo.to_owned(),
                        commit.to_owned(),
                        root,
                        token,
                        checked_u64(generation, "source generation")?,
                    )?));
                }
                if state == "acquiring" {
                    let token: String = row.get(4)?;
                    drop(desires);
                    let mut acquisitions = transaction
                        .query(
                            "SELECT generation,state FROM git_source_acquisitions WHERE token=?",
                            [token.clone()],
                        )
                        .await?;
                    let acquisition = acquisitions
                        .next()
                        .await?
                        .context("active libsql source acquisition missing")?;
                    let generation = checked_u64(acquisition.get(0)?, "source generation")?;
                    let acquisition_state: String = acquisition.get(1)?;
                    return Ok(if acquisition_state == "activation_unknown" {
                        SourceBeginOutcome::ActivationUnknown { token, generation }
                    } else {
                        SourceBeginOutcome::Deferred { token, generation }
                    });
                }
                let class = FailureClass::parse(&row.get::<String>(2)?)?;
                let retries = checked_u32(row.get(3)?, "source retry count")?;
                if intent == SyncIntent::ObserveMovement
                    || class != FailureClass::Retryable
                    || retries >= self.scheduler_limits.max_manual_retries
                {
                    return Ok(SourceBeginOutcome::Failed { class, retries });
                }
            }
            drop(desires);
            let prior = one_i64(
                &transaction,
                "SELECT generation FROM git_source_acquisition_sequence WHERE id=1",
            )
            .await?;
            let generation = prior.checked_add(1).context("source generation overflow")?;
            if transaction.execute("UPDATE git_source_acquisition_sequence SET generation=? WHERE id=1 AND generation=?",values![generation.into(),prior.into()]).await? != 1 {
                bail!("libsql source generation CAS failed")
            }
            let token = hex::encode(rand::random::<[u8; 32]>());
            let operation = operation_id(workspace, repo, commit, attempt_id, generation);
            transaction.execute("INSERT INTO git_source_acquisitions(token,generation,operation_id,workspace,repo,commit_oid,source_format_version,owner,attempt_id,expires_at,state,failure_class) VALUES(?,?,?,?,?,?,?,?,?,?,'held',NULL)",values![token.clone().into(),generation.into(),operation.clone().into(),workspace.into(),repo.into(),commit.into(),(source_format_version as i64).into(),owner.into(),attempt_id.into(),(now+ttl_secs).into()]).await?;
            transaction.execute("INSERT INTO git_source_desires(workspace,repo,commit_oid,source_format_version,state,root_hash,failure_class,retry_count,acquisition_token,updated_at) VALUES(?,?,?,?,'acquiring',NULL,NULL,0,?,?) ON CONFLICT(workspace,repo,commit_oid,source_format_version) DO UPDATE SET state='acquiring',root_hash=NULL,failure_class=NULL,retry_count=git_source_desires.retry_count+1,acquisition_token=excluded.acquisition_token,updated_at=excluded.updated_at",values![workspace.into(),repo.into(),commit.into(),(source_format_version as i64).into(),token.clone().into(),now.into()]).await?;
            Ok(SourceBeginOutcome::PermitToPrepare(GitSourcePreparePermit {
                token,
                generation: checked_u64(generation, "source generation")?,
                operation_id: operation,
                workspace: workspace.to_owned(),
                repo: repo.to_owned(),
                commit: commit.to_owned(),
                source_format_version,
                owner: owner.to_owned(),
                attempt_id: attempt_id.to_owned(),
            }))
        }
        .await;
        finish(transaction, result).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn reclaim_expired_identity(
        &self,
        transaction: &Connection,
        workspace: &str,
        repo: &str,
        commit: &str,
        source_format_version: u32,
        now: i64,
    ) -> Result<()> {
        let mut rows = transaction.query("SELECT token FROM git_source_acquisitions WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND state IN('held','graph_published') AND expires_at<=?",values![workspace.into(),repo.into(),commit.into(),(source_format_version as i64).into(),now.into()]).await?;
        if let Some(row) = rows.next().await? {
            let token: String = row.get(0)?;
            drop(rows);
            if transaction.execute("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=? WHERE acquisition_token=? AND state='acquiring'",values![now.into(),token.clone().into()]).await? != 1 {
                bail!("expired libsql source desire settlement lost")
            }
            if transaction.execute("UPDATE git_source_acquisitions SET state='failed',failure_class='retryable',expires_at=0 WHERE token=? AND state IN('held','graph_published')",[token]).await? != 1 {
                bail!("expired libsql source acquisition settlement lost")
            }
        }
        Ok(())
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
            bail!("prepared graph identity differs from held libsql acquisition")
        }
        let connection = self.database.connect()?;
        let transaction = connection
            .transaction_with_behavior(::libsql::TransactionBehavior::Immediate)
            .await?;
        let result = async {
            let now = one_i64(&transaction, "SELECT unixepoch()").await?;
            if one_i64(
                &transaction,
                "SELECT count(*) FROM artifact_gc_sweep WHERE expires_at>unixepoch()",
            )
            .await?
                != 0
            {
                bail!("libsql source graph publication is fenced by live GC sweep")
            }
            let changed = transaction.execute("UPDATE git_source_acquisitions SET root_hash=?,root_len=?,object_format=?,semantic_digest=?,object_set_digest=?,object_count=?,total_bytes=?,state='graph_published' WHERE token=? AND generation=? AND operation_id=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND owner=? AND attempt_id=? AND state='held' AND expires_at>?",values![view.root.hash.clone().into(),checked_i64(view.root.len,"root length")?.into(),view.object_format.into(),view.semantic_digest.clone().into(),view.object_set_digest.clone().into(),checked_i64(view.object_count,"object count")?.into(),checked_i64(view.total_bytes,"source bytes")?.into(),prepare.token.clone().into(),(prepare.generation as i64).into(),prepare.operation_id.clone().into(),prepare.workspace.clone().into(),prepare.repo.clone().into(),prepare.commit.clone().into(),(prepare.source_format_version as i64).into(),prepare.owner.clone().into(),prepare.attempt_id.clone().into(),now.into()]).await?;
            if changed != 1 {
                bail!("held libsql source preparation capability was lost")
            }
            for member in &view.members {
                transaction.execute("INSERT INTO git_source_acquisition_members(token,ordinal,child_hash,child_len,kind) VALUES(?,?,?,?,?)",values![prepare.token.clone().into(),(member.ordinal as i64).into(),member.blob.hash.clone().into(),checked_i64(member.blob.len,"member length")?.into(),member.kind.into()]).await?;
            }
            let acquisition = GitSourceAcquisition {
                token: prepare.token.clone(),
                generation: prepare.generation,
                operation_id: prepare.operation_id.clone(),
                workspace: prepare.workspace.clone(),
                repo: prepare.repo.clone(),
                commit: prepare.commit.clone(),
                source_format_version: prepare.source_format_version,
                root: view.root.clone(),
            };
            let publication = GitSourcePublicationPermit {
                token: prepare.token.clone(),
                generation: prepare.generation,
                workspace: prepare.workspace.clone(),
                repo: prepare.repo.clone(),
                commit: prepare.commit.clone(),
                root: view.root.clone(),
            };
            Ok((acquisition, publication))
        }
        .await;
        finish(transaction, result).await
    }

    pub async fn renew_preparation(
        &self,
        prepare: &GitSourcePreparePermit,
        ttl_secs: i64,
    ) -> Result<bool> {
        if !(1..=3600).contains(&ttl_secs) {
            bail!("libsql source preparation TTL is invalid")
        }
        let connection = self.database.connect()?;
        Ok(connection.execute("UPDATE git_source_acquisitions SET expires_at=unixepoch()+? WHERE token=? AND generation=? AND operation_id=? AND owner=? AND attempt_id=? AND state='held' AND expires_at>unixepoch()",values![ttl_secs.into(),prepare.token.clone().into(),(prepare.generation as i64).into(),prepare.operation_id.clone().into(),prepare.owner.clone().into(),prepare.attempt_id.clone().into()]).await? == 1)
    }

    pub async fn fail_preparation(
        &self,
        prepare: &GitSourcePreparePermit,
        class: FailureClass,
    ) -> Result<bool> {
        let connection = self.database.connect()?;
        let transaction = connection
            .transaction_with_behavior(::libsql::TransactionBehavior::Immediate)
            .await?;
        let result = async {
            if transaction.execute("UPDATE git_source_acquisitions SET state='failed',failure_class=?,expires_at=0 WHERE token=? AND generation=? AND operation_id=? AND owner=? AND attempt_id=? AND state='held'",values![class.as_str().into(),prepare.token.clone().into(),(prepare.generation as i64).into(),prepare.operation_id.clone().into(),prepare.owner.clone().into(),prepare.attempt_id.clone().into()]).await? != 1 {
                return Ok(false)
            }
            if transaction.execute("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class=?,acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'",values![class.as_str().into(),prepare.token.clone().into()]).await? != 1 {
                bail!("libsql source preparation desire settlement lost")
            }
            Ok(true)
        }.await;
        finish(transaction, result).await
    }

    pub async fn renew(&self, acquisition: &GitSourceAcquisition, ttl_secs: i64) -> Result<bool> {
        if !(1..=3600).contains(&ttl_secs) {
            bail!("libsql source acquisition TTL is invalid")
        }
        let connection = self.database.connect()?;
        Ok(connection.execute("UPDATE git_source_acquisitions SET expires_at=unixepoch()+? WHERE token=? AND generation=? AND operation_id=? AND state='graph_published' AND expires_at>unixepoch()",values![ttl_secs.into(),acquisition.token.clone().into(),(acquisition.generation as i64).into(),acquisition.operation_id.clone().into()]).await? == 1)
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
            bail!("libsql source acquisition and publication permit differ")
        }
        let plan = packager.owned_upload_plan(prepared)?;
        let publication_cancel = cancelled.child_token();
        let heartbeat_cancel = publication_cancel.clone();
        let registry = self.clone();
        let heartbeat_acquisition = acquisition.clone();
        let mut heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    _=heartbeat_cancel.cancelled()=>return Ok(()),
                    _=interval.tick()=>{
                        if !registry.renew(&heartbeat_acquisition,60).await?{
                            heartbeat_cancel.cancel();
                            bail!("libsql source acquisition lease was lost during upload")
                        }
                    }
                }
            }
        });
        let upload_cancel = publication_cancel.clone();
        let mut upload = tokio::task::spawn_blocking(move || plan.publish(&upload_cancel));
        tokio::select! {
            result=&mut upload=>{
                publication_cancel.cancel();
                let upload_result=result.context("libsql source upload task did not join")?;
                let heartbeat_result=heartbeat.await.context("libsql source upload heartbeat did not join")?;
                heartbeat_result?;upload_result
            }
            result=&mut heartbeat=>{
                publication_cancel.cancel();
                let heartbeat_result=result.context("libsql source upload heartbeat did not join")?;
                let upload_result=upload.await.context("cancelled libsql source upload did not join")?;
                heartbeat_result?;upload_result
            }
        }
    }

    pub async fn fail(
        &self,
        acquisition: &GitSourceAcquisition,
        class: FailureClass,
    ) -> Result<bool> {
        let connection = self.database.connect()?;
        let transaction = connection
            .transaction_with_behavior(::libsql::TransactionBehavior::Immediate)
            .await?;
        let result = async {
            if transaction.execute("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class=?,acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'",values![class.as_str().into(),acquisition.token.clone().into()]).await? == 0 {
                return Ok(false)
            }
            if transaction.execute("UPDATE git_source_acquisitions SET state='failed',failure_class=?,expires_at=0 WHERE token=? AND generation=? AND operation_id=? AND state='graph_published'",values![class.as_str().into(),acquisition.token.clone().into(),(acquisition.generation as i64).into(),acquisition.operation_id.clone().into()]).await? != 1 {
                bail!("libsql source failure capability lost")
            }
            Ok(true)
        }.await;
        finish(transaction, result).await
    }

    pub async fn register(
        &self,
        acquisition: &GitSourceAcquisition,
        prepared: &PreparedGitSource,
        cancelled: &CancellationToken,
    ) -> Result<DurableSourceSnapshot> {
        let view = prepared.registry_view(&self.source_limits)?;
        verify_acquisition_identity(acquisition, &view)?;
        let storage = self.storage.clone();
        let blobs = view
            .members
            .iter()
            .map(|member| member.blob.clone())
            .chain(std::iter::once(view.root.clone()))
            .collect::<Vec<_>>();
        let root_bytes = view.root_bytes.clone();
        let root_hash = view.root.hash.clone();
        let verification_cancel = CancellationToken::new();
        let blocking_cancel = verification_cancel.clone();
        let mut verification = tokio::task::spawn_blocking(move || {
            verify_storage_graph(&storage, &blobs, &root_hash, &root_bytes, &blocking_cancel)
        });
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                result=&mut verification=>{result.context("libsql source storage verifier did not join")??;break}
                _=cancelled.cancelled()=>{
                    verification_cancel.cancel();
                    verification.await.context("cancelled libsql source verifier did not join")??;
                    bail!("libsql source registration cancelled")
                }
                _=heartbeat.tick()=>{
                    if !self.renew(acquisition,60).await?{
                        verification_cancel.cancel();
                        verification.await.context("lease-lost libsql source verifier did not join")??;
                        bail!("libsql source acquisition lease was lost during verification")
                    }
                }
            }
        }
        let connection = self.database.connect()?;
        let transaction = connection
            .transaction_with_behavior(::libsql::TransactionBehavior::Immediate)
            .await?;
        let result:Result<DurableSourceSnapshot>=async{
            let now=one_i64(&transaction,"SELECT unixepoch()").await?;
            assert_exact_graph(&transaction,acquisition,&view,now).await?;
            transaction.execute("INSERT INTO git_source_roots(root_hash,root_len,workspace,repo,commit_oid,source_format_version,object_format,semantic_digest,object_set_digest,object_count,total_bytes,registration_operation,registration_generation,state,created_at,registered_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'registered',?,?)",values![view.root.hash.clone().into(),checked_i64(view.root.len,"root length")?.into(),view.workspace.clone().into(),view.repo.clone().into(),view.commit.clone().into(),(view.source_format_version as i64).into(),view.object_format.into(),view.semantic_digest.clone().into(),view.object_set_digest.clone().into(),checked_i64(view.object_count,"object count")?.into(),checked_i64(view.total_bytes,"source bytes")?.into(),acquisition.operation_id.clone().into(),(acquisition.generation as i64).into(),now.into(),now.into()]).await?;
            for member in &view.members{
                transaction.execute("INSERT INTO git_source_members(root_hash,ordinal,child_hash,child_len,kind) VALUES(?,?,?,?,?)",values![view.root.hash.clone().into(),(member.ordinal as i64).into(),member.blob.hash.clone().into(),checked_i64(member.blob.len,"member length")?.into(),member.kind.into()]).await?;
            }
            if transaction.execute("UPDATE git_source_acquisitions SET state='registered',expires_at=0 WHERE token=? AND generation=? AND state='graph_published'",values![acquisition.token.clone().into(),(acquisition.generation as i64).into()]).await?!=1{bail!("libsql source registration capability lost")}
            if transaction.execute("UPDATE git_source_desires SET state='registered',root_hash=?,failure_class=NULL,acquisition_token=NULL,updated_at=? WHERE acquisition_token=? AND state='acquiring'",values![view.root.hash.clone().into(),now.into(),acquisition.token.clone().into()]).await?!=1{bail!("libsql source registration desire lost")}
            DurableSourceSnapshot::registered(view.workspace.clone(),view.repo.clone(),view.commit.clone(),view.root.hash.clone(),acquisition.token.clone(),acquisition.generation)
        }.await;
        match result {
            Err(error) => {
                let rollback = transaction.rollback().await;
                if let Err(rollback) = rollback {
                    return Err(error).context(format!(
                        "libsql source registration rollback failed: {rollback}"
                    ));
                }
                let _ = self.fail(acquisition, FailureClass::Retryable).await?;
                Err(error)
            }
            Ok(snapshot) => match transaction.commit().await {
                Ok(()) => Ok(snapshot),
                Err(error) => {
                    let _ = self.mark_activation_unknown(acquisition).await?;
                    match self.reconcile_activation(acquisition).await? {
                        SourceAcquireOutcome::Ready(snapshot) => Ok(snapshot),
                        SourceAcquireOutcome::Failed { class, .. } => bail!(
                            "ambiguous libsql registration settled failed: {}",
                            class.as_str()
                        ),
                        _ => Err(error)
                            .context("ambiguous libsql source registration did not settle"),
                    }
                }
            },
        }
    }

    pub async fn mark_activation_unknown(
        &self,
        acquisition: &GitSourceAcquisition,
    ) -> Result<bool> {
        let connection = self.database.connect()?;
        Ok(connection.execute("UPDATE git_source_acquisitions SET state='activation_unknown',expires_at=0 WHERE token=? AND generation=? AND operation_id=? AND state='graph_published'",values![acquisition.token.clone().into(),(acquisition.generation as i64).into(),acquisition.operation_id.clone().into()]).await?==1)
    }

    pub async fn reconcile_activation(
        &self,
        acquisition: &GitSourceAcquisition,
    ) -> Result<SourceAcquireOutcome> {
        let connection = self.database.connect()?;
        let transaction = connection
            .transaction_with_behavior(::libsql::TransactionBehavior::Immediate)
            .await?;
        let result:Result<SourceAcquireOutcome>=async{
            let mut rows=transaction.query("SELECT state FROM git_source_acquisitions WHERE token=? AND generation=? AND operation_id=? AND root_hash=?",values![acquisition.token.clone().into(),(acquisition.generation as i64).into(),acquisition.operation_id.clone().into(),acquisition.root.hash.clone().into()]).await?;
            let row=rows.next().await?.context("libsql source activation acquisition missing")?;
            let state:String=row.get(0)?;drop(rows);
            let registered=one_i64_params(&transaction,"SELECT count(*) FROM git_source_roots WHERE root_hash=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND registration_operation=? AND registration_generation=? AND state='registered'",values![acquisition.root.hash.clone().into(),acquisition.workspace.clone().into(),acquisition.repo.clone().into(),acquisition.commit.clone().into(),(acquisition.source_format_version as i64).into(),acquisition.operation_id.clone().into(),(acquisition.generation as i64).into()]).await?;
            if registered==1{
                if state=="activation_unknown"{
                    if transaction.execute("UPDATE git_source_acquisitions SET state='registered' WHERE token=? AND generation=? AND state='activation_unknown'",values![acquisition.token.clone().into(),(acquisition.generation as i64).into()]).await?!=1{bail!("unknown libsql source settlement lost")}
                    if transaction.execute("UPDATE git_source_desires SET state='registered',root_hash=?,failure_class=NULL,acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'",values![acquisition.root.hash.clone().into(),acquisition.token.clone().into()]).await?!=1{bail!("unknown libsql source desire settlement lost")}
                }else if state!="registered"{bail!("registered libsql source has impossible acquisition state")}
                return Ok(SourceAcquireOutcome::Ready(DurableSourceSnapshot::registered(acquisition.workspace.clone(),acquisition.repo.clone(),acquisition.commit.clone(),acquisition.root.hash.clone(),acquisition.token.clone(),acquisition.generation)?))
            }
            if state!="activation_unknown"{bail!("libsql source activation is not unknown")}
            if transaction.execute("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=unixepoch() WHERE acquisition_token=? AND state='acquiring'",[acquisition.token.clone()]).await?!=1{bail!("uncommitted libsql source desire settlement lost")}
            if transaction.execute("UPDATE git_source_acquisitions SET state='failed',failure_class='retryable' WHERE token=? AND generation=? AND state='activation_unknown'",values![acquisition.token.clone().into(),(acquisition.generation as i64).into()]).await?!=1{bail!("uncommitted libsql source settlement lost")}
            let retries=one_i64_params(&transaction,"SELECT retry_count FROM git_source_desires WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=?",values![acquisition.workspace.clone().into(),acquisition.repo.clone().into(),acquisition.commit.clone().into(),(acquisition.source_format_version as i64).into()]).await?;
            Ok(SourceAcquireOutcome::Failed{class:FailureClass::Retryable,retries:checked_u32(retries,"source retry count")?})
        }.await;
        finish(transaction, result).await
    }

    pub async fn source_gc_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<SourceGcObject>> {
        if limit == 0 || limit > SOURCE_ROOT_PAGE_MAX {
            bail!("libsql source GC page limit is invalid")
        }
        let (hash, owner) = after.unwrap_or(("", ""));
        let connection = self.database.connect()?;
        let mut rows = connection.query("WITH objects(hash,len,owner) AS (SELECT root_hash,root_len,'r:'||root_hash FROM git_source_roots UNION ALL SELECT child_hash,child_len,'r:'||root_hash||':'||printf('%020d',ordinal) FROM git_source_members UNION ALL SELECT root_hash,root_len,'a:'||token FROM git_source_acquisitions WHERE state='activation_unknown' OR (state='graph_published' AND expires_at>unixepoch()) UNION ALL SELECT m.child_hash,m.child_len,'a:'||m.token||':'||printf('%020d',m.ordinal) FROM git_source_acquisition_members m JOIN git_source_acquisitions a ON a.token=m.token WHERE a.state='activation_unknown' OR (a.state='graph_published' AND a.expires_at>unixepoch())) SELECT hash,len,owner FROM objects WHERE hash>? OR (hash=? AND owner>?) ORDER BY hash,owner LIMIT ?", values![hash.into(),hash.into(),owner.into(),(limit as i64).into()]).await?;
        let mut objects = Vec::new();
        while let Some(row) = rows.next().await? {
            let length: i64 = row.get(1)?;
            objects.push(SourceGcObject {
                hash: row.get(0)?,
                len: u64::try_from(length).context("libsql source GC length is negative")?,
                owner: row.get(2)?,
            });
        }
        Ok(objects)
    }

    fn source_fingerprint(&self) -> String {
        let source = &self.source_limits;
        let scheduler = &self.scheduler_limits;
        let mut hash = Sha256::new();
        for value in [
            source.max_manifest_bytes,
            source.max_packs as u64,
            source.max_pack_bytes,
            source.max_index_bytes,
            source.max_total_pack_bytes,
            source.max_objects as u64,
            source.max_object_bytes,
            source.max_total_object_bytes,
            source.target_pack_raw_bytes,
        ] {
            hash.update(value.to_be_bytes());
        }
        hash.update(self.seal.as_ref());
        hash.update(SOURCE_FORMAT_VERSION.to_be_bytes());
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
            hash.update((value as u64).to_be_bytes());
        }
        hash.update(scheduler.max_claim_attempts.to_be_bytes());
        hash.update(scheduler.max_manual_retries.to_be_bytes());
        hex::encode(hash.finalize())
    }
}

pub(crate) async fn install_v7_schema(connection: &Connection) -> Result<()> {
    for statement in SQLITE_V7_SCHEMA.split(';').map(str::trim) {
        if !statement.is_empty() {
            connection.execute(statement, ()).await.with_context(|| {
                format!("install libsql v7 source object: {}", ddl_name(statement))
            })?;
        }
    }
    Ok(())
}

pub(crate) async fn validate_v7_schema(connection: &Connection) -> Result<()> {
    let expected = expected_sqlite_v7_ddl()?;
    let mut rows = connection
        .query(
            "SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL AND (name LIKE 'git_source_%' OR name LIKE 'branch_source_%' OR name LIKE 'artifact_intents%' OR tbl_name IN('git_source_roots','git_source_members','git_source_acquisition_sequence','git_source_acquisitions','git_source_acquisition_members','git_source_desires','branch_source_generations','branch_source_current','git_source_consumers','artifact_intents','git_source_maintenance')) ORDER BY name",
            (),
        )
        .await?;
    let mut actual = std::collections::BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let name = row.get::<String>(0)?;
        let sql = row.get::<String>(1)?;
        if actual.insert(name, canonical_ddl(&sql)).is_some() {
            bail!("libsql v7 source namespace repeats an object")
        }
    }
    drop(rows);
    if actual != expected {
        bail!("libsql v7 source namespace differs from the exact canonical inventory")
    }

    // These checks deliberately duplicate the canonical SQLite relational
    // proof against the remote authority. DDL identity alone cannot prove that
    // an already-published marker describes valid graph state.
    let singleton = query_pairs(
        connection,
        "SELECT id,generation FROM git_source_acquisition_sequence",
    )
    .await?;
    let max_generation = one_i64(
        connection,
        "SELECT COALESCE(MAX(generation),0) FROM git_source_acquisitions",
    )
    .await?;
    if singleton.len() != 1 || singleton[0].0 != 1 || singleton[0].1 < max_generation {
        bail!("libsql Git source acquisition sequence is invalid")
    }
    let mut operation_rows = connection
        .query(
            "SELECT generation,workspace,repo,commit_oid,attempt_id,operation_id FROM git_source_acquisitions",
            (),
        )
        .await?;
    while let Some(row) = operation_rows.next().await? {
        let generation: i64 = row.get(0)?;
        let workspace: String = row.get(1)?;
        let repo: String = row.get(2)?;
        let commit: String = row.get(3)?;
        let attempt: String = row.get(4)?;
        let stored: String = row.get(5)?;
        if stored != operation_id(&workspace, &repo, &commit, &attempt, generation) {
            bail!("libsql source acquisition operation provenance is invalid")
        }
    }
    drop(operation_rows);
    let maintenance = one_i64(connection, "SELECT CASE WHEN count(*)<>1 THEN 1 ELSE COALESCE(MAX(CASE WHEN id<>1 OR intent_cursor<0 OR acquisition_cursor<0 OR (root_cursor<>'' AND (length(root_cursor)<>64 OR root_cursor GLOB '*[^0-9a-f]*')) OR (config_fingerprint<>'' AND (length(config_fingerprint)<>64 OR config_fingerprint GLOB '*[^0-9a-f]*')) THEN 1 ELSE 0 END),1) END FROM git_source_maintenance").await?;
    let invalid_graphs = one_i64(connection, "SELECT count(*) FROM git_source_acquisitions a WHERE length(a.token)<>64 OR a.token GLOB '*[^0-9a-f]*' OR (a.state='held' AND EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token)) OR (a.state IN('graph_published','activation_unknown','registered') AND (a.root_hash IS NULL OR NOT EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token))) OR EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token GROUP BY m.token HAVING MIN(m.ordinal)<>0 OR MAX(m.ordinal)+1<>count(*) OR count(*)%2<>0 OR SUM(CASE WHEN (m.ordinal%2=0 AND m.kind='pack') OR (m.ordinal%2=1 AND m.kind='index') THEN 0 ELSE 1 END)<>0 OR SUM(m.child_len)<>a.total_bytes)").await?;
    let invalid_roots = one_i64(connection, "SELECT count(*) FROM git_source_roots r WHERE length(r.root_hash)<>64 OR r.root_hash GLOB '*[^0-9a-f]*' OR length(r.semantic_digest)<>64 OR r.semantic_digest GLOB '*[^0-9a-f]*' OR length(r.object_set_digest)<>64 OR r.object_set_digest GLOB '*[^0-9a-f]*' OR NOT EXISTS(SELECT 1 FROM git_source_members m WHERE m.root_hash=r.root_hash GROUP BY m.root_hash HAVING MIN(m.ordinal)=0 AND MAX(m.ordinal)+1=count(*) AND count(*)%2=0 AND SUM(m.child_len)=r.total_bytes AND SUM(CASE WHEN (m.ordinal%2=0 AND m.kind='pack') OR (m.ordinal%2=1 AND m.kind='index') THEN 0 ELSE 1 END)=0) OR EXISTS(SELECT 1 FROM git_source_members m WHERE m.root_hash=r.root_hash AND (length(m.child_hash)<>64 OR m.child_hash GLOB '*[^0-9a-f]*'))").await?;
    let invalid_registered = one_i64(connection, "SELECT count(*) FROM git_source_acquisitions a LEFT JOIN git_source_roots r ON r.root_hash=a.root_hash WHERE a.state='registered' AND (r.root_hash IS NULL OR r.state<>'registered' OR r.root_len<>a.root_len OR r.workspace<>a.workspace OR r.repo<>a.repo OR r.commit_oid<>a.commit_oid OR r.source_format_version<>a.source_format_version OR r.object_format<>a.object_format OR r.semantic_digest<>a.semantic_digest OR r.object_set_digest<>a.object_set_digest OR r.object_count<>a.object_count OR r.total_bytes<>a.total_bytes OR r.registration_operation<>a.operation_id OR r.registration_generation<>a.generation OR EXISTS(SELECT 1 FROM git_source_acquisition_members am LEFT JOIN git_source_members m ON m.root_hash=r.root_hash AND m.ordinal=am.ordinal WHERE am.token=a.token AND (m.ordinal IS NULL OR m.child_hash<>am.child_hash OR m.child_len<>am.child_len OR m.kind<>am.kind)) OR EXISTS(SELECT 1 FROM git_source_members m LEFT JOIN git_source_acquisition_members am ON am.token=a.token AND am.ordinal=m.ordinal WHERE m.root_hash=r.root_hash AND am.ordinal IS NULL))").await?;
    let roots_without_registration = one_i64(connection, "SELECT count(*) FROM git_source_roots r WHERE r.state='registered' AND NOT EXISTS(SELECT 1 FROM git_source_acquisitions a WHERE a.state='registered' AND a.root_hash=r.root_hash AND a.root_len=r.root_len AND a.workspace=r.workspace AND a.repo=r.repo AND a.commit_oid=r.commit_oid AND a.source_format_version=r.source_format_version AND a.object_format=r.object_format AND a.semantic_digest=r.semantic_digest AND a.object_set_digest=r.object_set_digest AND a.object_count=r.object_count AND a.total_bytes=r.total_bytes AND a.operation_id=r.registration_operation AND a.generation=r.registration_generation)").await?;
    let conflicting_descriptors = one_i64(connection, "SELECT count(*) FROM (SELECT hash,count(DISTINCT len||':'||kind) variants FROM (SELECT root_hash hash,root_len len,'root' kind FROM git_source_roots UNION ALL SELECT root_hash,root_len,'root' FROM git_source_acquisitions WHERE root_hash IS NOT NULL UNION ALL SELECT child_hash,child_len,kind FROM git_source_members UNION ALL SELECT child_hash,child_len,kind FROM git_source_acquisition_members) GROUP BY hash HAVING variants<>1)").await?;
    let root_child_aliases = one_i64(connection, "SELECT count(*) FROM (SELECT root_hash hash FROM git_source_roots UNION SELECT root_hash FROM git_source_acquisitions WHERE root_hash IS NOT NULL) r JOIN (SELECT child_hash hash FROM git_source_members UNION SELECT child_hash FROM git_source_acquisition_members) m USING(hash)").await?;
    let invalid_desires = one_i64(connection, "SELECT count(*) FROM git_source_desires d LEFT JOIN git_source_acquisitions a ON a.token=d.acquisition_token LEFT JOIN git_source_roots r ON r.root_hash=d.root_hash WHERE d.source_format_version<>1 OR (d.state='acquiring' AND (a.token IS NULL OR a.workspace<>d.workspace OR a.repo<>d.repo OR a.commit_oid<>d.commit_oid OR a.source_format_version<>d.source_format_version OR a.state NOT IN('held','graph_published','activation_unknown'))) OR (d.state='registered' AND (r.root_hash IS NULL OR r.workspace<>d.workspace OR r.repo<>d.repo OR r.commit_oid<>d.commit_oid OR r.source_format_version<>d.source_format_version OR r.state<>'registered'))").await?;
    let orphan_acquisitions = one_i64(connection, "SELECT count(*) FROM git_source_acquisitions a WHERE (a.state IN('held','graph_published','activation_unknown') AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='acquiring' AND d.acquisition_token=a.token AND d.workspace=a.workspace AND d.repo=a.repo AND d.commit_oid=a.commit_oid AND d.source_format_version=a.source_format_version)) OR (a.state='registered' AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='registered' AND d.root_hash=a.root_hash AND d.workspace=a.workspace AND d.repo=a.repo AND d.commit_oid=a.commit_oid AND d.source_format_version=a.source_format_version))").await?;
    let orphan_roots = one_i64(connection, "SELECT count(*) FROM git_source_roots r WHERE r.state='registered' AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='registered' AND d.root_hash=r.root_hash AND d.workspace=r.workspace AND d.repo=r.repo AND d.commit_oid=r.commit_oid AND d.source_format_version=r.source_format_version)").await?;
    let invalid_branches = one_i64(connection, "SELECT count(*) FROM branch_source_current c JOIN branch_source_generations g USING(workspace,repo,branch,generation) LEFT JOIN branch_observations b USING(workspace,repo,branch) WHERE b.workspace IS NULL OR b.generation<>g.generation OR b.desired_commit<>g.commit_oid OR NOT EXISTS(SELECT 1 FROM git_source_roots r JOIN git_source_desires d ON d.root_hash=r.root_hash AND d.workspace=r.workspace AND d.repo=r.repo AND d.commit_oid=r.commit_oid AND d.source_format_version=r.source_format_version WHERE r.root_hash=g.root_hash AND r.workspace=g.workspace AND r.repo=g.repo AND r.commit_oid=g.commit_oid AND r.source_format_version=g.source_format_version AND r.state='registered' AND d.state='registered')").await?;
    let invalid_intents = one_i64(connection, "SELECT count(*) FROM artifact_intents i JOIN branch_source_generations g ON g.workspace=i.workspace AND g.repo=i.repo AND g.branch=i.branch AND g.generation=i.branch_generation LEFT JOIN git_source_consumers c ON c.root_hash=i.source_root_hash AND c.consumer_id=i.consumer_id LEFT JOIN artifact_jobs j ON j.id=i.artifact_id LEFT JOIN git_source_desires d ON d.workspace=i.workspace AND d.repo=i.repo AND d.commit_oid=i.commit_oid WHERE length(i.consumer_id)<>55 OR substr(i.consumer_id,1,7)<>'intent:' OR substr(i.consumer_id,8) GLOB '*[^0-9a-f]*' OR (SELECT count(*) FROM artifact_intents sibling WHERE sibling.consumer_id=i.consumer_id)<>1 OR g.root_hash<>i.source_root_hash OR g.commit_oid<>i.commit_oid OR d.workspace IS NULL OR d.source_format_version<>i.source_format_version OR d.state<>'registered' OR d.root_hash<>i.source_root_hash OR c.consumer_id IS NULL OR c.purpose<>'intent' OR c.expires_at<>9223372036854775807 OR (i.state='promoted' AND (j.id IS NULL OR j.workspace<>i.workspace OR j.repo<>i.repo OR j.commit_oid<>i.commit_oid OR j.kind<>i.kind OR j.format_version<>i.format_version))").await?;
    let invalid_intent_consumers = one_i64(connection, "SELECT count(*) FROM artifact_intents i WHERE (i.state='deferred' AND EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id)) OR (i.state='promoted' AND ((SELECT count(*) FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id)<>1 OR NOT EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id AND ac.artifact_id=i.artifact_id AND ac.expires_at=9223372036854775807)))").await?;
    let orphan_source_consumers = one_i64(connection, "SELECT count(*) FROM git_source_consumers c WHERE c.purpose='intent' AND (length(c.consumer_id)<>55 OR substr(c.consumer_id,1,7)<>'intent:' OR substr(c.consumer_id,8) GLOB '*[^0-9a-f]*' OR length(c.session_id)<>64 OR c.session_id GLOB '*[^0-9a-f]*' OR c.expires_at<>9223372036854775807 OR NOT EXISTS(SELECT 1 FROM artifact_intents i WHERE i.consumer_id=c.consumer_id AND i.source_root_hash=c.root_hash AND i.workspace=c.workspace AND i.repo=c.repo AND i.commit_oid=c.commit_oid AND i.source_format_version=c.source_format_version))").await?;
    let orphan_artifact_consumers = one_i64(connection, "SELECT count(*) FROM artifact_consumers ac WHERE substr(ac.consumer_id,1,7)='intent:' AND (ac.expires_at<>9223372036854775807 OR (SELECT count(*) FROM artifact_intents i WHERE i.state='promoted' AND i.consumer_id=ac.consumer_id AND i.artifact_id=ac.artifact_id)<>1)").await?;
    if maintenance
        + invalid_graphs
        + invalid_roots
        + invalid_registered
        + roots_without_registration
        + conflicting_descriptors
        + root_child_aliases
        + invalid_desires
        + orphan_acquisitions
        + orphan_roots
        + invalid_branches
        + invalid_intents
        + invalid_intent_consumers
        + orphan_source_consumers
        + orphan_artifact_consumers
        != 0
    {
        bail!("libsql Git source registry relational state is invalid")
    }
    Ok(())
}

async fn one_i64(connection: &Connection, sql: &str) -> Result<i64> {
    let mut rows = connection.query(sql, ()).await?;
    let row = rows.next().await?.context("libsql scalar row missing")?;
    let value = row.get(0)?;
    if rows.next().await?.is_some() {
        bail!("libsql scalar query returned multiple rows")
    }
    Ok(value)
}

async fn one_i64_params(connection: &Connection, sql: &str, params: Vec<Value>) -> Result<i64> {
    let mut rows = connection.query(sql, params).await?;
    let row = rows.next().await?.context("libsql scalar row missing")?;
    let value = row.get(0)?;
    if rows.next().await?.is_some() {
        bail!("libsql scalar query returned multiple rows")
    }
    Ok(value)
}

async fn assert_exact_graph(
    connection: &Connection,
    acquisition: &GitSourceAcquisition,
    view: &crate::git_source::GitSourceRegistryView,
    now: i64,
) -> Result<()> {
    let found = one_i64_params(connection,"SELECT count(*) FROM git_source_acquisitions WHERE token=? AND generation=? AND operation_id=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND root_hash=? AND root_len=? AND state='graph_published' AND expires_at>?",values![acquisition.token.clone().into(),(acquisition.generation as i64).into(),acquisition.operation_id.clone().into(),view.workspace.clone().into(),view.repo.clone().into(),view.commit.clone().into(),(view.source_format_version as i64).into(),view.root.hash.clone().into(),checked_i64(view.root.len,"root length")?.into(),now.into()]).await?;
    if found != 1 {
        bail!("libsql source acquisition is stale or mismatched")
    }
    let mut rows = connection
        .query(
            "SELECT ordinal,child_hash,child_len,kind FROM git_source_acquisition_members WHERE token=? ORDER BY ordinal",
            [acquisition.token.clone()],
        )
        .await?;
    let mut ordinal = 0usize;
    while let Some(row) = rows.next().await? {
        let expected = view
            .members
            .get(ordinal)
            .context("libsql source acquisition has extra members")?;
        if row.get::<i64>(0)? != expected.ordinal as i64
            || row.get::<String>(1)? != expected.blob.hash
            || row.get::<i64>(2)? != checked_i64(expected.blob.len, "member length")?
            || row.get::<String>(3)? != expected.kind
        {
            bail!("libsql source acquisition graph changed")
        }
        ordinal += 1;
    }
    if ordinal != view.members.len() {
        bail!("libsql source acquisition graph is incomplete")
    }
    Ok(())
}

async fn one_string(connection: &Connection, sql: &str) -> Result<String> {
    let mut rows = connection.query(sql, ()).await?;
    let row = rows.next().await?.context("libsql scalar row missing")?;
    let value = row.get(0)?;
    if rows.next().await?.is_some() {
        bail!("libsql scalar query returned multiple rows")
    }
    Ok(value)
}

async fn one_maintenance(
    connection: &Connection,
) -> Result<(i64, String, i64, String, i64, String, i64)> {
    let mut rows = connection
        .query(
            "SELECT id,config_fingerprint,intent_cursor,intent_workspace_cursor,acquisition_cursor,root_cursor,updated_at FROM git_source_maintenance",
            (),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .context("libsql maintenance row missing")?;
    let value = (
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
    );
    if rows.next().await?.is_some() {
        bail!("libsql maintenance singleton has multiple rows")
    }
    Ok(value)
}

async fn finish<T>(transaction: ::libsql::Transaction, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => {
            transaction.commit().await?;
            Ok(value)
        }
        Err(error) => match transaction.rollback().await {
            Ok(()) => Err(error),
            Err(rollback) => Err(error).context(format!(
                "rollback libsql source registry transaction failed: {rollback}"
            )),
        },
    }
}

async fn query_pairs(connection: &Connection, sql: &str) -> Result<Vec<(i64, i64)>> {
    let mut rows = connection.query(sql, ()).await?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().await? {
        result.push((row.get(0)?, row.get(1)?));
    }
    Ok(result)
}

fn ddl_name(statement: &str) -> &str {
    statement
        .split_whitespace()
        .nth(2)
        .unwrap_or("source object")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{ClaimedArtifact, CompletionEvidence, CompletionVerifier};
    use crate::artifact_scheduler_libsql::LibsqlArtifactScheduler;
    use crate::storage::LocalStorage;
    use std::net::TcpListener;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use std::time::Duration;

    struct Server {
        child: Child,
        _dir: tempfile::TempDir,
        url: String,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
    async fn server() -> Option<Server> {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dir = tempfile::tempdir().unwrap();
        let mut child = match Command::new("sqld")
            .arg("--db-path")
            .arg(dir.path().join("db"))
            .arg("--http-listen-addr")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--http-self-url")
            .arg(format!("http://127.0.0.1:{port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) if std::env::var_os("RIPCLONE_REQUIRE_SQLD_TESTS").is_some() => {
                panic!("required sqld source-registry server unavailable: {error}")
            }
            Err(_) => return None,
        };
        let url = format!("http://127.0.0.1:{port}");
        for _ in 0..100 {
            if let Ok(db) = ::libsql::Builder::new_remote(url.clone(), String::new())
                .build()
                .await
                && let Ok(connection) = db.connect()
                && connection.query("SELECT 1", ()).await.is_ok()
            {
                return Some(Server {
                    child,
                    _dir: dir,
                    url,
                });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let _ = child.kill();
        panic!("sqld source-registry server did not become ready")
    }
    async fn connection() -> Option<(Server, ::libsql::Database, Connection)> {
        let server = server().await?;
        let database = ::libsql::Builder::new_remote(server.url.clone(), String::new())
            .build()
            .await
            .unwrap();
        let connection = database.connect().unwrap();
        Some((server, database, connection))
    }

    struct Accept;
    impl CompletionVerifier for Accept {
        fn identity(&self) -> &'static str {
            "libsql-source-registry-tests-v1"
        }
        fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
            crate::artifact_scheduler::validate_evidence(claim, evidence)
        }
    }

    #[tokio::test]
    async fn exact_source_schema_round_trip_and_planted_ddl_rejected() {
        let Some((_server, _db, connection)) = connection().await else {
            return;
        };
        connection
            .execute("PRAGMA foreign_keys=ON", ())
            .await
            .unwrap();
        connection
            .execute(
                "CREATE TABLE artifact_jobs(id INTEGER PRIMARY KEY,workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,kind TEXT NOT NULL,format_version INTEGER NOT NULL,state TEXT NOT NULL)",
                (),
            )
            .await
            .unwrap();
        connection
            .execute(
                "CREATE TABLE artifact_consumers(artifact_id INTEGER NOT NULL,consumer_id TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(artifact_id,consumer_id))",
                (),
            )
            .await
            .unwrap();
        connection
            .execute(
                "CREATE TABLE branch_observations(workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation INTEGER NOT NULL,desired_commit TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch))",
                (),
            )
            .await
            .unwrap();
        install_v7_schema(&connection).await.unwrap();
        validate_v7_schema(&connection).await.unwrap();
        connection
            .execute("DROP INDEX artifact_intents_source", ())
            .await
            .unwrap();
        connection
            .execute(
                "CREATE INDEX artifact_intents_source ON artifact_intents(state,source_root_hash)",
                (),
            )
            .await
            .unwrap();
        assert!(validate_v7_schema(&connection).await.is_err());
    }

    #[tokio::test]
    async fn partial_source_namespace_is_detected() {
        let Some((_server, _db, connection)) = connection().await else {
            return;
        };
        connection
            .execute("CREATE TABLE git_source_roots(planted TEXT)", ())
            .await
            .unwrap();
        assert!(validate_v7_schema(&connection).await.is_err());
    }

    #[tokio::test]
    async fn identity_first_held_coalesces_and_failed_observation_is_suppressed() {
        let Some((server, database, _connection)) = connection().await else {
            return;
        };
        let limits = SchedulerLimits::default();
        let shared = Arc::new(database);
        LibsqlArtifactScheduler::from_shared_database(
            shared.clone(),
            limits.clone(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        let storage_dir = tempfile::tempdir().unwrap();
        let storage: StorageRef = Arc::new(LocalStorage::new(storage_dir.path()).unwrap());
        let registry = LibsqlGitSourceRegistry::new(
            shared,
            storage,
            limits,
            GitSourceLimits::default(),
            [9; 32],
        )
        .await
        .unwrap();
        let commit = "0123456789012345678901234567890123456789";
        let first = registry
            .begin_acquisition(
                "ws",
                "o/r",
                commit,
                SOURCE_FORMAT_VERSION,
                "worker-a",
                "attempt-a",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        let SourceBeginOutcome::PermitToPrepare(permit) = first else {
            panic!("first acquisition did not mint a Held preparation permit")
        };
        assert!(matches!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    commit,
                    SOURCE_FORMAT_VERSION,
                    "worker-b",
                    "attempt-b",
                    60,
                    SyncIntent::EnsureCurrent,
                )
                .await
                .unwrap(),
            SourceBeginOutcome::Deferred { .. }
        ));
        assert!(
            registry
                .fail_preparation(&permit, FailureClass::Retryable)
                .await
                .unwrap()
        );
        assert!(matches!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    commit,
                    SOURCE_FORMAT_VERSION,
                    "observer",
                    "observe",
                    60,
                    SyncIntent::ObserveMovement,
                )
                .await
                .unwrap(),
            SourceBeginOutcome::Failed {
                class: FailureClass::Retryable,
                ..
            }
        ));
        drop(server);
    }
}
