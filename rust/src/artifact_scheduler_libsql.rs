//! Fleet-safe normalized artifact scheduling on remote libSQL/Turso.
//!
//! Admission and claim serialize through a single, tiny control-row write
//! transaction. Heartbeat, ownership checks, and settlement are fenced O(1)
//! statements against the claimed job. No operation rewrites global state.

#[cfg(test)]
use crate::artifact_scheduler::CompletionEvidence;
use crate::artifact_scheduler::{
    ArtifactKey, ArtifactKind, ArtifactRecord, ArtifactState, ClaimedArtifact,
    CompletionSealAuthority, CompletionVerifier, FailureClass, ObservationOutcome, RetryOutcome,
    ScheduleOutcome, SchedulerLimits, VerifiedCompletionEvidence, scheduler_fingerprint,
    ObservationSnapshot, validate_format_version, validate_lease, validate_limits,
    validate_observation_identity, validate_resolved_commit,
};
use crate::artifact_scheduler_backend::ArtifactSchedulerPersistence;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use libsql::{Connection, Database, Row, Transaction, TransactionBehavior, Value};
use std::sync::Arc;

const VERSION: i64 = 1;
const PROVENANCE: &str = "ripclone-artifact-scheduler-libsql-v1";
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS artifact_scheduler_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,provenance TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS artifact_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN('head','full_history','files')),format_version INTEGER NOT NULL CHECK(format_version BETWEEN 1 AND 4294967295),state TEXT NOT NULL CHECK(state IN('queued','running','ready','failed')),owner TEXT,heartbeat_at INTEGER,lease_expires_at INTEGER,lease_generation INTEGER NOT NULL DEFAULT 0 CHECK(lease_generation>=0),claim_attempts INTEGER NOT NULL DEFAULT 0 CHECK(claim_attempts BETWEEN 0 AND 4294967295),retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count BETWEEN 0 AND 4294967295),manifest TEXT,error TEXT,failure_class TEXT CHECK(failure_class IS NULL OR failure_class IN('retryable','permanent','dead_letter')),created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,UNIQUE(workspace,repo,commit_oid,kind,format_version))",
    "CREATE INDEX IF NOT EXISTS artifact_jobs_claim ON artifact_jobs(state,kind,created_at,id)",
    "CREATE INDEX IF NOT EXISTS artifact_jobs_lease ON artifact_jobs(state,lease_expires_at)",
    "CREATE TABLE IF NOT EXISTS branch_observations(workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>=1),desired_commit TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch))",
    "CREATE TABLE IF NOT EXISTS artifact_observations(workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN('head','full_history','files')),desired_commit TEXT NOT NULL,desired_artifact_id INTEGER NOT NULL,desired_generation INTEGER NOT NULL CHECK(desired_generation>=1),published_artifact_id INTEGER,format_version INTEGER NOT NULL CHECK(format_version BETWEEN 1 AND 4294967295),observed_at INTEGER NOT NULL,PRIMARY KEY(workspace,repo,branch,kind))",
    "CREATE INDEX IF NOT EXISTS artifact_observations_desired ON artifact_observations(desired_artifact_id)",
    "CREATE INDEX IF NOT EXISTS artifact_observations_published ON artifact_observations(published_artifact_id)",
    "CREATE TABLE IF NOT EXISTS artifact_consumers(artifact_id INTEGER NOT NULL,consumer_id TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(artifact_id,consumer_id))",
    "CREATE INDEX IF NOT EXISTS artifact_consumers_expiry ON artifact_consumers(expires_at)",
    "CREATE TABLE IF NOT EXISTS scheduler_state(id INTEGER PRIMARY KEY CHECK(id=1),fairness_cursor INTEGER NOT NULL CHECK(fairness_cursor BETWEEN 0 AND 3),workspace_cursor TEXT NOT NULL DEFAULT '',config_fingerprint TEXT NOT NULL DEFAULT '')",
];

#[derive(Clone)]
pub struct LibsqlArtifactScheduler {
    db: Arc<Database>,
    limits: SchedulerLimits,
    verifier: Arc<dyn CompletionVerifier>,
    completion_sealer: Arc<CompletionSealAuthority>,
}

impl LibsqlArtifactScheduler {
    pub async fn connect_remote(
        url: &str,
        token: &str,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        let db = libsql::Builder::new_remote(url.to_owned(), token.to_owned())
            .build()
            .await
            .with_context(|| format!("open libsql artifact scheduler {url}"))?;
        Self::from_database(db, limits, verifier).await
    }
    pub async fn from_database(
        db: Database,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        Self::from_shared_database(Arc::new(db), limits, verifier).await
    }
    pub async fn from_shared_database(
        db: Arc<Database>,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        validate_limits(&limits)?;
        let identity = verifier.identity().trim();
        if identity.is_empty() {
            bail!("completion verifier identity is empty")
        }
        let conn = db.connect()?;
        // Remote libSQL transactions provide the fleet-wide migration mutex.
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let existing=one_i64(&tx,"SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN('artifact_scheduler_schema','artifact_jobs','branch_observations','artifact_observations','artifact_consumers','scheduler_state')",vec![]).await?;
        if existing == 0 {
            for ddl in SCHEMA {
                tx.execute(ddl, ()).await?;
            }
            tx.execute(
                "INSERT INTO artifact_scheduler_schema(id,version,provenance) VALUES(1,?,?)",
                libsql::params![VERSION, PROVENANCE],
            )
            .await?;
            tx.execute(
                "INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)",
                (),
            )
            .await?;
        } else if existing != 6 {
            bail!("partial or unprovenanced libsql artifact scheduler schema")
        }
        validate_schema(&tx).await?;
        let fingerprint = scheduler_fingerprint(&limits, identity);
        let stored = one_string(
            &tx,
            "SELECT config_fingerprint FROM scheduler_state WHERE id=1",
            vec![],
        )
        .await?;
        if stored.is_empty() {
            let existing=one_i64(&tx,"SELECT (SELECT count(*) FROM artifact_jobs)+(SELECT count(*) FROM branch_observations)+(SELECT count(*) FROM artifact_observations)+(SELECT count(*) FROM artifact_consumers)",vec![]).await?;
            if existing != 0 {
                bail!("unprovenanced libsql scheduler state is not empty")
            }
            let changed=exec(&tx,"UPDATE scheduler_state SET config_fingerprint=? WHERE id=1 AND config_fingerprint=''",vec![fingerprint.clone().into()]).await?;
            if changed != 1 {
                bail!("scheduler configuration CAS failed")
            }
        } else if stored != fingerprint {
            bail!("scheduler running-limit configuration differs from existing fleet")
        }
        tx.commit().await?;
        let completion_sealer = Arc::new(CompletionSealAuthority::new(verifier.identity())?);
        Ok(Self {
            db,
            limits,
            verifier,
            completion_sealer,
        })
    }
    async fn conn(&self) -> Result<Connection> {
        self.db.connect().context("libsql scheduler connect")
    }
    async fn tx(&self) -> Result<Transaction> {
        Ok(self
            .conn()
            .await?
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?)
    }
    async fn schedule_in(
        &self,
        tx: &Transaction,
        key: &ArtifactKey,
        preflight: bool,
    ) -> Result<ScheduleOutcome> {
        if let Some(r) = get_key(tx, key).await? {
            return Ok(match r.state {
                ArtifactState::Ready => ScheduleOutcome::AlreadyReady(r.id),
                ArtifactState::Failed => ScheduleOutcome::Failed(
                    r.id,
                    r.failure_class.unwrap_or(FailureClass::Permanent),
                ),
                _ => ScheduleOutcome::Subscribed(r.id),
            });
        }
        if preflight {
            self.preflight(tx, &key.workspace, &[key.kind]).await?
        }
        let now = now(tx).await?;
        exec(tx,"INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?)",vec![key.workspace.clone().into(),key.repo.clone().into(),key.commit.clone().into(),key.kind.as_str().into(),(key.format_version as i64).into(),now.into(),now.into()]).await?;
        Ok(ScheduleOutcome::Enqueued(tx.last_insert_rowid()))
    }
    async fn preflight(&self, tx: &Transaction, w: &str, kinds: &[ArtifactKind]) -> Result<()> {
        let add = kinds.len() as i64;
        let total = one_i64(
            tx,
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')",
            vec![],
        )
        .await?;
        let workspace = one_i64(
            tx,
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=?",
            vec![w.into()],
        )
        .await?;
        let expensive_add = kinds.iter().filter(|k| k.expensive()).count() as i64;
        let expensive=one_i64(tx,"SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind IN('full_history','files')",vec![]).await?;
        if total + add > self.limits.total_backlog as i64
            || workspace + add > self.limits.workspace_backlog as i64
            || expensive + expensive_add
                > self
                    .limits
                    .total_backlog
                    .saturating_sub(self.limits.head_reserved) as i64
        {
            bail!("artifact queue capacity exhausted for atomic observation batch")
        }
        for k in [
            ArtifactKind::Head,
            ArtifactKind::FullHistory,
            ArtifactKind::Files,
        ] {
            let n = kinds.iter().filter(|x| **x == k).count() as i64;
            if n == 0 {
                continue;
            }
            let count = one_i64(
                tx,
                "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind=?",
                vec![k.as_str().into()],
            )
            .await?;
            if count + n > self.backlog(k) as i64 {
                bail!("artifact queue capacity exhausted for {}", k.as_str())
            }
        }
        Ok(())
    }
    fn backlog(&self, k: ArtifactKind) -> usize {
        match k {
            ArtifactKind::Head => self.limits.head_backlog,
            ArtifactKind::FullHistory => self.limits.full_history_backlog,
            ArtifactKind::Files => self.limits.files_backlog,
        }
    }
    fn running(&self, k: ArtifactKind) -> usize {
        match k {
            ArtifactKind::Head => self.limits.head_running,
            ArtifactKind::FullHistory => self.limits.full_history_running,
            ArtifactKind::Files => self.limits.files_running,
        }
    }
}

#[async_trait]
impl ArtifactSchedulerPersistence for LibsqlArtifactScheduler {
    fn completion_verifier(&self) -> Arc<dyn CompletionVerifier> {
        self.verifier.clone()
    }
    fn completion_sealer(&self) -> Arc<CompletionSealAuthority> {
        self.completion_sealer.clone()
    }
    async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome> {
        validate_format_version(key.format_version)?;
        let tx = self.tx().await?;
        let r = self.schedule_in(&tx, key, true).await;
        finish(tx, r).await
    }
    async fn subscribe_consumer(
        &self,
        key: &ArtifactKey,
        id: &str,
        ttl: i64,
    ) -> Result<ScheduleOutcome> {
        validate_format_version(key.format_version)?;
        if id.trim().is_empty() {
            bail!("artifact consumer id is empty")
        }
        if !(2..=86400).contains(&ttl) {
            bail!("consumer subscription TTL is invalid")
        }
        let tx = self.tx().await?;
        let r=async{let out=self.schedule_in(&tx,key,true).await?;exec(&tx,"INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?) ON CONFLICT(artifact_id,consumer_id) DO UPDATE SET expires_at=excluded.expires_at",vec![outcome_id(&out).into(),id.into(),(now(&tx).await?+ttl).into()]).await?;Ok(out)}.await;
        finish(tx, r).await
    }
    async fn release_consumer(&self, aid: i64, id: &str) -> Result<()> {
        let tx = self.tx().await?;
        let r=async{exec(&tx,"DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?",vec![aid.into(),id.into()]).await?;exec(&tx,"DELETE FROM artifact_jobs WHERE id=? AND state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",vec![aid.into()]).await?;Ok(())}.await;
        finish(tx, r).await
    }
    async fn observe(
        &self,
        w: &str,
        r: &str,
        b: &str,
        c: &str,
        kinds: &[ArtifactKind],
        v: u32,
        expected: Option<u64>,
    ) -> Result<ObservationOutcome> {
        validate_observation_identity(w, r, b, "write")?;
        validate_resolved_commit(c)?;
        validate_format_version(v)?;
        if kinds.is_empty() {
            bail!("observation requests no artifact kinds")
        }
        let mut kinds = kinds.to_vec();
        kinds.sort_by_key(|k| kindex(*k));
        kinds.dedup();
        let tx = self.tx().await?;
        let result=async{
   let current=query_one(&tx,"SELECT generation,desired_commit FROM branch_observations WHERE workspace=? AND repo=? AND branch=?",vec![w.into(),r.into(),b.into()]).await?;
   let current_generation=current.as_ref().map(|row|row.get::<i64>(0)).transpose()?.map(|x|x as u64);
   let same_commit=current.as_ref().map(|row|row.get::<String>(1)).transpose()?.as_deref()==Some(c);
   let mut fully_observed=same_commit;
   if same_commit{for kind in &kinds{fully_observed &= one_i64(&tx,"SELECT count(*) FROM artifact_observations WHERE workspace=? AND repo=? AND branch=? AND kind=? AND desired_commit=? AND format_version=?",vec![w.into(),r.into(),b.into(),kind.as_str().into(),c.into(),(v as i64).into()]).await?==1;}}
   if fully_observed{return Ok(ObservationOutcome::Unchanged{generation:current_generation.context("existing observation has no generation")?})}
   let current=current_generation;
   if current!=expected{return Ok(ObservationOutcome::Stale{current_generation:current.unwrap_or(0)})}let generation=current.unwrap_or(0).checked_add(1).context("observation generation overflow")?;
   for kind in &kinds {exec(&tx,"DELETE FROM artifact_jobs WHERE state='queued' AND id IN(SELECT desired_artifact_id FROM artifact_observations WHERE workspace=? AND repo=? AND branch=? AND kind=?) AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations WHERE NOT(workspace=? AND repo=? AND branch=? AND kind=?)) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",vec![w.into(),r.into(),b.into(),kind.as_str().into(),w.into(),r.into(),b.into(),kind.as_str().into()]).await?;}
   let mut additions=Vec::new();for kind in &kinds {let key=ArtifactKey{workspace:w.into(),repo:r.into(),commit:c.into(),kind:*kind,format_version:v};if get_key(&tx,&key).await?.is_none(){additions.push(*kind)}}self.preflight(&tx,w,&additions).await?;
   let mut artifacts=Vec::new();for kind in kinds {let key=ArtifactKey{workspace:w.into(),repo:r.into(),commit:c.into(),kind,format_version:v};let out=self.schedule_in(&tx,&key,false).await?;let id=outcome_id(&out);let t=now(&tx).await?;exec(&tx,"INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES(?,?,?,?,?,?,?,CASE WHEN (SELECT state FROM artifact_jobs WHERE id=?)='ready' THEN ? ELSE NULL END,?,?) ON CONFLICT(workspace,repo,branch,kind) DO UPDATE SET desired_commit=excluded.desired_commit,desired_artifact_id=excluded.desired_artifact_id,desired_generation=excluded.desired_generation,published_artifact_id=CASE WHEN (SELECT state FROM artifact_jobs WHERE id=excluded.desired_artifact_id)='ready' THEN excluded.desired_artifact_id WHEN artifact_observations.format_version=excluded.format_version THEN artifact_observations.published_artifact_id ELSE NULL END,format_version=excluded.format_version,observed_at=excluded.observed_at",vec![w.into(),r.into(),b.into(),kind.as_str().into(),c.into(),id.into(),(generation as i64).into(),id.into(),id.into(),(v as i64).into(),t.into()]).await?;artifacts.push((kind,out));}
   exec(&tx,"INSERT INTO branch_observations(workspace,repo,branch,generation,desired_commit,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(workspace,repo,branch) DO UPDATE SET generation=excluded.generation,desired_commit=excluded.desired_commit,updated_at=excluded.updated_at",vec![w.into(),r.into(),b.into(),(generation as i64).into(),c.into(),now(&tx).await?.into()]).await?;
   exec(&tx,"DELETE FROM artifact_jobs WHERE workspace=? AND repo=? AND state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",vec![w.into(),r.into()]).await?;Ok(ObservationOutcome::Accepted{generation,artifacts})}.await;
        finish(tx, result).await
    }
    async fn observation_snapshot(&self, w: &str, r: &str, b: &str) -> Result<ObservationSnapshot> {
        validate_observation_identity(w, r, b, "snapshot")?;
        let conn = self.conn().await?;
        let row=query_one(&conn,"SELECT generation,desired_commit FROM branch_observations WHERE workspace=? AND repo=? AND branch=?",vec![w.into(),r.into(),b.into()]).await?;
        Ok(match row {
            Some(row) => ObservationSnapshot::new(
                w,
                r,
                b,
                Some(row.get::<i64>(0)? as u64),
                Some(row.get::<String>(1)?),
            ),
            None => ObservationSnapshot::new(w, r, b, None, None),
        })
    }
    async fn retry_failed(&self, key: &ArtifactKey) -> Result<RetryOutcome> {
        validate_format_version(key.format_version)?;
        let tx = self.tx().await?;
        let result = async {
            let Some(row) = query_one(
                &tx,
                "SELECT id,state,failure_class,retry_count FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?",
                key_params(key),
            )
            .await?
            else {
                return Ok(RetryOutcome::NotFailed);
            };
            let id = row.get::<i64>(0)?;
            if row.get::<String>(1)? != "failed" {
                return Ok(RetryOutcome::NotFailed);
            }
            let class = FailureClass::parse(
                row.get::<Option<String>>(2)?
                    .as_deref()
                    .unwrap_or("permanent"),
            )?;
            if class != FailureClass::Retryable {
                return Ok(RetryOutcome::NotRetryable(class));
            }
            if row.get::<i64>(3)? as u32 >= self.limits.max_manual_retries {
                return Ok(RetryOutcome::Exhausted);
            }
            self.preflight(&tx, &key.workspace, &[key.kind]).await?;
            exec(
                &tx,
                "UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,retry_count=retry_count+1,error=NULL,failure_class=NULL,updated_at=? WHERE id=? AND state='failed'",
                vec![now(&tx).await?.into(), id.into()],
            )
            .await?;
            Ok(RetryOutcome::Requeued(id))
        }
        .await;
        finish(tx, result).await
    }
    async fn claim(&self, owner: &str, lease: i64) -> Result<Option<ClaimedArtifact>> {
        validate_lease(owner, lease)?;
        let tx = self.tx().await?;
        let result=async{
  if one_i64(&tx,"SELECT count(*) FROM artifact_jobs WHERE state='running'",vec![]).await?>=self.limits.total_running as i64{return Ok(None)}let row=query_one(&tx,"SELECT fairness_cursor,workspace_cursor FROM scheduler_state WHERE id=1",vec![]).await?.context("scheduler control row missing")?;let cursor=row.get::<i64>(0)?;let wc=row.get::<String>(1)?;let lanes=[ArtifactKind::Head,ArtifactKind::Head,ArtifactKind::FullHistory,ArtifactKind::Files];
  for off in 0..4 {let pos=(cursor as usize+off)%4;let kind=lanes[pos];if one_i64(&tx,"SELECT count(*) FROM artifact_jobs WHERE state='running' AND kind=?",vec![kind.as_str().into()]).await?>=self.running(kind) as i64{continue}let sql=if kind.expensive(){"SELECT q.id FROM artifact_jobs q WHERE q.state='queued' AND q.kind=? AND (SELECT count(*) FROM artifact_jobs x WHERE x.state='running' AND x.workspace=q.workspace)<? AND NOT EXISTS(SELECT 1 FROM artifact_jobs x WHERE x.state='running' AND x.workspace=q.workspace AND x.repo=q.repo AND x.kind IN('full_history','files')) ORDER BY CASE WHEN q.workspace>? THEN 0 ELSE 1 END,q.workspace,q.created_at,q.id LIMIT 1"}else{"SELECT q.id FROM artifact_jobs q WHERE q.state='queued' AND q.kind=? AND (SELECT count(*) FROM artifact_jobs x WHERE x.state='running' AND x.workspace=q.workspace)<? ORDER BY CASE WHEN q.workspace>? THEN 0 ELSE 1 END,q.workspace,q.created_at,q.id LIMIT 1"};let Some(id)=opt_i64(&tx,sql,vec![kind.as_str().into(),(self.limits.workspace_running as i64).into(),wc.clone().into()]).await? else{continue};let t=now(&tx).await?;if exec(&tx,"UPDATE artifact_jobs SET state='running',owner=?,heartbeat_at=?,lease_expires_at=?,lease_generation=lease_generation+1,claim_attempts=claim_attempts+1,updated_at=? WHERE id=? AND state='queued'",vec![owner.into(),t.into(),(t+lease).into(),t.into(),id.into()]).await?==1{let record=get_id(&tx,id).await?.context("claimed artifact disappeared")?;exec(&tx,"UPDATE scheduler_state SET fairness_cursor=?,workspace_cursor=? WHERE id=1",vec![(((pos+1)%4) as i64).into(),record.key.workspace.clone().into()]).await?;return Ok(Some(ClaimedArtifact{record}))}}
  Ok(None)}.await;
        finish(tx, result).await
    }
    async fn heartbeat(&self, c: &ClaimedArtifact, o: &str, lease: i64) -> Result<bool> {
        validate_lease(o, lease)?;
        let tx = self.tx().await?;
        let r=async{let t=now(&tx).await?;Ok(exec(&tx,"UPDATE artifact_jobs SET heartbeat_at=?,lease_expires_at=?,updated_at=? WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?",vec![t.into(),(t+lease).into(),t.into(),c.record.id.into(),o.into(),(c.record.lease_generation as i64).into(),t.into()]).await?==1)}.await;
        finish(tx, r).await
    }
    async fn owns(&self, c: &ClaimedArtifact, o: &str) -> Result<bool> {
        let conn = self.conn().await?;
        let t = now(&conn).await?;
        Ok(one_i64(&conn,"SELECT count(*) FROM artifact_jobs WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?",vec![c.record.id.into(),o.into(),(c.record.lease_generation as i64).into(),t.into()]).await?==1)
    }
    async fn complete_verified(
        &self,
        c: &ClaimedArtifact,
        o: &str,
        verified: &VerifiedCompletionEvidence,
    ) -> Result<bool> {
        let e = self.completion_sealer.verify(c, verified)?;
        let tx = self.tx().await?;
        let r=async{let t=now(&tx).await?;let won=exec(&tx,"UPDATE artifact_jobs SET state='ready',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,manifest=?,error=NULL,failure_class=NULL,updated_at=? WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?",vec![e.manifest().into(),t.into(),c.record.id.into(),o.into(),(c.record.lease_generation as i64).into(),t.into()]).await?==1;if won{exec(&tx,"UPDATE artifact_observations SET published_artifact_id=? WHERE desired_artifact_id=?",vec![c.record.id.into(),c.record.id.into()]).await?;}Ok(won)}.await;
        finish(tx, r).await
    }
    async fn fail(
        &self,
        c: &ClaimedArtifact,
        o: &str,
        class: FailureClass,
        error: &str,
    ) -> Result<bool> {
        if error.trim().is_empty() {
            bail!("artifact failure reason is empty")
        }
        let tx = self.tx().await?;
        let r=async{let t=now(&tx).await?;Ok(exec(&tx,"UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error=?,failure_class=?,updated_at=? WHERE id=? AND state='running' AND owner=? AND lease_generation=? AND lease_expires_at>=?",vec![error.into(),class.as_str().into(),t.into(),c.record.id.into(),o.into(),(c.record.lease_generation as i64).into(),t.into()]).await?==1)}.await;
        finish(tx, r).await
    }
    async fn reconcile_expired(&self) -> Result<(u64, u64)> {
        let tx = self.tx().await?;
        let r=async{let t=now(&tx).await?;exec(&tx,"DELETE FROM artifact_consumers WHERE expires_at<=?",vec![t.into()]).await?;exec(&tx,"DELETE FROM artifact_jobs WHERE state='queued' AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations) AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",vec![]).await?;let f=exec(&tx,"UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error='lease expired after attempt limit',failure_class='dead_letter',updated_at=? WHERE state='running' AND lease_expires_at<=? AND claim_attempts>=?",vec![t.into(),t.into(),(self.limits.max_claim_attempts as i64).into()]).await?;let q=exec(&tx,"UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error='lease expired; reclaimed',updated_at=? WHERE state='running' AND lease_expires_at<=? AND claim_attempts<?",vec![t.into(),t.into(),(self.limits.max_claim_attempts as i64).into()]).await?;Ok((q,f))}.await;
        finish(tx, r).await
    }
    async fn get(&self, id: i64) -> Result<Option<ArtifactRecord>> {
        get_id(&self.conn().await?, id).await
    }
    async fn get_by_key(&self, key: &ArtifactKey) -> Result<Option<ArtifactRecord>> {
        validate_format_version(key.format_version)?;
        get_key(&self.conn().await?, key).await
    }
    async fn published(
        &self,
        w: &str,
        r: &str,
        b: &str,
        k: ArtifactKind,
        v: u32,
    ) -> Result<Option<ArtifactRecord>> {
        validate_format_version(v)?;
        let conn = self.conn().await?;
        let id=opt_i64(&conn,"SELECT j.id FROM artifact_observations a JOIN artifact_jobs j ON j.id=a.published_artifact_id AND j.workspace=a.workspace AND j.repo=a.repo AND j.kind=a.kind AND j.format_version=a.format_version WHERE a.workspace=? AND a.repo=? AND a.branch=? AND a.kind=? AND a.format_version=? AND j.state='ready' AND j.manifest IS NOT NULL",vec![w.into(),r.into(),b.into(),k.as_str().into(),(v as i64).into()]).await?;
        match id {
            Some(id) => {
                let record = get_id(&conn, id)
                    .await?
                    .context("published artifact disappeared")?;
                if record
                    .manifest
                    .as_deref()
                    .is_none_or(|manifest| manifest.trim().is_empty())
                {
                    bail!("published libsql artifact has a blank manifest")
                }
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }
    async fn counts(&self) -> Result<Vec<(ArtifactKind, ArtifactState, u64)>> {
        let conn = self.conn().await?;
        let mut rows=conn.query("SELECT kind,state,count(*) FROM artifact_jobs GROUP BY kind,state ORDER BY kind,state",()).await?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await? {
            out.push((
                ArtifactKind::parse(&r.get::<String>(0)?)?,
                ArtifactState::parse(&r.get::<String>(1)?)?,
                r.get::<i64>(2)? as u64,
            ))
        }
        Ok(out)
    }
}

async fn validate_schema(c: &Connection) -> Result<()> {
    let row = query_one(
        c,
        "SELECT version,provenance FROM artifact_scheduler_schema WHERE id=1",
        vec![],
    )
    .await?
    .context("artifact scheduler schema marker missing")?;
    if row.get::<i64>(0)? != VERSION || row.get::<String>(1)? != PROVENANCE {
        bail!("unsupported or foreign libsql artifact scheduler schema")
    }
    let expected = [
        (
            "artifact_scheduler_schema",
            &["id", "version", "provenance"][..],
        ),
        (
            "artifact_jobs",
            &[
                "id",
                "workspace",
                "repo",
                "commit_oid",
                "kind",
                "format_version",
                "state",
                "owner",
                "heartbeat_at",
                "lease_expires_at",
                "lease_generation",
                "claim_attempts",
                "retry_count",
                "manifest",
                "error",
                "failure_class",
                "created_at",
                "updated_at",
            ],
        ),
        (
            "branch_observations",
            &[
                "workspace",
                "repo",
                "branch",
                "generation",
                "desired_commit",
                "updated_at",
            ],
        ),
        (
            "artifact_observations",
            &[
                "workspace",
                "repo",
                "branch",
                "kind",
                "desired_commit",
                "desired_artifact_id",
                "desired_generation",
                "published_artifact_id",
                "format_version",
                "observed_at",
            ],
        ),
        (
            "artifact_consumers",
            &["artifact_id", "consumer_id", "expires_at"],
        ),
        (
            "scheduler_state",
            &[
                "id",
                "fairness_cursor",
                "workspace_cursor",
                "config_fingerprint",
            ],
        ),
    ];
    for (table, expected_names) in expected {
        let mut rows = c
            .query(&format!("PRAGMA table_info('{table}')"), ())
            .await?;
        let mut names = Vec::new();
        while let Some(r) = rows.next().await? {
            let name = r.get::<String>(1)?;
            names.push(name.clone());
            let notnull = r.get::<i64>(3)?;
            let default = r.get::<Option<String>>(4)?;
            if table == "artifact_jobs"
                && ["lease_generation", "claim_attempts", "retry_count"].contains(&name.as_str())
                && (notnull != 1 || default.as_deref() != Some("0"))
            {
                bail!("libsql scheduler counter column shape is unsafe")
            };
            if table == "scheduler_state"
                && ["workspace_cursor", "config_fingerprint"].contains(&name.as_str())
                && (notnull != 1 || default.as_deref() != Some("''"))
            {
                bail!("libsql scheduler control column shape is unsafe")
            }
        }
        if names != expected_names {
            bail!("libsql artifact scheduler table {table} has unexpected columns")
        }
    }
    // PRAGMA exposes column shape but not CHECK/UNIQUE clauses. Require the
    // safety-critical constraints in sqlite_master too, so a planted marker
    // cannot bless lookalike tables that admit invalid future rows.
    let constraints = [
        ("artifact_scheduler_schema", &["check(id=1)"][..]),
        (
            "artifact_jobs",
            &[
                "check(kindin('head','full_history','files'))",
                "check(format_versionbetween1and4294967295)",
                "check(statein('queued','running','ready','failed'))",
                "check(lease_generation>=0)",
                "check(claim_attemptsbetween0and4294967295)",
                "check(retry_countbetween0and4294967295)",
                "unique(workspace,repo,commit_oid,kind,format_version)",
            ],
        ),
        (
            "branch_observations",
            &["check(generation>=1)", "primarykey(workspace,repo,branch)"],
        ),
        (
            "artifact_observations",
            &[
                "check(kindin('head','full_history','files'))",
                "check(desired_generation>=1)",
                "check(format_versionbetween1and4294967295)",
                "primarykey(workspace,repo,branch,kind)",
            ],
        ),
        (
            "artifact_consumers",
            &["primarykey(artifact_id,consumer_id)"],
        ),
        (
            "scheduler_state",
            &["check(id=1)", "check(fairness_cursorbetween0and3)"],
        ),
    ];
    for (table, required) in constraints {
        let sql = one_string(
            c,
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            vec![table.into()],
        )
        .await?;
        let compact = canonical_ddl(&sql);
        if required.iter().any(|fragment| !compact.contains(fragment)) {
            bail!("libsql artifact scheduler table {table} is missing required constraints")
        }
        let expected_sql = SCHEMA
            .iter()
            .find(|ddl| ddl.starts_with(&format!("CREATE TABLE IF NOT EXISTS {table}")))
            .context("internal libsql scheduler schema definition missing")?;
        if canonical_ddl(&sql) != canonical_ddl(expected_sql) {
            bail!("libsql artifact scheduler table {table} differs from schema version")
        }
    }
    for index in [
        "artifact_jobs_claim",
        "artifact_jobs_lease",
        "artifact_observations_desired",
        "artifact_observations_published",
        "artifact_consumers_expiry",
    ] {
        let actual = one_string(
            c,
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            vec![index.into()],
        )
        .await
        .with_context(|| format!("required libsql scheduler index {index} missing"))?;
        let expected = SCHEMA
            .iter()
            .find(|ddl| ddl.starts_with(&format!("CREATE INDEX IF NOT EXISTS {index}")))
            .context("internal libsql scheduler index definition missing")?;
        if canonical_ddl(&actual) != canonical_ddl(expected) {
            bail!("libsql scheduler index {index} differs from schema version")
        }
    }
    let invalid_jobs=one_i64(c,"SELECT count(*) FROM artifact_jobs WHERE state IS NULL OR typeof(state)<>'text' OR state NOT IN('queued','running','ready','failed') OR kind IS NULL OR typeof(kind)<>'text' OR kind NOT IN('head','full_history','files') OR format_version IS NULL OR typeof(format_version)<>'integer' OR format_version NOT BETWEEN 1 AND 4294967295 OR typeof(id)<>'integer' OR typeof(workspace)<>'text' OR typeof(repo)<>'text' OR typeof(commit_oid)<>'text' OR typeof(lease_generation)<>'integer' OR lease_generation<0 OR typeof(claim_attempts)<>'integer' OR claim_attempts NOT BETWEEN 0 AND 4294967295 OR typeof(retry_count)<>'integer' OR retry_count NOT BETWEEN 0 AND 4294967295 OR typeof(created_at)<>'integer' OR typeof(updated_at)<>'integer' OR (owner IS NOT NULL AND typeof(owner)<>'text') OR (heartbeat_at IS NOT NULL AND typeof(heartbeat_at)<>'integer') OR (lease_expires_at IS NOT NULL AND typeof(lease_expires_at)<>'integer') OR (manifest IS NOT NULL AND typeof(manifest)<>'text') OR (error IS NOT NULL AND typeof(error)<>'text') OR (failure_class IS NOT NULL AND (typeof(failure_class)<>'text' OR failure_class NOT IN('retryable','permanent','dead_letter'))) OR (state='running' AND (owner IS NULL OR length(trim(owner))=0 OR lease_expires_at IS NULL)) OR (state='ready' AND (manifest IS NULL OR length(trim(manifest))=0))",vec![]).await?;
    if invalid_jobs != 0 {
        bail!("libsql artifact scheduler contains invalid artifact jobs")
    }
    reject_rust_blank(
        c,
        "SELECT owner FROM artifact_jobs WHERE state='running'",
        "artifact lease owner",
    )
    .await?;
    reject_rust_blank(
        c,
        "SELECT manifest FROM artifact_jobs WHERE state='ready'",
        "artifact manifest",
    )
    .await?;
    let invalid_observations=one_i64(c,"SELECT count(*) FROM artifact_observations a LEFT JOIN artifact_jobs d ON d.id=a.desired_artifact_id AND d.workspace=a.workspace AND d.repo=a.repo AND d.kind=a.kind AND d.commit_oid=a.desired_commit AND d.format_version=a.format_version AND d.format_version BETWEEN 1 AND 4294967295 LEFT JOIN artifact_jobs p ON p.id=a.published_artifact_id AND p.workspace=a.workspace AND p.repo=a.repo AND p.kind=a.kind AND p.format_version=a.format_version AND p.state='ready' AND p.manifest IS NOT NULL AND length(trim(p.manifest))>0 WHERE typeof(a.workspace)<>'text' OR typeof(a.repo)<>'text' OR typeof(a.branch)<>'text' OR typeof(a.kind)<>'text' OR a.kind NOT IN('head','full_history','files') OR typeof(a.desired_commit)<>'text' OR typeof(a.desired_artifact_id)<>'integer' OR typeof(a.desired_generation)<>'integer' OR a.desired_generation<1 OR (a.published_artifact_id IS NOT NULL AND typeof(a.published_artifact_id)<>'integer') OR typeof(a.format_version)<>'integer' OR a.format_version NOT BETWEEN 1 AND 4294967295 OR typeof(a.observed_at)<>'integer' OR d.id IS NULL OR (a.published_artifact_id IS NOT NULL AND p.id IS NULL)",vec![]).await?;
    if invalid_observations != 0 {
        bail!("libsql artifact scheduler contains invalid artifact observations")
    }
    let invalid_branches=one_i64(c,"SELECT count(*) FROM branch_observations WHERE typeof(workspace)<>'text' OR typeof(repo)<>'text' OR typeof(branch)<>'text' OR typeof(generation)<>'integer' OR generation<1 OR typeof(desired_commit)<>'text' OR typeof(updated_at)<>'integer'",vec![]).await?;
    if invalid_branches != 0 {
        bail!("libsql artifact scheduler contains invalid branch observations")
    }
    let invalid_consumers=one_i64(c,"SELECT count(*) FROM artifact_consumers WHERE typeof(artifact_id)<>'integer' OR typeof(consumer_id)<>'text' OR length(trim(consumer_id))=0 OR typeof(expires_at)<>'integer'",vec![]).await?;
    if invalid_consumers != 0 {
        bail!("libsql artifact scheduler contains invalid consumers")
    }
    reject_rust_blank(
        c,
        "SELECT consumer_id FROM artifact_consumers",
        "artifact consumer id",
    )
    .await?;
    let invalid_control=one_i64(c,"SELECT count(*) FROM scheduler_state WHERE id<>1 OR typeof(id)<>'integer' OR typeof(fairness_cursor)<>'integer' OR fairness_cursor NOT BETWEEN 0 AND 3 OR typeof(workspace_cursor)<>'text' OR typeof(config_fingerprint)<>'text'",vec![]).await?;
    if invalid_control != 0 {
        bail!("libsql artifact scheduler contains invalid control state")
    }
    Ok(())
}
async fn reject_rust_blank(c: &Connection, sql: &str, field: &str) -> Result<()> {
    let mut rows = c.query(sql, ()).await?;
    while let Some(row) = rows.next().await? {
        if row.get::<String>(0)?.trim().is_empty() {
            bail!("libsql artifact scheduler contains blank {field}")
        }
    }
    Ok(())
}
fn canonical_ddl(sql: &str) -> String {
    sql.to_ascii_lowercase()
        .replace("if not exists", "")
        .chars()
        .filter(|c| !c.is_whitespace() && *c != ';')
        .collect()
}
const SELECT: &str = "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs";
async fn get_id(c: &Connection, id: i64) -> Result<Option<ArtifactRecord>> {
    query_one(c, &format!("{SELECT} WHERE id=?"), vec![id.into()])
        .await?
        .map(row_record)
        .transpose()
}
async fn get_key(c: &Connection, k: &ArtifactKey) -> Result<Option<ArtifactRecord>> {
    query_one(
        c,
        &format!(
            "{SELECT} WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?"
        ),
        key_params(k),
    )
    .await?
    .map(row_record)
    .transpose()
}
fn key_params(k: &ArtifactKey) -> Vec<Value> {
    vec![
        k.workspace.clone().into(),
        k.repo.clone().into(),
        k.commit.clone().into(),
        k.kind.as_str().into(),
        (k.format_version as i64).into(),
    ]
}
fn row_record(r: Row) -> Result<ArtifactRecord> {
    Ok(ArtifactRecord {
        id: r.get(0)?,
        key: ArtifactKey {
            workspace: r.get(1)?,
            repo: r.get(2)?,
            commit: r.get(3)?,
            kind: ArtifactKind::parse(&r.get::<String>(4)?)?,
            format_version: u32::try_from(r.get::<i64>(5)?).context("invalid artifact format")?,
        },
        state: ArtifactState::parse(&r.get::<String>(6)?)?,
        owner: r.get(7)?,
        lease_expires_at: r.get(8)?,
        lease_generation: u64::try_from(r.get::<i64>(9)?).context("invalid lease generation")?,
        claim_attempts: u32::try_from(r.get::<i64>(10)?).context("invalid claim attempts")?,
        retry_count: u32::try_from(r.get::<i64>(11)?).context("invalid retry count")?,
        manifest: r.get(12)?,
        error: r.get(13)?,
        failure_class: r
            .get::<Option<String>>(14)?
            .map(|x| FailureClass::parse(&x))
            .transpose()?,
    })
}
async fn query_one(c: &Connection, sql: &str, p: Vec<Value>) -> Result<Option<Row>> {
    let mut r = c.query(sql, p).await?;
    Ok(r.next().await?)
}
async fn one_i64(c: &Connection, sql: &str, p: Vec<Value>) -> Result<i64> {
    query_one(c, sql, p)
        .await?
        .context("required scalar row missing")?
        .get(0)
        .map_err(Into::into)
}
async fn opt_i64(c: &Connection, sql: &str, p: Vec<Value>) -> Result<Option<i64>> {
    Ok(query_one(c, sql, p).await?.map(|r| r.get(0)).transpose()?)
}
async fn one_string(c: &Connection, sql: &str, p: Vec<Value>) -> Result<String> {
    query_one(c, sql, p)
        .await?
        .context("required scalar row missing")?
        .get(0)
        .map_err(Into::into)
}
async fn exec(c: &Connection, sql: &str, p: Vec<Value>) -> Result<u64> {
    Ok(c.execute(sql, p).await?)
}
async fn now(c: &Connection) -> Result<i64> {
    one_i64(c, "SELECT unixepoch()", vec![]).await
}
async fn finish<T>(tx: Transaction, r: Result<T>) -> Result<T> {
    match r {
        Ok(v) => {
            tx.commit().await?;
            Ok(v)
        }
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e)
        }
    }
}
fn outcome_id(o: &ScheduleOutcome) -> i64 {
    match o {
        ScheduleOutcome::Enqueued(x)
        | ScheduleOutcome::Subscribed(x)
        | ScheduleOutcome::AlreadyReady(x)
        | ScheduleOutcome::Failed(x, _) => *x,
    }
}
fn kindex(k: ArtifactKind) -> u8 {
    match k {
        ArtifactKind::Head => 0,
        ArtifactKind::FullHistory => 1,
        ArtifactKind::Files => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::process::{Child, Command, Stdio};
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
        let child = Command::new("sqld")
            .arg("--db-path")
            .arg(dir.path().join("db"))
            .arg("--http-listen-addr")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--http-self-url")
            .arg(format!("http://127.0.0.1:{port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        let mut child = match child {
            Ok(child) => child,
            Err(error) if std::env::var_os("RIPCLONE_REQUIRE_SQLD_TESTS").is_some() => {
                panic!("required sqld conformance server is unavailable: {error}")
            }
            Err(_) => return None,
        };
        let url = format!("http://127.0.0.1:{port}");
        for _ in 0..100 {
            if let Ok(db) = libsql::Builder::new_remote(url.clone(), String::new())
                .build()
                .await
            {
                if let Ok(c) = db.connect() {
                    if c.query("SELECT 1", ()).await.is_ok() {
                        return Some(Server {
                            child,
                            _dir: dir,
                            url,
                        });
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let _ = child.kill();
        panic!("installed sqld did not become ready")
    }
    struct Accept;
    impl CompletionVerifier for Accept {
        fn identity(&self) -> &'static str {
            "libsql-test-accept-v1"
        }
        fn verify(&self, claim: &ClaimedArtifact, e: &CompletionEvidence) -> Result<()> {
            crate::artifact_scheduler::validate_evidence(claim, e)
        }
    }
    struct Reject;
    impl CompletionVerifier for Reject {
        fn identity(&self) -> &'static str {
            "libsql-test-reject-v1"
        }
        fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
            bail!("rejected")
        }
    }
    fn key(commit: &str, kind: ArtifactKind) -> ArtifactKey {
        ArtifactKey {
            workspace: "w".into(),
            repo: "r".into(),
            commit: commit.into(),
            kind,
            format_version: 1,
        }
    }
    async fn db(url: &str) -> Database {
        libsql::Builder::new_remote(url.into(), String::new())
            .build()
            .await
            .unwrap()
    }
    async fn scheduler(url: &str, limits: SchedulerLimits) -> LibsqlArtifactScheduler {
        LibsqlArtifactScheduler::from_database(db(url).await, limits, Arc::new(Accept))
            .await
            .unwrap()
    }
    async fn startup_error(url: &str) -> String {
        match LibsqlArtifactScheduler::from_database(
            db(url).await,
            Default::default(),
            Arc::new(Accept),
        )
        .await
        {
            Ok(_) => panic!("expected scheduler startup rejection"),
            Err(error) => error.to_string(),
        }
    }

    #[tokio::test]
    async fn concurrent_instances_generation_aba_and_publication() {
        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        let b = scheduler(&s.url, Default::default()).await;
        let k = key("c1", ArtifactKind::Head);
        let (x, y) = tokio::join!(a.schedule(&k), b.schedule(&k));
        let ids = [outcome_id(&x.unwrap()), outcome_id(&y.unwrap())];
        assert_eq!(ids[0], ids[1]);
        assert!(
            a.get_by_key(&key("bad", ArtifactKind::Head))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            a.schedule(&ArtifactKey {
                format_version: 0,
                ..k.clone()
            })
            .await
            .is_err()
        );

        let o = a
            .observe(
                "w",
                "r",
                "main",
                "c1",
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            o,
            ObservationOutcome::Accepted { generation: 1, .. }
        ));
        assert_eq!(
            b.observe(
                "w",
                "r",
                "main",
                "c1",
                &[ArtifactKind::Files, ArtifactKind::Head],
                1,
                None,
            )
            .await
            .unwrap(),
            ObservationOutcome::Unchanged { generation: 1 }
        );
        let snapshot = b.observation_snapshot("w", "r", "main").await.unwrap();
        assert_eq!(snapshot.workspace(), "w");
        assert_eq!(snapshot.repo(), "r");
        assert_eq!(snapshot.branch(), "main");
        assert_eq!(snapshot.generation(), Some(1));
        assert_eq!(snapshot.commit(), Some("c1"));
        assert!(matches!(
            b.observe("w", "r", "main", "c2", &[ArtifactKind::Head], 1, None)
                .await
                .unwrap(),
            ObservationOutcome::Stale {
                current_generation: 1
            }
        ));
        let c1 = a.claim("a", 30).await.unwrap().unwrap();
        let raw = a.conn().await.unwrap();
        raw.execute(
            "UPDATE artifact_jobs SET lease_expires_at=0 WHERE id=?",
            [c1.record.id],
        )
        .await
        .unwrap();
        assert_eq!(b.reconcile_expired().await.unwrap(), (1, 0));
        let c2 = b.claim("b", 30).await.unwrap().unwrap();
        assert_eq!(c1.record.id, c2.record.id);
        assert!(c2.record.lease_generation > c1.record.lease_generation);
        let evidence = CompletionEvidence::new(k.clone(), "manifest").unwrap();
        assert!(!a.complete(&c1, "a", &evidence).await.unwrap());
        assert!(b.complete(&c2, "b", &evidence).await.unwrap());
        assert_eq!(
            a.published("w", "r", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .unwrap()
                .id,
            c2.record.id
        );
    }

    #[tokio::test]
    async fn consumers_reserve_retries_deadletter_and_verifier_are_fenced() {
        let Some(s) = server().await else { return };
        let limits = SchedulerLimits {
            total_backlog: 2,
            workspace_backlog: 2,
            head_backlog: 2,
            full_history_backlog: 1,
            files_backlog: 1,
            head_reserved: 1,
            max_claim_attempts: 2,
            ..Default::default()
        };
        let a = scheduler(&s.url, limits.clone()).await;
        let held = key("held", ArtifactKind::Head);
        let held_id = outcome_id(&a.subscribe_consumer(&held, "clone", 30).await.unwrap());
        a.observe("w", "r", "main", "new", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        assert!(a.get(held_id).await.unwrap().is_some());
        let raw = a.conn().await.unwrap();
        raw.execute("UPDATE artifact_consumers SET expires_at=0", ())
            .await
            .unwrap();
        a.reconcile_expired().await.unwrap();
        assert!(a.get(held_id).await.unwrap().is_none());

        let claim = a.claim("one", 30).await.unwrap().unwrap();
        assert!(
            a.fail(&claim, "one", FailureClass::Retryable, "boom")
                .await
                .unwrap()
        );
        assert_eq!(
            a.retry_failed(&claim.record.key).await.unwrap(),
            RetryOutcome::Requeued(claim.record.id)
        );
        let claim = a.claim("two", 30).await.unwrap().unwrap();
        raw.execute(
            "UPDATE artifact_jobs SET lease_expires_at=0 WHERE id=?",
            [claim.record.id],
        )
        .await
        .unwrap();
        assert_eq!(a.reconcile_expired().await.unwrap(), (0, 1));
        assert!(matches!(
            a.retry_failed(&claim.record.key).await.unwrap(),
            RetryOutcome::NotRetryable(FailureClass::DeadLetter)
        ));

        // A different verifier identity may not join the fleet, and even a
        // matching identity that rejects evidence cannot publish.
        assert!(
            LibsqlArtifactScheduler::from_database(
                db(&s.url).await,
                limits.clone(),
                Arc::new(Reject)
            )
            .await
            .is_err()
        );
        struct RejectSameId;
        impl CompletionVerifier for RejectSameId {
            fn identity(&self) -> &'static str {
                "libsql-test-accept-v1"
            }
            fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
                bail!("bad CAS")
            }
        }
        let rejecting = LibsqlArtifactScheduler::from_database(
            db(&s.url).await,
            limits,
            Arc::new(RejectSameId),
        )
        .await
        .unwrap();
        let k = key("verify", ArtifactKind::Head);
        rejecting.schedule(&k).await.unwrap();
        let c = rejecting.claim("v", 30).await.unwrap().unwrap();
        assert!(
            rejecting
                .complete(&c, "v", &CompletionEvidence::new(k, "manifest").unwrap())
                .await
                .is_err()
        );
        assert_eq!(
            rejecting.get(c.record.id).await.unwrap().unwrap().state,
            ArtifactState::Running
        );
    }

    #[tokio::test]
    async fn reserve_and_fleet_running_caps_are_aggregated() {
        let Some(s) = server().await else { return };
        let limits = SchedulerLimits {
            total_backlog: 3,
            workspace_backlog: 3,
            head_reserved: 1,
            total_running: 3,
            head_running: 1,
            full_history_running: 1,
            files_running: 1,
            workspace_running: 3,
            ..Default::default()
        };
        let a = scheduler(&s.url, limits).await;
        let b = LibsqlArtifactScheduler::from_database(
            db(&s.url).await,
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .err()
        .unwrap();
        assert!(b.to_string().contains("configuration differs"));
        a.schedule(&key("f1", ArtifactKind::FullHistory))
            .await
            .unwrap();
        let mut files = key("f2", ArtifactKind::Files);
        files.repo = "r2".into();
        a.schedule(&files).await.unwrap();
        assert!(
            a.schedule(&key("f3", ArtifactKind::FullHistory))
                .await
                .is_err()
        );
        a.schedule(&key("head", ArtifactKind::Head)).await.unwrap();
        let (x, y, z, q) = tokio::join!(
            a.claim("a", 30),
            a.claim("b", 30),
            a.claim("c", 30),
            a.claim("d", 30)
        );
        assert_eq!(
            [x, y, z, q]
                .into_iter()
                .filter(|r| matches!(r, Ok(Some(_))))
                .count(),
            3
        );
        assert_eq!(
            a.counts()
                .await
                .unwrap()
                .into_iter()
                .filter(|(_, state, _)| *state == ArtifactState::Running)
                .map(|(_, _, n)| n)
                .sum::<u64>(),
            3
        );
    }

    #[tokio::test]
    async fn planted_marker_missing_defaults_and_null_state_fail_closed() {
        let Some(s) = server().await else { return };
        let d = db(&s.url).await;
        let c = d.connect().unwrap();
        c.execute("CREATE TABLE artifact_scheduler_schema(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,provenance TEXT NOT NULL)",()).await.unwrap();
        c.execute(
            "INSERT INTO artifact_scheduler_schema VALUES(1,1,?)",
            [PROVENANCE],
        )
        .await
        .unwrap();
        c.execute("CREATE TABLE artifact_jobs(id INTEGER PRIMARY KEY,workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,kind TEXT NOT NULL,format_version INTEGER NOT NULL,state TEXT NOT NULL,owner TEXT,heartbeat_at INTEGER,lease_expires_at INTEGER,lease_generation INTEGER,claim_attempts INTEGER,retry_count INTEGER,manifest TEXT,error TEXT,failure_class TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,UNIQUE(workspace,repo,commit_oid,kind,format_version))",()).await.unwrap();
        assert!(
            LibsqlArtifactScheduler::from_database(d, Default::default(), Arc::new(Accept))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn exact_schema_corrupt_rows_and_indexes_fail_closed() {
        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        a.schedule(&key("running-null", ArtifactKind::Head))
            .await
            .unwrap();
        a.conn().await.unwrap().execute("UPDATE artifact_jobs SET state='running',owner=NULL,lease_expires_at=NULL WHERE commit_oid='running-null'",()).await.unwrap();
        assert!(
            startup_error(&s.url)
                .await
                .contains("invalid artifact jobs")
        );

        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        a.observe("w", "r", "main", "observed", &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        a.conn()
            .await
            .unwrap()
            .execute(
                "UPDATE artifact_observations SET desired_commit='mismatch'",
                (),
            )
            .await
            .unwrap();
        assert!(
            startup_error(&s.url)
                .await
                .contains("invalid artifact observations")
        );

        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        let c = a.conn().await.unwrap();
        c.execute("DROP INDEX artifact_jobs_claim", ())
            .await
            .unwrap();
        c.execute(
            "CREATE INDEX artifact_jobs_claim ON artifact_jobs(kind,state,created_at,id)",
            (),
        )
        .await
        .unwrap();
        assert!(
            startup_error(&s.url)
                .await
                .contains("index artifact_jobs_claim differs")
        );

        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        a.schedule(&key("wrong-type", ArtifactKind::Head))
            .await
            .unwrap();
        a.conn()
            .await
            .unwrap()
            .execute("UPDATE artifact_jobs SET created_at='not-an-integer'", ())
            .await
            .unwrap();
        assert!(
            startup_error(&s.url)
                .await
                .contains("invalid artifact jobs")
        );

        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        let k = key("blank-manifest", ArtifactKind::Head);
        a.observe("w", "r", "main", &k.commit, &[ArtifactKind::Head], 1, None)
            .await
            .unwrap();
        let claim = a.claim("owner", 30).await.unwrap().unwrap();
        a.complete(
            &claim,
            "owner",
            &CompletionEvidence::new(k, "valid").unwrap(),
        )
        .await
        .unwrap();
        a.conn()
            .await
            .unwrap()
            .execute(
                "UPDATE artifact_jobs SET manifest=char(9) WHERE state='ready'",
                (),
            )
            .await
            .unwrap();
        assert!(
            a.published("w", "r", "main", ArtifactKind::Head, 1)
                .await
                .is_err()
        );
        assert!(
            startup_error(&s.url)
                .await
                .contains("blank artifact manifest")
        );

        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        a.schedule(&key("blank-owner", ArtifactKind::Head))
            .await
            .unwrap();
        a.conn().await.unwrap().execute("UPDATE artifact_jobs SET state='running',owner=char(10),lease_expires_at=9999999999 WHERE commit_oid='blank-owner'",()).await.unwrap();
        assert!(
            startup_error(&s.url)
                .await
                .contains("blank artifact lease owner")
        );

        let Some(s) = server().await else { return };
        let a = scheduler(&s.url, Default::default()).await;
        let consumer = key("blank-consumer", ArtifactKind::Head);
        let artifact_id = outcome_id(&a.subscribe_consumer(&consumer, "valid", 30).await.unwrap());
        a.conn()
            .await
            .unwrap()
            .execute(
                "UPDATE artifact_consumers SET consumer_id=char(160) WHERE artifact_id=?",
                [artifact_id],
            )
            .await
            .unwrap();
        assert!(
            startup_error(&s.url)
                .await
                .contains("blank artifact consumer id")
        );
    }
}
