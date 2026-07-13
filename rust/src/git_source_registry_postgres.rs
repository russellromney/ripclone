use super::*;
use sqlx::PgConnection;
use sqlx::postgres::PgPool;

pub(crate) const POSTGRES_V7_SCHEMA: &str = r#"
ALTER TABLE scheduler_state ADD COLUMN IF NOT EXISTS limits_fingerprint TEXT NOT NULL DEFAULT '';
CREATE TABLE git_source_roots(root_hash TEXT PRIMARY KEY,root_len BIGINT NOT NULL CHECK(root_len>0),workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,source_format_version BIGINT NOT NULL CHECK(source_format_version BETWEEN 1 AND 4294967295),object_format TEXT NOT NULL CHECK(object_format IN('sha1','sha256')),semantic_digest TEXT NOT NULL CHECK(length(semantic_digest)=64),object_set_digest TEXT NOT NULL CHECK(length(object_set_digest)=64),object_count BIGINT NOT NULL CHECK(object_count>0),total_bytes BIGINT NOT NULL CHECK(total_bytes>0),registration_operation TEXT NOT NULL UNIQUE,registration_generation BIGINT NOT NULL UNIQUE CHECK(registration_generation>0),state TEXT NOT NULL CHECK(state IN('registered','quarantined')),created_at BIGINT NOT NULL,registered_at BIGINT NOT NULL,UNIQUE(workspace,repo,commit_oid,source_format_version),UNIQUE(root_hash,workspace,repo,commit_oid,source_format_version));
CREATE TABLE git_source_members(root_hash TEXT NOT NULL,ordinal BIGINT NOT NULL CHECK(ordinal>=0),child_hash TEXT NOT NULL,child_len BIGINT NOT NULL CHECK(child_len>0),kind TEXT NOT NULL CHECK(kind IN('pack','index')),PRIMARY KEY(root_hash,ordinal),UNIQUE(root_hash,child_hash),FOREIGN KEY(root_hash) REFERENCES git_source_roots(root_hash) ON DELETE RESTRICT);
CREATE INDEX git_source_members_child ON git_source_members(child_hash,root_hash);
CREATE TABLE git_source_acquisition_sequence(id SMALLINT PRIMARY KEY CHECK(id=1),generation BIGINT NOT NULL CHECK(generation>=0));
INSERT INTO git_source_acquisition_sequence(id,generation) VALUES(1,0);
CREATE TABLE git_source_acquisitions(token TEXT PRIMARY KEY,generation BIGINT NOT NULL UNIQUE CHECK(generation>0),operation_id TEXT NOT NULL UNIQUE,active_identity TEXT UNIQUE,workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,source_format_version BIGINT NOT NULL,owner TEXT NOT NULL,attempt_id TEXT NOT NULL,root_hash TEXT,root_len BIGINT,object_format TEXT,semantic_digest TEXT,object_set_digest TEXT,object_count BIGINT,total_bytes BIGINT,expires_at BIGINT NOT NULL,state TEXT NOT NULL CHECK(state IN('held','graph_published','activation_unknown','registered','failed')),failure_class TEXT CHECK(failure_class IN('retryable','permanent','dead_letter')),CHECK((state='held' AND active_identity IS NOT NULL AND root_hash IS NULL AND failure_class IS NULL) OR (state IN('graph_published','activation_unknown') AND active_identity IS NOT NULL AND root_hash IS NOT NULL AND root_len>0 AND object_count>0 AND total_bytes>0 AND failure_class IS NULL) OR (state='registered' AND active_identity IS NULL AND root_hash IS NOT NULL AND failure_class IS NULL) OR (state='failed' AND active_identity IS NULL AND failure_class IS NOT NULL)));
CREATE INDEX git_source_acquisitions_recovery ON git_source_acquisitions(state,generation,token);
CREATE TABLE git_source_acquisition_members(token TEXT NOT NULL,ordinal BIGINT NOT NULL CHECK(ordinal>=0),child_hash TEXT NOT NULL,child_len BIGINT NOT NULL CHECK(child_len>0),kind TEXT NOT NULL CHECK(kind IN('pack','index')),PRIMARY KEY(token,ordinal),UNIQUE(token,child_hash),FOREIGN KEY(token) REFERENCES git_source_acquisitions(token) ON DELETE CASCADE);
CREATE INDEX git_source_acquisition_members_child ON git_source_acquisition_members(child_hash,token);
CREATE TABLE git_source_desires(workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,source_format_version BIGINT NOT NULL,state TEXT NOT NULL CHECK(state IN('acquiring','registered','failed')),root_hash TEXT,failure_class TEXT CHECK(failure_class IN('retryable','permanent','dead_letter')),retry_count BIGINT NOT NULL DEFAULT 0 CHECK(retry_count BETWEEN 0 AND 4294967295),acquisition_token TEXT,updated_at BIGINT NOT NULL,PRIMARY KEY(workspace,repo,commit_oid,source_format_version),CHECK((state='acquiring' AND acquisition_token IS NOT NULL AND root_hash IS NULL AND failure_class IS NULL) OR (state='registered' AND acquisition_token IS NULL AND root_hash IS NOT NULL AND failure_class IS NULL) OR (state='failed' AND acquisition_token IS NULL AND root_hash IS NULL AND failure_class IS NOT NULL)),FOREIGN KEY(root_hash) REFERENCES git_source_roots(root_hash) ON DELETE RESTRICT,FOREIGN KEY(acquisition_token) REFERENCES git_source_acquisitions(token) ON DELETE RESTRICT);
CREATE TABLE branch_source_generations(workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation BIGINT NOT NULL CHECK(generation>0),commit_oid TEXT NOT NULL,source_format_version BIGINT NOT NULL,root_hash TEXT NOT NULL,created_at BIGINT NOT NULL,PRIMARY KEY(workspace,repo,branch,generation),FOREIGN KEY(root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT);
CREATE INDEX branch_source_generations_root ON branch_source_generations(root_hash,workspace,repo);
CREATE TABLE branch_source_current(workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,generation BIGINT NOT NULL,PRIMARY KEY(workspace,repo,branch),FOREIGN KEY(workspace,repo,branch,generation) REFERENCES branch_source_generations(workspace,repo,branch,generation) ON DELETE RESTRICT);
CREATE TABLE git_source_consumers(root_hash TEXT NOT NULL,consumer_id TEXT NOT NULL,session_id TEXT NOT NULL UNIQUE,workspace TEXT NOT NULL,repo TEXT NOT NULL,commit_oid TEXT NOT NULL,source_format_version BIGINT NOT NULL,purpose TEXT NOT NULL CHECK(purpose IN('intent','builder')),expires_at BIGINT NOT NULL,PRIMARY KEY(root_hash,consumer_id),FOREIGN KEY(root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT);
CREATE INDEX git_source_consumers_expiry ON git_source_consumers(expires_at,root_hash,consumer_id);
CREATE TABLE artifact_intents(id BIGSERIAL PRIMARY KEY,workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,branch_generation BIGINT NOT NULL,source_root_hash TEXT NOT NULL,source_format_version BIGINT NOT NULL,commit_oid TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN('head','full_history','files')),format_version BIGINT NOT NULL CHECK(format_version BETWEEN 1 AND 4294967295),state TEXT NOT NULL CHECK(state IN('deferred','promoted')),artifact_id BIGINT,consumer_id TEXT NOT NULL,created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,UNIQUE(workspace,repo,branch,branch_generation,kind,format_version),CHECK((state='deferred' AND artifact_id IS NULL) OR (state='promoted' AND artifact_id IS NOT NULL)),FOREIGN KEY(workspace,repo,branch,branch_generation) REFERENCES branch_source_generations(workspace,repo,branch,generation) ON DELETE RESTRICT,FOREIGN KEY(source_root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT,FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE RESTRICT);
CREATE INDEX artifact_intents_promotion ON artifact_intents(state,updated_at,id);
CREATE INDEX artifact_intents_source ON artifact_intents(source_root_hash,state,id);
CREATE TABLE git_source_maintenance(id SMALLINT PRIMARY KEY CHECK(id=1),intent_cursor BIGINT NOT NULL DEFAULT 0 CHECK(intent_cursor>=0),intent_workspace_cursor TEXT NOT NULL DEFAULT '',acquisition_cursor BIGINT NOT NULL DEFAULT 0 CHECK(acquisition_cursor>=0),root_cursor TEXT NOT NULL DEFAULT '',config_fingerprint TEXT NOT NULL DEFAULT '',updated_at BIGINT NOT NULL DEFAULT 0);
INSERT INTO git_source_maintenance(id) VALUES(1);
"#;

const PG_SOURCE_TABLES: &[&str] = &[
    "artifact_intents",
    "branch_source_current",
    "branch_source_generations",
    "git_source_acquisition_members",
    "git_source_acquisition_sequence",
    "git_source_acquisitions",
    "git_source_consumers",
    "git_source_desires",
    "git_source_maintenance",
    "git_source_members",
    "git_source_roots",
];

pub(crate) async fn validate_postgres_v7(c: &mut PgConnection, complete: bool) -> Result<()> {
    let names:Vec<String>=sqlx::query_scalar("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r' AND (c.relname LIKE 'git\\_source\\_%' ESCAPE '\\' OR c.relname IN('branch_source_generations','branch_source_current','artifact_intents')) ORDER BY c.relname").fetch_all(&mut *c).await?;
    if complete && names != PG_SOURCE_TABLES {
        bail!("postgres v7 source registry table inventory differs")
    }
    if !complete && !names.is_empty() {
        bail!("postgres v7 source registry contains an unpublished partial schema")
    }
    if !complete {
        return Ok(());
    }
    let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name = ANY($1)").bind(PG_SOURCE_TABLES).fetch_one(&mut *c).await?;
    if columns != 101 {
        bail!("postgres v7 source registry column inventory differs")
    }
    let shape:(i64,i64,i64,i64)=sqlx::query_as("SELECT count(*) FILTER(WHERE data_type='smallint'),count(*) FILTER(WHERE data_type='bigint'),count(*) FILTER(WHERE data_type='text'),count(*) FILTER(WHERE is_nullable='YES') FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=ANY($1)").bind(PG_SOURCE_TABLES).fetch_one(&mut *c).await?;
    if shape != (2, 37, 62, 13) {
        bail!("postgres v7 source registry column definitions differ")
    }
    let invalid_defaults:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=ANY($1) AND column_default IS NOT NULL AND NOT ((table_name='artifact_intents' AND column_name='id' AND column_default=$2) OR (table_name='git_source_desires' AND column_name='retry_count' AND column_default='0') OR (table_name='git_source_maintenance' AND ((column_name IN('intent_cursor','acquisition_cursor','updated_at') AND column_default='0') OR (column_name IN('intent_workspace_cursor','root_cursor','config_fingerprint') AND column_default=$3))))").bind(PG_SOURCE_TABLES).bind("nextval('artifact_intents_id_seq'::regclass)").bind("''::text").fetch_one(&mut *c).await?;
    if invalid_defaults != 0 {
        bail!("postgres v7 source registry defaults differ")
    }
    let singleton:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_maintenance WHERE id=1 AND intent_cursor>=0 AND acquisition_cursor>=0").fetch_one(&mut *c).await?;
    let sequence: Option<(i16, i64)> =
        sqlx::query_as("SELECT id,generation FROM git_source_acquisition_sequence")
            .fetch_optional(&mut *c)
            .await?;
    let max_generation: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(generation),0) FROM git_source_acquisitions")
            .fetch_one(&mut *c)
            .await?;
    if singleton != 1 || sequence.is_none_or(|v| v.0 != 1 || v.1 < max_generation) {
        bail!("postgres v7 source registry singleton state is invalid")
    }
    let invalid:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM git_source_roots r WHERE r.root_hash !~ '^[0-9a-f]{64}$' OR r.semantic_digest !~ '^[0-9a-f]{64}$' OR r.object_set_digest !~ '^[0-9a-f]{64}$' OR NOT EXISTS(SELECT 1 FROM git_source_members m WHERE m.root_hash=r.root_hash GROUP BY m.root_hash HAVING min(m.ordinal)=0 AND max(m.ordinal)+1=count(*) AND count(*)%2=0 AND sum(m.child_len)=r.total_bytes)) + (SELECT count(*) FROM git_source_acquisitions a WHERE a.token !~ '^[0-9a-f]{64}$' OR (a.state IN('held','graph_published','activation_unknown') AND a.active_identity IS NULL) OR (a.state IN('registered','failed') AND a.active_identity IS NOT NULL)) + (SELECT count(*) FROM artifact_intents i LEFT JOIN git_source_consumers c ON c.consumer_id=i.consumer_id AND c.root_hash=i.source_root_hash AND c.purpose='intent' LEFT JOIN artifact_jobs j ON j.id=i.artifact_id WHERE c.root_hash IS NULL OR i.consumer_id !~ '^intent:[0-9a-f]{48}$' OR c.expires_at<>9223372036854775807 OR (i.state='deferred' AND (i.artifact_id IS NOT NULL OR EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id))) OR (i.state='promoted' AND (j.id IS NULL OR NOT EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.artifact_id=i.artifact_id AND ac.consumer_id=i.consumer_id AND ac.expires_at=9223372036854775807))))").fetch_one(&mut *c).await?;
    if invalid != 0 {
        bail!("postgres v7 source registry persisted state is invalid")
    }
    Ok(())
}
#[derive(Clone)]
pub struct PostgresGitSourceRegistry {
    pool: PgPool,
    storage: StorageRef,
    scheduler_limits: SchedulerLimits,
    source_limits: GitSourceLimits,
    seal: Arc<[u8; 32]>,
}

impl PostgresGitSourceRegistry {
    pub async fn new(
        pool: PgPool,
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
        let mut c = registry.pool.acquire().await?.detach();
        validate_postgres_v7(&mut c, true).await?;
        let fingerprint = registry.source_fingerprint();
        let mut tx = registry.pool.begin().await?;
        let scheduler_fingerprint: String = sqlx::query_scalar(
            "SELECT limits_fingerprint FROM scheduler_state WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if scheduler_fingerprint != scheduler_limits_fingerprint(&registry.scheduler_limits) {
            bail!("PostgreSQL source registry scheduler limits differ from durable fleet limits")
        }
        let stored: String = sqlx::query_scalar(
            "SELECT config_fingerprint FROM git_source_maintenance WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if stored.is_empty() {
            let state:i64=sqlx::query_scalar("SELECT (SELECT generation FROM git_source_acquisition_sequence WHERE id=1)+(SELECT count(*) FROM git_source_roots)+(SELECT count(*) FROM git_source_acquisitions)+(SELECT count(*) FROM git_source_desires)+(SELECT count(*) FROM branch_source_generations)+(SELECT count(*) FROM artifact_intents)").fetch_one(&mut *tx).await?;
            if state != 0 {
                bail!("empty PostgreSQL source registry fingerprint has authoritative state")
            }
            sqlx::query("UPDATE git_source_maintenance SET config_fingerprint=$1 WHERE id=1 AND config_fingerprint=''").bind(&fingerprint).execute(&mut *tx).await?;
        } else if stored != fingerprint {
            bail!(
                "PostgreSQL source registry limits or authority seal differ from fleet configuration"
            )
        }
        tx.commit().await?;
        Ok(registry)
    }

    fn source_fingerprint(&self) -> String {
        let mut h = Sha256::new();
        let l = &self.source_limits;
        for v in [
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
            h.update(v.to_be_bytes())
        }
        h.update(self.seal.as_ref());
        h.update(SOURCE_FORMAT_VERSION.to_be_bytes());
        let s = &self.scheduler_limits;
        for v in [
            s.total_backlog,
            s.workspace_backlog,
            s.head_reserved,
            s.head_backlog,
            s.full_history_backlog,
            s.files_backlog,
            s.total_running,
            s.head_running,
            s.full_history_running,
            s.files_running,
            s.workspace_running,
        ] {
            h.update((v as u64).to_be_bytes())
        }
        h.update(s.max_claim_attempts.to_be_bytes());
        h.update(s.max_manual_retries.to_be_bytes());
        hex::encode(h.finalize())
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
        let mut tx = self.pool.begin().await?;
        let now = postgres_time(&mut tx).await?;
        if let Some(token)=sqlx::query_scalar::<_,String>("SELECT token FROM git_source_acquisitions WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND source_format_version=$4 AND state IN('held','graph_published') AND expires_at<=$5 FOR UPDATE").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(now).fetch_optional(&mut *tx).await?{
            sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=$1 WHERE acquisition_token=$2 AND state='acquiring'").bind(now).bind(&token).execute(&mut *tx).await?;
            sqlx::query("UPDATE git_source_acquisitions SET state='failed',active_identity=NULL,failure_class='retryable',expires_at=0 WHERE token=$1 AND state IN('held','graph_published')").bind(&token).execute(&mut *tx).await?;
        }
        if let Some(row)=sqlx::query("SELECT state,root_hash,failure_class,retry_count,acquisition_token FROM git_source_desires WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND source_format_version=$4 FOR UPDATE").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).fetch_optional(&mut *tx).await?{
            let state:String=row.try_get("state")?;
            if state=="registered"{let root:String=row.try_get("root_hash")?;let (token,generation):(String,i64)=sqlx::query_as("SELECT token,generation FROM git_source_acquisitions WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND source_format_version=$4 AND root_hash=$5 AND state='registered' ORDER BY generation DESC LIMIT 1").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(&root).fetch_one(&mut *tx).await?;tx.commit().await?;return Ok(SourceBeginOutcome::Ready(DurableSourceSnapshot::registered(workspace.into(),repo.into(),commit.into(),root,token,checked_u64(generation,"source generation")?)?))}
            if state=="acquiring"{let token:String=row.try_get("acquisition_token")?;let (generation,state):(i64,String)=sqlx::query_as("SELECT generation,state FROM git_source_acquisitions WHERE token=$1").bind(&token).fetch_one(&mut *tx).await?;tx.commit().await?;return Ok(if state=="activation_unknown"{SourceBeginOutcome::ActivationUnknown{token,generation:checked_u64(generation,"source generation")?}}else{SourceBeginOutcome::Deferred{token,generation:checked_u64(generation,"source generation")?}})}
            let class=FailureClass::parse(row.try_get::<String,_>("failure_class")?.as_str())?;let retries=checked_u32(row.try_get("retry_count")?,"source retry count")?;
            if intent==SyncIntent::ObserveMovement||class!=FailureClass::Retryable||retries>=self.scheduler_limits.max_manual_retries{tx.commit().await?;return Ok(SourceBeginOutcome::Failed{class,retries})}
        }
        let prior: i64 = sqlx::query_scalar(
            "SELECT generation FROM git_source_acquisition_sequence WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let generation = prior.checked_add(1).context("source generation overflow")?;
        sqlx::query(
            "UPDATE git_source_acquisition_sequence SET generation=$1 WHERE id=1 AND generation=$2",
        )
        .bind(generation)
        .bind(prior)
        .execute(&mut *tx)
        .await?;
        let token = hex::encode(rand::random::<[u8; 32]>());
        let operation_id = operation_id(workspace, repo, commit, attempt_id, generation);
        let active_identity = source_identity(workspace, repo, commit, source_format_version);
        sqlx::query("INSERT INTO git_source_acquisitions(token,generation,operation_id,active_identity,workspace,repo,commit_oid,source_format_version,owner,attempt_id,expires_at,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'held')").bind(&token).bind(generation).bind(&operation_id).bind(&active_identity).bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(owner).bind(attempt_id).bind(now+ttl_secs).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO git_source_desires(workspace,repo,commit_oid,source_format_version,state,retry_count,acquisition_token,updated_at) VALUES($1,$2,$3,$4,'acquiring',0,$5,$6) ON CONFLICT(workspace,repo,commit_oid,source_format_version) DO UPDATE SET state='acquiring',root_hash=NULL,failure_class=NULL,retry_count=git_source_desires.retry_count+1,acquisition_token=excluded.acquisition_token,updated_at=excluded.updated_at").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(&token).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(SourceBeginOutcome::PermitToPrepare(
            GitSourcePreparePermit {
                token,
                generation: checked_u64(generation, "source generation")?,
                operation_id,
                workspace: workspace.into(),
                repo: repo.into(),
                commit: commit.into(),
                source_format_version,
                owner: owner.into(),
                attempt_id: attempt_id.into(),
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
        let mut tx = self.pool.begin().await?;
        let now = postgres_time(&mut tx).await?;
        let sweep: i64 =
            sqlx::query_scalar("SELECT count(*) FROM artifact_gc_sweep WHERE expires_at>$1")
                .bind(now)
                .fetch_one(&mut *tx)
                .await?;
        if sweep != 0 {
            bail!("source graph publication is fenced by live GC sweep")
        }
        let changed=sqlx::query("UPDATE git_source_acquisitions SET root_hash=$1,root_len=$2,object_format=$3,semantic_digest=$4,object_set_digest=$5,object_count=$6,total_bytes=$7,state='graph_published' WHERE token=$8 AND generation=$9 AND operation_id=$10 AND owner=$11 AND attempt_id=$12 AND state='held' AND expires_at>$13").bind(&view.root.hash).bind(checked_i64(view.root.len,"root length")?).bind(view.object_format).bind(&view.semantic_digest).bind(&view.object_set_digest).bind(checked_i64(view.object_count,"object count")?).bind(checked_i64(view.total_bytes,"source bytes")?).bind(&prepare.token).bind(prepare.generation as i64).bind(&prepare.operation_id).bind(&prepare.owner).bind(&prepare.attempt_id).bind(now).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            bail!("held source preparation capability was lost")
        }
        for member in &view.members {
            sqlx::query("INSERT INTO git_source_acquisition_members(token,ordinal,child_hash,child_len,kind) VALUES($1,$2,$3,$4,$5)").bind(&prepare.token).bind(member.ordinal as i64).bind(&member.blob.hash).bind(checked_i64(member.blob.len,"member length")?).bind(member.kind).execute(&mut *tx).await?;
        }
        tx.commit().await?;
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
        let permit = GitSourcePublicationPermit {
            token: prepare.token.clone(),
            generation: prepare.generation,
            workspace: prepare.workspace.clone(),
            repo: prepare.repo.clone(),
            commit: prepare.commit.clone(),
            root: view.root.clone(),
        };
        Ok((acquisition, permit))
    }

    pub async fn renew_preparation(&self, p: &GitSourcePreparePermit, ttl: i64) -> Result<bool> {
        if !(1..=3600).contains(&ttl) {
            bail!("source preparation TTL is invalid")
        }
        Ok(sqlx::query("UPDATE git_source_acquisitions SET expires_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT+$1 WHERE token=$2 AND generation=$3 AND operation_id=$4 AND owner=$5 AND attempt_id=$6 AND state='held' AND expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT").bind(ttl).bind(&p.token).bind(p.generation as i64).bind(&p.operation_id).bind(&p.owner).bind(&p.attempt_id).execute(&self.pool).await?.rows_affected()==1)
    }
    pub async fn renew(&self, a: &GitSourceAcquisition, ttl: i64) -> Result<bool> {
        if !(1..=3600).contains(&ttl) {
            bail!("source acquisition TTL is invalid")
        }
        Ok(sqlx::query("UPDATE git_source_acquisitions SET expires_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT+$1 WHERE token=$2 AND generation=$3 AND operation_id=$4 AND state='graph_published' AND expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT").bind(ttl).bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).execute(&self.pool).await?.rows_affected()==1)
    }

    pub async fn fail_preparation(
        &self,
        p: &GitSourcePreparePermit,
        class: FailureClass,
    ) -> Result<bool> {
        self.fail_token(&p.token, p.generation, &p.operation_id, "held", class)
            .await
    }
    pub async fn fail(&self, a: &GitSourceAcquisition, class: FailureClass) -> Result<bool> {
        self.fail_token(
            &a.token,
            a.generation,
            &a.operation_id,
            "graph_published",
            class,
        )
        .await
    }
    async fn fail_token(
        &self,
        token: &str,
        generation: u64,
        operation: &str,
        state: &str,
        class: FailureClass,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let changed=sqlx::query("UPDATE git_source_acquisitions SET state='failed',active_identity=NULL,failure_class=$1,expires_at=0 WHERE token=$2 AND generation=$3 AND operation_id=$4 AND state=$5").bind(class.as_str()).bind(token).bind(generation as i64).bind(operation).bind(state).execute(&mut *tx).await?.rows_affected();
        if changed == 0 {
            return Ok(false);
        }
        if sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class=$1,acquisition_token=NULL,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE acquisition_token=$2 AND state='acquiring'").bind(class.as_str()).bind(token).execute(&mut *tx).await?.rows_affected()!=1{bail!("source desire failure settlement lost")}
        tx.commit().await?;
        Ok(true)
    }

    pub async fn publish_protected<U: GitSourceUploader + Clone + 'static>(
        &self,
        a: &GitSourceAcquisition,
        packager: &GitSourcePackager<'_, U>,
        prepared: &PreparedGitSource,
        permit: &GitSourcePublicationPermit,
        cancelled: &CancellationToken,
    ) -> Result<()> {
        permit.validate(prepared)?;
        if a.token != permit.token || a.generation != permit.generation || a.root != permit.root {
            bail!("source acquisition and publication permit differ")
        }
        let plan = packager.owned_upload_plan(prepared)?;
        let owned = cancelled.child_token();
        let beat_cancel = owned.clone();
        let registry = (*self).clone();
        let acquisition = a.clone();
        let mut beat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {_ = beat_cancel.cancelled()=>return Ok(()),_=interval.tick()=>if !registry.renew(&acquisition,60).await?{beat_cancel.cancel();bail!("source acquisition lease was lost during upload")}}
            }
        });
        let upload_cancel = owned.clone();
        let mut upload = tokio::task::spawn_blocking(move || plan.publish(&upload_cancel));
        tokio::select! {result=&mut upload=>{owned.cancel();let uploaded=result.context("source upload task did not join")?;beat.await.context("source upload heartbeat did not join")??;uploaded},result=&mut beat=>{owned.cancel();let heartbeat=result.context("source upload heartbeat did not join")?;let uploaded=upload.await.context("cancelled source upload task did not join")?;heartbeat?;uploaded}}
    }

    pub async fn register(
        &self,
        a: &GitSourceAcquisition,
        prepared: &PreparedGitSource,
        cancelled: &CancellationToken,
    ) -> Result<DurableSourceSnapshot> {
        if cancelled.is_cancelled() {
            bail!("Git source registration cancelled")
        }
        let view = prepared.registry_view(&self.source_limits)?;
        verify_acquisition_identity(a, &view)?;
        let storage = self.storage.clone();
        let blobs = view
            .members
            .iter()
            .map(|m| m.blob.clone())
            .chain(std::iter::once(view.root.clone()))
            .collect::<Vec<_>>();
        let root_bytes = view.root_bytes.clone();
        let root_hash = view.root.hash.clone();
        let verify_cancel = cancelled.child_token();
        let blocking = verify_cancel.clone();
        let mut verify = tokio::task::spawn_blocking(move || {
            verify_storage_graph(&storage, &blobs, &root_hash, &root_bytes, &blocking)
        });
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {result=&mut verify=>{result.context("Git source verifier did not join")??;break},_=cancelled.cancelled()=>{verify_cancel.cancel();verify.await.context("cancelled Git source verifier did not join")??;bail!("Git source registration cancelled")},_=interval.tick()=>if !self.renew(a,60).await?{verify_cancel.cancel();verify.await.context("lease-lost Git source verifier did not join")??;bail!("Git source acquisition lease was lost during verification")}}
        }
        let mut unknown = self.pool.begin().await?;
        if sqlx::query("UPDATE git_source_acquisitions SET state='activation_unknown' WHERE token=$1 AND generation=$2 AND operation_id=$3 AND state='graph_published'").bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).execute(&mut *unknown).await?.rows_affected()!=1{bail!("source registration capability was lost")}
        unknown.commit().await?;
        let registration:Result<DurableSourceSnapshot>=async{let mut tx=self.pool.begin().await?;let now=postgres_time(&mut tx).await?;
            let descriptor:Option<(String,i64,String,String,String,i64,i64)>=sqlx::query_as("SELECT root_hash,root_len,object_format,semantic_digest,object_set_digest,object_count,total_bytes FROM git_source_acquisitions WHERE token=$1 AND generation=$2 AND operation_id=$3 AND state='activation_unknown' FOR UPDATE").bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).fetch_optional(&mut *tx).await?;let expected=(view.root.hash.clone(),checked_i64(view.root.len,"root length")?,view.object_format.to_owned(),view.semantic_digest.clone(),view.object_set_digest.clone(),checked_i64(view.object_count,"object count")?,checked_i64(view.total_bytes,"source bytes")?);if descriptor!=Some(expected){bail!("source acquisition descriptor differs at registration")}
            let members:Vec<(i64,String,i64,String)>=sqlx::query_as("SELECT ordinal,child_hash,child_len,kind FROM git_source_acquisition_members WHERE token=$1 ORDER BY ordinal").bind(&a.token).fetch_all(&mut *tx).await?;if members.len()!=view.members.len()||members.iter().zip(&view.members).any(|(got,want)|got.0!=want.ordinal as i64||got.1!=want.blob.hash||got.2!=want.blob.len as i64||got.3!=want.kind){bail!("source acquisition members differ at registration")}
            sqlx::query("INSERT INTO git_source_roots(root_hash,root_len,workspace,repo,commit_oid,source_format_version,object_format,semantic_digest,object_set_digest,object_count,total_bytes,registration_operation,registration_generation,state,created_at,registered_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'registered',$14,$15)").bind(&view.root.hash).bind(view.root.len as i64).bind(&view.workspace).bind(&view.repo).bind(&view.commit).bind(view.source_format_version as i64).bind(view.object_format).bind(&view.semantic_digest).bind(&view.object_set_digest).bind(view.object_count as i64).bind(view.total_bytes as i64).bind(&a.operation_id).bind(a.generation as i64).bind(now).bind(now).execute(&mut *tx).await?;
            for m in &view.members{sqlx::query("INSERT INTO git_source_members(root_hash,ordinal,child_hash,child_len,kind) VALUES($1,$2,$3,$4,$5)").bind(&view.root.hash).bind(m.ordinal as i64).bind(&m.blob.hash).bind(m.blob.len as i64).bind(m.kind).execute(&mut *tx).await?;}
            sqlx::query("UPDATE git_source_acquisitions SET state='registered',active_identity=NULL,expires_at=0 WHERE token=$1 AND generation=$2 AND state='activation_unknown'").bind(&a.token).bind(a.generation as i64).execute(&mut *tx).await?;sqlx::query("UPDATE git_source_desires SET state='registered',root_hash=$1,failure_class=NULL,acquisition_token=NULL,updated_at=$2 WHERE acquisition_token=$3 AND state='acquiring'").bind(&view.root.hash).bind(now).bind(&a.token).execute(&mut *tx).await?;let snapshot=DurableSourceSnapshot::registered(view.workspace.clone(),view.repo.clone(),view.commit.clone(),view.root.hash.clone(),a.token.clone(),a.generation)?;tx.commit().await?;Ok(snapshot)}.await;
        match registration {
            Ok(v) => Ok(v),
            Err(error) => match self.recover_unknown(a).await? {
                Some(v) => Ok(v),
                None => Err(error).context("PostgreSQL source registration settled failed"),
            },
        }
    }

    async fn recover_unknown(
        &self,
        a: &GitSourceAcquisition,
    ) -> Result<Option<DurableSourceSnapshot>> {
        let mut tx = self.pool.begin().await?;
        let root:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_roots WHERE root_hash=$1 AND workspace=$2 AND repo=$3 AND commit_oid=$4 AND source_format_version=$5 AND registration_operation=$6 AND registration_generation=$7 AND state='registered'").bind(&a.root.hash).bind(&a.workspace).bind(&a.repo).bind(&a.commit).bind(a.source_format_version as i64).bind(&a.operation_id).bind(a.generation as i64).fetch_one(&mut *tx).await?;
        if root == 1 {
            sqlx::query("UPDATE git_source_acquisitions SET state='registered',active_identity=NULL,expires_at=0 WHERE token=$1 AND generation=$2 AND state='activation_unknown'").bind(&a.token).bind(a.generation as i64).execute(&mut *tx).await?;
            sqlx::query("UPDATE git_source_desires SET state='registered',root_hash=$1,failure_class=NULL,acquisition_token=NULL,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE acquisition_token=$2 AND state='acquiring'").bind(&a.root.hash).bind(&a.token).execute(&mut *tx).await?;
            tx.commit().await?;
            return Ok(Some(DurableSourceSnapshot::registered(
                a.workspace.clone(),
                a.repo.clone(),
                a.commit.clone(),
                a.root.hash.clone(),
                a.token.clone(),
                a.generation,
            )?));
        }
        sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE acquisition_token=$1 AND state='acquiring'").bind(&a.token).execute(&mut *tx).await?;
        sqlx::query("UPDATE git_source_acquisitions SET state='failed',active_identity=NULL,failure_class='retryable',expires_at=0 WHERE token=$1 AND generation=$2 AND state='activation_unknown'").bind(&a.token).bind(a.generation as i64).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(None)
    }
    pub async fn live_source_objects_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<SourceGcObject>> {
        if limit == 0 || limit > SOURCE_ROOT_PAGE_MAX {
            bail!("source GC page limit is invalid")
        }
        let (hash, owner) = after.unwrap_or(("", ""));
        let rows=sqlx::query("WITH objects(hash,len,owner) AS (SELECT root_hash,root_len,CONCAT('r:',root_hash) FROM git_source_roots UNION ALL SELECT child_hash,child_len,CONCAT('r:',root_hash,':',LPAD(ordinal::text,20,'0')) FROM git_source_members UNION ALL SELECT root_hash,root_len,CONCAT('a:',token) FROM git_source_acquisitions WHERE state='activation_unknown' OR (state='graph_published' AND expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT) UNION ALL SELECT m.child_hash,m.child_len,CONCAT('a:',m.token,':',LPAD(m.ordinal::text,20,'0')) FROM git_source_acquisition_members m JOIN git_source_acquisitions a ON a.token=m.token WHERE a.state='activation_unknown' OR (a.state='graph_published' AND a.expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT)) SELECT hash,len,owner FROM objects WHERE hash>$1 OR (hash=$2 AND owner>$3) ORDER BY hash,owner LIMIT $4").bind(hash).bind(hash).bind(owner).bind(limit as i64).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                Ok(SourceGcObject {
                    hash: r.try_get("hash")?,
                    len: checked_u64(r.try_get("len")?, "source GC length")?,
                    owner: r.try_get("owner")?,
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn claim_authenticated(
        &self,
        artifact_id: i64,
        artifact_owner: &str,
        lease_generation: u64,
        workspace: &str,
        repo: &str,
        commit: &str,
        session_id: &str,
        ttl: i64,
    ) -> Result<AuthenticatedGitSource> {
        if artifact_id <= 0
            || artifact_owner.trim().is_empty()
            || lease_generation == 0
            || session_id.len() != 64
            || !session_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || !(1..=86400).contains(&ttl)
        {
            bail!("builder source claim is invalid")
        }
        let mut tx = self.pool.begin().await?;
        let row=sqlx::query("SELECT r.root_hash,r.root_len,r.object_format,r.registration_generation,r.registration_operation FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id JOIN git_source_roots r ON r.root_hash=i.source_root_hash WHERE i.artifact_id=$1 AND i.state='promoted' AND i.workspace=$2 AND i.repo=$3 AND i.commit_oid=$4 AND i.source_format_version=$5 AND j.state='running' AND j.owner=$6 AND j.lease_generation=$7 AND j.lease_expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT AND r.state='registered' FOR UPDATE").bind(artifact_id).bind(workspace).bind(repo).bind(commit).bind(SOURCE_FORMAT_VERSION as i64).bind(artifact_owner).bind(lease_generation as i64).fetch_optional(&mut *tx).await?.context("promoted artifact does not own a live registered source claim")?;
        let root = CasBlob {
            hash: row.try_get("root_hash")?,
            len: checked_u64(row.try_get("root_len")?, "root length")?,
        };
        let consumer = format!("builder:{artifact_id}:{session_id}");
        let claimed=sqlx::query("INSERT INTO git_source_consumers(root_hash,consumer_id,session_id,workspace,repo,commit_oid,source_format_version,purpose,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,'builder',EXTRACT(EPOCH FROM clock_timestamp())::BIGINT+$8) ON CONFLICT(root_hash,consumer_id) DO UPDATE SET expires_at=excluded.expires_at WHERE git_source_consumers.session_id=excluded.session_id AND git_source_consumers.workspace=excluded.workspace AND git_source_consumers.repo=excluded.repo AND git_source_consumers.commit_oid=excluded.commit_oid").bind(&root.hash).bind(&consumer).bind(session_id).bind(workspace).bind(repo).bind(commit).bind(SOURCE_FORMAT_VERSION as i64).bind(ttl).execute(&mut *tx).await?.rows_affected();
        if claimed != 1 {
            bail!("builder source claim conflicts with another exact capability")
        }
        let object_format =
            parse_object_format(row.try_get::<String, _>("object_format")?.as_str())?;
        let generation: i64 = row.try_get("registration_generation")?;
        let operation: String = row.try_get("registration_operation")?;
        let mac = evidence_mac(
            &self.seal,
            &root,
            workspace,
            repo,
            commit,
            object_format,
            generation,
            &operation,
        );
        let authority = AuthenticatedGitSource::from_registry_record(GitSourceRegistryRecord {
            root,
            workspace: workspace.into(),
            repo: repo.into(),
            commit: commit.into(),
            object_format,
            evidence_mac: mac,
        })?;
        tx.commit().await?;
        Ok(authority)
    }
    pub async fn renew_builder_claim(
        &self,
        artifact_id: i64,
        owner: &str,
        generation: u64,
        root: &str,
        session: &str,
        ttl: i64,
    ) -> Result<bool> {
        if artifact_id <= 0
            || owner.trim().is_empty()
            || generation == 0
            || !(1..=86400).contains(&ttl)
        {
            bail!("builder source claim TTL is invalid")
        }
        Ok(sqlx::query("UPDATE git_source_consumers SET expires_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT+$1 WHERE root_hash=$2 AND session_id=$3 AND purpose='builder' AND expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT AND EXISTS(SELECT 1 FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.artifact_id=$4 AND i.source_root_hash=git_source_consumers.root_hash AND i.state='promoted' AND j.state='running' AND j.owner=$5 AND j.lease_generation=$6 AND j.lease_expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT)").bind(ttl).bind(root).bind(session).bind(artifact_id).bind(owner).bind(generation as i64).execute(&self.pool).await?.rows_affected()==1)
    }
    pub async fn release_builder_claim(&self, root: &str, session: &str) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM git_source_consumers WHERE root_hash=$1 AND session_id=$2 AND purpose='builder'").bind(root).bind(session).execute(&self.pool).await?.rows_affected()==1)
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
        let ids: Vec<i64> = sqlx::query_scalar(
            "WITH ranked AS (SELECT id,workspace,updated_at,row_number() OVER(PARTITION BY workspace ORDER BY updated_at,id) AS lane_rank FROM artifact_intents WHERE state='deferred') SELECT id FROM ranked ORDER BY lane_rank,CASE WHEN workspace>$1 THEN 0 ELSE 1 END,workspace,updated_at,id LIMIT $2",
        )
        .bind(&cursor)
        .bind((limit as i64) * 64)
        .fetch_all(&self.pool)
        .await?;
        let mut promoted = 0;
        for id in ids {
            if promoted >= limit {
                break;
            }
            let mut tx = self.pool.begin().await?;
            sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
                .fetch_one(&mut *tx)
                .await?;
            let Some(row)=sqlx::query("SELECT workspace,repo,branch,branch_generation,commit_oid,kind,format_version,consumer_id FROM artifact_intents WHERE id=$1 AND state='deferred' FOR UPDATE").bind(id).fetch_optional(&mut *tx).await? else{tx.rollback().await?;continue};
            let workspace: String = row.try_get("workspace")?;
            let kind = ArtifactKind::parse(row.try_get("kind")?)?;
            if !postgres_capacity(&mut tx, &self.scheduler_limits, &workspace, kind).await? {
                tx.rollback().await?;
                continue;
            }
            let artifact = postgres_ensure_job(
                &mut tx,
                &workspace,
                row.try_get("repo")?,
                row.try_get("commit_oid")?,
                kind,
                row.try_get("format_version")?,
            )
            .await?;
            sqlx::query("UPDATE artifact_intents SET state='promoted',artifact_id=$1,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE id=$2 AND state='deferred'").bind(artifact).bind(id).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES($1,$2,$3) ON CONFLICT(artifact_id,consumer_id) DO UPDATE SET expires_at=excluded.expires_at").bind(artifact).bind(row.try_get::<String,_>("consumer_id")?).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *tx).await?;
            postgres_upsert_observation(
                &mut tx,
                &workspace,
                row.try_get("repo")?,
                row.try_get("branch")?,
                row.try_get("branch_generation")?,
                row.try_get("commit_oid")?,
                kind,
                artifact,
                row.try_get("format_version")?,
            )
            .await?;
            sqlx::query("UPDATE git_source_maintenance SET intent_workspace_cursor=$1,intent_cursor=$2,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE id=1").bind(&workspace).bind(id).execute(&mut *tx).await?;
            tx.commit().await?;
            promoted += 1
        }
        Ok(promoted)
    }

    pub async fn reconcile_terminal_intents(&self, limit: u32) -> Result<u32> {
        if limit == 0 || limit > 512 {
            bail!("intent reconciliation page is invalid")
        }
        let ids:Vec<(i64,i64,String)>=sqlx::query_as("SELECT i.id,i.artifact_id,i.consumer_id FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.state='promoted' AND (j.state='ready' OR (j.state='failed' AND (j.failure_class IN('permanent','dead_letter') OR (j.failure_class='retryable' AND j.retry_count>=$1)))) ORDER BY i.id LIMIT $2").bind(self.scheduler_limits.max_manual_retries as i64).bind(limit as i64).fetch_all(&self.pool).await?;
        let mut settled = 0;
        for (id, artifact, consumer) in ids {
            let mut tx = self.pool.begin().await?;
            let terminal:Option<i64>=sqlx::query_scalar("SELECT i.id FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.id=$1 AND i.artifact_id=$2 AND i.consumer_id=$3 AND i.state='promoted' AND (j.state='ready' OR (j.state='failed' AND (j.failure_class IN('permanent','dead_letter') OR (j.failure_class='retryable' AND j.retry_count>=$4)))) FOR UPDATE OF i,j").bind(id).bind(artifact).bind(&consumer).bind(self.scheduler_limits.max_manual_retries as i64).fetch_optional(&mut *tx).await?;
            if terminal.is_none() {
                tx.rollback().await?;
                continue;
            }
            if sqlx::query(
                "DELETE FROM git_source_consumers WHERE consumer_id=$1 AND purpose='intent'",
            )
            .bind(&consumer)
            .execute(&mut *tx)
            .await?
            .rows_affected()
                != 1
                || sqlx::query(
                    "DELETE FROM artifact_consumers WHERE artifact_id=$1 AND consumer_id=$2",
                )
                .bind(artifact)
                .bind(&consumer)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    != 1
            {
                bail!("terminal intent consumers are incomplete")
            }
            sqlx::query(
                "DELETE FROM artifact_intents WHERE id=$1 AND artifact_id=$2 AND state='promoted'",
            )
            .bind(id)
            .bind(artifact)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            settled += 1
        }
        Ok(settled)
    }

    pub async fn prune_metadata_page(&self, limit: u32) -> Result<u64> {
        if limit == 0 || limit > 512 {
            bail!("source metadata prune page is invalid")
        }
        let mut tx = self.pool.begin().await?;
        let mut changed=sqlx::query("WITH victims AS (SELECT ctid FROM git_source_consumers WHERE purpose='builder' AND expires_at<=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT ORDER BY expires_at,root_hash,consumer_id LIMIT $1 FOR UPDATE SKIP LOCKED) DELETE FROM git_source_consumers WHERE ctid IN(SELECT ctid FROM victims)").bind(limit as i64).execute(&mut *tx).await?.rows_affected();
        changed+=sqlx::query("WITH victims AS (SELECT g.ctid FROM branch_source_generations g LEFT JOIN branch_source_current c ON c.workspace=g.workspace AND c.repo=g.repo AND c.branch=g.branch AND c.generation=g.generation LEFT JOIN artifact_intents i ON i.workspace=g.workspace AND i.repo=g.repo AND i.branch=g.branch AND i.branch_generation=g.generation WHERE c.workspace IS NULL AND i.id IS NULL ORDER BY g.created_at,g.workspace,g.repo,g.branch,g.generation LIMIT $1 FOR UPDATE OF g SKIP LOCKED) DELETE FROM branch_source_generations WHERE ctid IN(SELECT ctid FROM victims)").bind(limit as i64).execute(&mut *tx).await?.rows_affected();
        let cutoff: i64 = sqlx::query_scalar(
            "SELECT GREATEST(0,generation-1024) FROM git_source_acquisition_sequence WHERE id=1",
        )
        .fetch_one(&mut *tx)
        .await?;
        changed+=sqlx::query("WITH victims AS (SELECT a.ctid FROM git_source_acquisitions a LEFT JOIN git_source_desires d ON d.acquisition_token=a.token WHERE a.state='failed' AND a.generation<=$1 AND d.acquisition_token IS NULL ORDER BY a.generation LIMIT $2 FOR UPDATE OF a SKIP LOCKED) DELETE FROM git_source_acquisitions WHERE ctid IN(SELECT ctid FROM victims)").bind(cutoff).bind(limit as i64).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(changed)
    }

    pub async fn retire_registered_roots_page(&self, grace_secs: i64, limit: u32) -> Result<u32> {
        if !(60..=30 * 24 * 60 * 60).contains(&grace_secs) || limit == 0 || limit > 256 {
            bail!("source root retirement grace or page is invalid")
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        let sweep:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_gc_sweep WHERE expires_at>EXTRACT(EPOCH FROM clock_timestamp())::BIGINT").fetch_one(&mut *tx).await?;
        if sweep != 0 {
            bail!("source root retirement is fenced by live GC sweep")
        }
        let cursor: String = sqlx::query_scalar(
            "SELECT root_cursor FROM git_source_maintenance WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let roots:Vec<String>=sqlx::query_scalar("SELECT r.root_hash FROM git_source_roots r WHERE r.state='registered' AND r.registered_at<=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT-$1 AND r.root_hash>$2 AND NOT EXISTS(SELECT 1 FROM branch_source_generations g WHERE g.root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM artifact_intents i WHERE i.source_root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM git_source_consumers c WHERE c.root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM git_source_acquisitions a WHERE a.root_hash=r.root_hash AND a.state IN('held','graph_published','activation_unknown')) ORDER BY r.root_hash LIMIT $3 FOR UPDATE OF r SKIP LOCKED").bind(grace_secs).bind(&cursor).bind(limit as i64).fetch_all(&mut *tx).await?;
        if roots.is_empty() {
            if !cursor.is_empty() {
                sqlx::query("UPDATE git_source_maintenance SET root_cursor='',updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE id=1").execute(&mut *tx).await?;
            }
            tx.commit().await?;
            return Ok(0);
        }
        for root in &roots {
            sqlx::query("DELETE FROM git_source_desires WHERE root_hash=$1 AND state='registered'")
                .bind(root)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM git_source_acquisitions WHERE root_hash=$1 AND state IN('registered','failed')").bind(root).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM git_source_members WHERE root_hash=$1")
                .bind(root)
                .execute(&mut *tx)
                .await?;
            if sqlx::query("DELETE FROM git_source_roots WHERE root_hash=$1 AND state='registered' AND NOT EXISTS(SELECT 1 FROM branch_source_generations WHERE root_hash=$2) AND NOT EXISTS(SELECT 1 FROM artifact_intents WHERE source_root_hash=$3) AND NOT EXISTS(SELECT 1 FROM git_source_consumers WHERE root_hash=$4)").bind(root).bind(root).bind(root).bind(root).execute(&mut *tx).await?.rows_affected()!=1{bail!("source root retirement lost its reference proof")}
        }
        sqlx::query("UPDATE git_source_maintenance SET root_cursor=$1,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE id=1").bind(roots.last().unwrap()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(roots.len() as u32)
    }
}

#[async_trait]
impl ArtifactObservation for PostgresGitSourceRegistry {
    async fn snapshot(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<ObservationSnapshot> {
        let row:Option<(i64,String)>=sqlx::query_as("SELECT generation,desired_commit FROM branch_observations WHERE workspace=$1 AND repo=$2 AND branch=$3").bind(workspace).bind(repo).bind(branch).fetch_optional(&self.pool).await?;
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
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        let registered:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions a JOIN git_source_roots r ON r.root_hash=a.root_hash WHERE a.token=$1 AND a.generation=$2 AND a.state='registered' AND a.workspace=$3 AND a.repo=$4 AND a.commit_oid=$5 AND a.root_hash=$6 AND r.state='registered'").bind(source.registration_token()).bind(source.registration_generation() as i64).bind(source.workspace()).bind(source.repo()).bind(source.commit()).bind(source.manifest()).fetch_one(&mut *tx).await?;
        if registered != 1 {
            bail!("source snapshot is not an exact registered capability")
        }
        let current:Option<(i64,String)>=sqlx::query_as("SELECT generation,desired_commit FROM branch_observations WHERE workspace=$1 AND repo=$2 AND branch=$3 FOR UPDATE").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).fetch_optional(&mut *tx).await?;
        let current_generation = current
            .as_ref()
            .map(|v| checked_u64(v.0, "branch generation"))
            .transpose()?;
        if current_generation != snapshot.generation() {
            tx.rollback().await?;
            return Ok(ArtifactObservationOutcome::Stale {
                current_generation: current_generation.unwrap_or(0),
            });
        }
        let same = current.as_ref().is_some_and(|v| v.1 == source.commit());
        let generation = if same {
            current_generation.context("same tip lacks generation")?
        } else {
            current_generation
                .unwrap_or(0)
                .checked_add(1)
                .context("branch generation overflow")?
        };
        if !same {
            let deferred:Vec<String>=sqlx::query_scalar("SELECT consumer_id FROM artifact_intents WHERE workspace=$1 AND repo=$2 AND branch=$3 AND state='deferred'").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).fetch_all(&mut *tx).await?;
            for consumer in deferred {
                sqlx::query(
                    "DELETE FROM git_source_consumers WHERE consumer_id=$1 AND purpose='intent'",
                )
                .bind(consumer)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query("DELETE FROM artifact_intents WHERE workspace=$1 AND repo=$2 AND branch=$3 AND state='deferred'").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO branch_source_generations(workspace,repo,branch,generation,commit_oid,source_format_version,root_hash,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT)").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).bind(SOURCE_FORMAT_VERSION as i64).bind(source.manifest()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO branch_observations(workspace,repo,branch,generation,desired_commit,updated_at) VALUES($1,$2,$3,$4,$5,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT) ON CONFLICT(workspace,repo,branch) DO UPDATE SET generation=excluded.generation,desired_commit=excluded.desired_commit,updated_at=excluded.updated_at").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO branch_source_current(workspace,repo,branch,generation) VALUES($1,$2,$3,$4) ON CONFLICT(workspace,repo,branch) DO UPDATE SET generation=excluded.generation").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).execute(&mut *tx).await?;
        } else {
            let exact:i64=sqlx::query_scalar("SELECT count(*) FROM branch_source_generations g JOIN branch_source_current c ON c.workspace=g.workspace AND c.repo=g.repo AND c.branch=g.branch AND c.generation=g.generation WHERE g.workspace=$1 AND g.repo=$2 AND g.branch=$3 AND g.generation=$4 AND g.commit_oid=$5 AND g.root_hash=$6").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).bind(source.manifest()).fetch_one(&mut *tx).await?;
            if exact != 1 {
                bail!("same-tip source generation differs from registered capability")
            }
        }
        let mut outcomes = Vec::new();
        for kind in unique {
            if let Some((id,state,artifact))=sqlx::query_as::<_,(i64,String,Option<i64>)>("SELECT id,state,artifact_id FROM artifact_intents WHERE workspace=$1 AND repo=$2 AND branch=$3 AND branch_generation=$4 AND kind=$5 AND format_version=$6").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(kind.as_str()).bind(format_version as i64).fetch_optional(&mut *tx).await?{if state=="deferred"{outcomes.push((kind,ArtifactIntentOutcome::Deferred(id)));continue}let artifact=artifact.context("promoted intent lacks artifact")?;postgres_upsert_observation(&mut tx,snapshot.workspace(),snapshot.repo(),snapshot.branch(),generation as i64,source.commit(),kind,artifact,format_version as i64).await?;outcomes.push((kind,postgres_job_outcome(&mut tx,artifact,intent,self.scheduler_limits.max_manual_retries).await?));continue}
            let consumer = format!(
                "{}{}",
                SOURCE_INTENT_CONSUMER_PREFIX,
                hex::encode(rand::random::<[u8; 24]>())
            );
            let session = hex::encode(rand::random::<[u8; 32]>());
            let existing:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND kind=$4 AND format_version=$5").bind(snapshot.workspace()).bind(snapshot.repo()).bind(source.commit()).bind(kind.as_str()).bind(format_version as i64).fetch_one(&mut *tx).await?;
            let promote = existing == 1
                || postgres_capacity(&mut tx, &self.scheduler_limits, snapshot.workspace(), kind)
                    .await?;
            let artifact = if promote {
                Some(
                    postgres_ensure_job(
                        &mut tx,
                        snapshot.workspace(),
                        snapshot.repo(),
                        source.commit(),
                        kind,
                        format_version as i64,
                    )
                    .await?,
                )
            } else {
                None
            };
            let intent_id:i64=sqlx::query_scalar("INSERT INTO artifact_intents(workspace,repo,branch,branch_generation,source_root_hash,source_format_version,commit_oid,kind,format_version,state,artifact_id,consumer_id,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, $12,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT) RETURNING id").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.manifest()).bind(SOURCE_FORMAT_VERSION as i64).bind(source.commit()).bind(kind.as_str()).bind(format_version as i64).bind(if promote{"promoted"}else{"deferred"}).bind(artifact).bind(&consumer).fetch_one(&mut *tx).await?;
            sqlx::query("INSERT INTO git_source_consumers(root_hash,consumer_id,session_id,workspace,repo,commit_oid,source_format_version,purpose,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,'intent',$8)").bind(source.manifest()).bind(&consumer).bind(session).bind(source.workspace()).bind(source.repo()).bind(source.commit()).bind(SOURCE_FORMAT_VERSION as i64).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *tx).await?;
            if let Some(artifact) = artifact {
                sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES($1,$2,$3)").bind(artifact).bind(&consumer).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *tx).await?;
                postgres_upsert_observation(
                    &mut tx,
                    snapshot.workspace(),
                    snapshot.repo(),
                    snapshot.branch(),
                    generation as i64,
                    source.commit(),
                    kind,
                    artifact,
                    format_version as i64,
                )
                .await?;
                outcomes.push((
                    kind,
                    postgres_job_outcome(
                        &mut tx,
                        artifact,
                        intent,
                        self.scheduler_limits.max_manual_retries,
                    )
                    .await?,
                ))
            } else {
                outcomes.push((kind, ArtifactIntentOutcome::Deferred(intent_id)))
            }
        }
        tx.commit().await?;
        Ok(ArtifactObservationOutcome::Recorded {
            generation,
            advanced: !same,
            artifacts: outcomes,
        })
    }
}

async fn postgres_upsert_observation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace: &str,
    repo: &str,
    branch: &str,
    generation: i64,
    commit: &str,
    kind: ArtifactKind,
    artifact: i64,
    format: i64,
) -> Result<()> {
    sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES($1,$2,$3,$4,$5,$6,$7,CASE WHEN (SELECT state FROM artifact_jobs WHERE id=$8)='ready' THEN $9 ELSE NULL END,$10,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT) ON CONFLICT(workspace,repo,branch,kind) DO UPDATE SET desired_commit=excluded.desired_commit,desired_artifact_id=excluded.desired_artifact_id,desired_generation=excluded.desired_generation,published_artifact_id=CASE WHEN excluded.published_artifact_id IS NOT NULL THEN excluded.published_artifact_id WHEN artifact_observations.format_version=excluded.format_version THEN artifact_observations.published_artifact_id ELSE NULL END,format_version=excluded.format_version,observed_at=excluded.observed_at").bind(workspace).bind(repo).bind(branch).bind(kind.as_str()).bind(commit).bind(artifact).bind(generation).bind(artifact).bind(artifact).bind(format).execute(&mut **tx).await?;
    Ok(())
}

async fn postgres_capacity(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    limits: &SchedulerLimits,
    workspace: &str,
    kind: ArtifactKind,
) -> Result<bool> {
    let total: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')")
            .fetch_one(&mut **tx)
            .await?;
    let local: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=$1",
    )
    .bind(workspace)
    .fetch_one(&mut **tx)
    .await?;
    let lane: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind=$1",
    )
    .bind(kind.as_str())
    .fetch_one(&mut **tx)
    .await?;
    let expensive:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind IN('full_history','files')").fetch_one(&mut **tx).await?;
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
async fn postgres_ensure_job(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace: &str,
    repo: &str,
    commit: &str,
    kind: ArtifactKind,
    format: i64,
) -> Result<i64> {
    if let Some(id)=sqlx::query_scalar("SELECT id FROM artifact_jobs WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND kind=$4 AND format_version=$5").bind(workspace).bind(repo).bind(commit).bind(kind.as_str()).bind(format).fetch_optional(&mut **tx).await?{return Ok(id)}
    Ok(sqlx::query_scalar("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'queued',EXTRACT(EPOCH FROM clock_timestamp())::BIGINT,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT) RETURNING id").bind(workspace).bind(repo).bind(commit).bind(kind.as_str()).bind(format).fetch_one(&mut **tx).await?)
}
async fn postgres_job_outcome(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: i64,
    intent: SyncIntent,
    max_retries: u32,
) -> Result<ArtifactIntentOutcome> {
    let row = sqlx::query("SELECT state,failure_class,retry_count FROM artifact_jobs WHERE id=$1")
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
    let mut state: String = row.try_get("state")?;
    let class = row
        .try_get::<Option<String>, _>("failure_class")?
        .map(|v| FailureClass::parse(&v))
        .transpose()?;
    let retries = checked_u32(row.try_get("retry_count")?, "artifact retries")?;
    if state=="failed"&&intent==SyncIntent::EnsureCurrent&&class==Some(FailureClass::Retryable)&&retries<max_retries&&sqlx::query("UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,manifest=NULL,error=NULL,failure_class=NULL,retry_count=retry_count+1,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE id=$1 AND state='failed' AND failure_class='retryable' AND retry_count=$2").bind(id).bind(retries as i64).execute(&mut **tx).await?.rows_affected()==1{state="queued".into()}
    Ok(match state.as_str() {
        "ready" => ArtifactIntentOutcome::Ready(id),
        "failed" => ArtifactIntentOutcome::Failed(id, class.unwrap_or(FailureClass::Permanent)),
        "queued" | "running" => ArtifactIntentOutcome::Subscribed(id),
        _ => bail!("artifact job state is invalid"),
    })
}

fn source_identity(workspace: &str, repo: &str, commit: &str, version: u32) -> String {
    let mut h = Sha256::new();
    for value in [workspace, repo, commit] {
        h.update((value.len() as u64).to_be_bytes());
        h.update(value.as_bytes())
    }
    h.update(version.to_be_bytes());
    hex::encode(h.finalize())
}
async fn postgres_time(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut **tx)
            .await?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{
        ClaimedArtifact, CompletionEvidence, CompletionVerifier, validate_evidence,
    };
    use crate::artifact_scheduler_postgres::PostgresArtifactScheduler;
    use crate::git_source::prepared_source_for_registry_test;
    use sqlx::postgres::PgPoolOptions;

    struct Accept;
    impl CompletionVerifier for Accept {
        fn identity(&self) -> &'static str {
            "postgres-source-registry-live-v1"
        }
        fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
            validate_evidence(claim, evidence)
        }
    }

    async fn reset(pool: &PgPool) {
        for table in [
            "artifact_intents",
            "git_source_consumers",
            "branch_source_current",
            "branch_source_generations",
            "git_source_desires",
            "git_source_acquisition_members",
            "git_source_acquisitions",
            "git_source_acquisition_sequence",
            "git_source_maintenance",
            "git_source_members",
            "git_source_roots",
            "ready_publication_fence_members",
            "ready_publication_fences",
            "ready_publication_fence_sequence",
            "artifact_base_retention",
            "artifact_gc_sweep",
            "artifact_transport_leases",
            "artifact_consumers",
            "artifact_observations",
            "branch_observations",
            "artifact_jobs",
            "scheduler_state",
            "artifact_scheduler_schema",
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP TABLE IF EXISTS {table} CASCADE"
            )))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn postgres_v7_source_registry_live_matrix() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&url)
            .await
            .unwrap();
        let mut lock = pool.acquire().await.unwrap().detach();
        sqlx::query("SELECT pg_advisory_lock(731904220)")
            .execute(&mut lock)
            .await
            .unwrap();
        reset(&pool).await;
        let limits = SchedulerLimits::default();
        let (a, b) = tokio::join!(
            PostgresArtifactScheduler::from_pool(pool.clone(), limits.clone(), Arc::new(Accept)),
            PostgresArtifactScheduler::from_pool(pool.clone(), limits.clone(), Arc::new(Accept))
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            7
        );
        let temp = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(temp.path()).unwrap();
        let registry = PostgresGitSourceRegistry::new(
            pool.clone(),
            storage.clone(),
            limits.clone(),
            GitSourceLimits::default(),
            [7; 32],
        )
        .await
        .unwrap();
        assert!(
            PostgresGitSourceRegistry::new(
                pool.clone(),
                storage.clone(),
                limits.clone(),
                GitSourceLimits::default(),
                [8; 32]
            )
            .await
            .is_err(),
            "authority seal drift was accepted"
        );
        assert!(
            registry
                .begin_acquisition(
                    "",
                    "o/r",
                    &"a".repeat(40),
                    1,
                    "owner",
                    "attempt",
                    60,
                    SyncIntent::EnsureCurrent
                )
                .await
                .is_err()
        );
        let permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &"a".repeat(40),
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
            _ => panic!("expected Held"),
        };
        assert!(matches!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    &"a".repeat(40),
                    1,
                    "other",
                    "other",
                    60,
                    SyncIntent::EnsureCurrent
                )
                .await
                .unwrap(),
            SourceBeginOutcome::Deferred { .. }
        ));
        let pack_bytes = b"pack";
        let index_bytes = b"index";
        let pack = CasBlob {
            hash: hex::encode(Sha256::digest(pack_bytes)),
            len: 4,
        };
        let index = CasBlob {
            hash: hex::encode(Sha256::digest(index_bytes)),
            len: 5,
        };
        storage.put(&pack.hash, pack_bytes).unwrap();
        storage.put(&index.hash, index_bytes).unwrap();
        let prepared =
            prepared_source_for_registry_test("ws", "o/r", &"a".repeat(40), pack, index).unwrap();
        let view = prepared.registry_view(&GitSourceLimits::default()).unwrap();
        storage.put(&view.root.hash, &view.root_bytes).unwrap();
        let (acquisition, _) = registry
            .bind_prepared_graph(&permit, &prepared)
            .await
            .unwrap();
        let durable = registry
            .register(&acquisition, &prepared, &CancellationToken::new())
            .await
            .unwrap();
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let observed = registry
            .record_tip_and_intents(
                &before,
                &durable,
                &[
                    ArtifactKind::Head,
                    ArtifactKind::FullHistory,
                    ArtifactKind::Files,
                ],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        assert!(
            matches!(observed,ArtifactObservationOutcome::Recorded{advanced:true,artifacts,..} if artifacts.len()==3)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact_intents")
                .fetch_one(&pool)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact_observations")
                .fetch_one(&pool)
                .await
                .unwrap(),
            3
        );
        let prepared_cancel = prepared_source_for_registry_test(
            "ws",
            "o/r",
            &"b".repeat(40),
            view.members[0].blob.clone(),
            view.members[1].blob.clone(),
        )
        .unwrap();
        let cancel_view = prepared_cancel
            .registry_view(&GitSourceLimits::default())
            .unwrap();
        storage
            .put(&cancel_view.root.hash, &cancel_view.root_bytes)
            .unwrap();
        let cancel_permit = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &"b".repeat(40),
                1,
                "owner",
                "cancel",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::PermitToPrepare(v) => v,
            _ => panic!("expected second Held"),
        };
        let (cancel_acquisition, _) = registry
            .bind_prepared_graph(&cancel_permit, &prepared_cancel)
            .await
            .unwrap();
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            registry
                .register(&cancel_acquisition, &prepared_cancel, &cancelled)
                .await
                .is_err(),
            "pre-cancelled registration was admitted"
        );
        // A cancelled verifier preserves an activation-recoverable/failed durable state; restart does not mint another active identity.
        assert!(matches!(
            registry
                .begin_acquisition(
                    "ws",
                    "o/r",
                    &"b".repeat(40),
                    1,
                    "owner",
                    "restart",
                    60,
                    SyncIntent::EnsureCurrent
                )
                .await
                .unwrap(),
            SourceBeginOutcome::Failed { .. }
                | SourceBeginOutcome::ActivationUnknown { .. }
                | SourceBeginOutcome::Deferred { .. }
        ));
        validate_postgres_v7(&mut pool.acquire().await.unwrap().detach(), true)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_maintenance SET id=2 WHERE id=1")
            .execute(&pool)
            .await
            .unwrap_err();
        sqlx::query("UPDATE git_source_maintenance SET config_fingerprint=upper(config_fingerprint) WHERE id=1").execute(&pool).await.unwrap();
        assert!(
            PostgresGitSourceRegistry::new(
                pool.clone(),
                storage,
                limits,
                GitSourceLimits::default(),
                [7; 32]
            )
            .await
            .is_err(),
            "malformed durable fingerprint was accepted"
        );
        reset(&pool).await;
        sqlx::query("SELECT pg_advisory_unlock(731904220)")
            .execute(&mut lock)
            .await
            .unwrap();
    }
}
