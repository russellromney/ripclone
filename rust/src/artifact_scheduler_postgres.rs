//! PostgreSQL persistence for the normalized artifact scheduler.
//!
//! Admission and claim transactions lock the singleton scheduler control row.
//! The lock is held only while touching normalized rows; heartbeats and fenced
//! settlement are O(1) conditional updates and do not take the control lock.

use crate::artifact_scheduler::{
    ActivationFenceProvenance, ArtifactKey, ArtifactKind, ArtifactRecord, ArtifactState,
    ClaimedArtifact, CompletionSealAuthority, CompletionVerifier, FailureClass, ObservationOutcome,
    ObservationSnapshot, QuarantineOutcome, ReadyPublicationFence, RetryOutcome, ScheduleOutcome,
    SchedulerLimits, UnknownActivationFencePage, VerifiedCompletionEvidence, scheduler_fingerprint,
    validate_format_version, validate_lease, validate_limits, validate_observation_identity,
    validate_resolved_commit,
};
#[cfg(test)]
use crate::artifact_scheduler::{CompletionEvidence, validate_evidence};
use crate::artifact_scheduler_backend::{
    ArtifactSchedulerPersistence, GcDeleteFence, SchedulerGcRoot, TRANSPORT_ROOT_PAGE_MAX,
    TransportRootLease, validate_transport_lease_identity,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::{Postgres, Row, Transaction};
use std::sync::Arc;

const SCHEMA_VERSION: i64 = 6;
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS artifact_scheduler_schema(
 id SMALLINT CONSTRAINT artifact_scheduler_schema_pkey PRIMARY KEY,
 version BIGINT NOT NULL,
 CONSTRAINT artifact_scheduler_schema_singleton CHECK(id=1));
CREATE TABLE IF NOT EXISTS artifact_jobs(
 id BIGSERIAL CONSTRAINT artifact_jobs_pkey PRIMARY KEY,
 workspace TEXT NOT NULL, repo TEXT NOT NULL,
 commit_oid TEXT NOT NULL, kind TEXT NOT NULL,
 format_version BIGINT NOT NULL,
 state TEXT NOT NULL, owner TEXT,
 heartbeat_at BIGINT, lease_expires_at BIGINT,
 lease_generation BIGINT NOT NULL DEFAULT 0,
 claim_attempts BIGINT NOT NULL DEFAULT 0,
 retry_count BIGINT NOT NULL DEFAULT 0,
 manifest TEXT, error TEXT, failure_class TEXT, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
 CONSTRAINT artifact_jobs_identity UNIQUE(workspace,repo,commit_oid,kind,format_version),
 CONSTRAINT artifact_jobs_format CHECK(format_version BETWEEN 1 AND 4294967295),
 CONSTRAINT artifact_jobs_state CHECK(state IN('queued','running','ready','failed')),
 CONSTRAINT artifact_jobs_kind CHECK(kind IN('head','full_history','files')),
 CONSTRAINT artifact_jobs_lease_generation CHECK(lease_generation BETWEEN 0 AND 9223372036854775807),
 CONSTRAINT artifact_jobs_claim_attempts CHECK(claim_attempts BETWEEN 0 AND 4294967295),
 CONSTRAINT artifact_jobs_retry_count CHECK(retry_count BETWEEN 0 AND 4294967295),
 CONSTRAINT artifact_jobs_failure_class CHECK(failure_class IS NULL OR failure_class IN('retryable','permanent','dead_letter')));
CREATE INDEX IF NOT EXISTS artifact_jobs_claim ON artifact_jobs(state,kind,created_at,id);
CREATE INDEX IF NOT EXISTS artifact_jobs_lease ON artifact_jobs(state,lease_expires_at);
CREATE TABLE IF NOT EXISTS branch_observations(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,
 generation BIGINT NOT NULL,
 desired_commit TEXT NOT NULL,updated_at BIGINT NOT NULL,
 CONSTRAINT branch_observations_pkey PRIMARY KEY(workspace,repo,branch),
 CONSTRAINT branch_observations_generation CHECK(generation>=1));
CREATE TABLE IF NOT EXISTS artifact_observations(
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,kind TEXT NOT NULL,
 desired_commit TEXT NOT NULL,desired_artifact_id BIGINT NOT NULL,
 desired_generation BIGINT NOT NULL,
 published_artifact_id BIGINT,
 format_version BIGINT NOT NULL,
 observed_at BIGINT NOT NULL,
 CONSTRAINT artifact_observations_pkey PRIMARY KEY(workspace,repo,branch,kind),
 CONSTRAINT artifact_observations_generation CHECK(desired_generation>=1),
 CONSTRAINT artifact_observations_format CHECK(format_version BETWEEN 1 AND 4294967295));
CREATE INDEX IF NOT EXISTS artifact_observations_desired
 ON artifact_observations(desired_artifact_id);
CREATE INDEX IF NOT EXISTS artifact_observations_published
 ON artifact_observations(published_artifact_id);
CREATE TABLE IF NOT EXISTS artifact_consumers(
 artifact_id BIGINT NOT NULL,consumer_id TEXT NOT NULL,expires_at BIGINT NOT NULL,
 CONSTRAINT artifact_consumers_pkey PRIMARY KEY(artifact_id,consumer_id));
CREATE INDEX IF NOT EXISTS artifact_consumers_expiry ON artifact_consumers(expires_at);
CREATE TABLE IF NOT EXISTS artifact_transport_leases(
 root_hash TEXT NOT NULL,session_id TEXT NOT NULL,workspace TEXT NOT NULL,repo TEXT NOT NULL,
 expires_at BIGINT NOT NULL,
 CONSTRAINT artifact_transport_leases_pkey PRIMARY KEY(root_hash,session_id));
CREATE INDEX IF NOT EXISTS artifact_transport_leases_expiry
 ON artifact_transport_leases(expires_at);
CREATE TABLE IF NOT EXISTS artifact_gc_sweep(
 id SMALLINT CONSTRAINT artifact_gc_sweep_pkey PRIMARY KEY,owner TEXT NOT NULL,expires_at BIGINT NOT NULL,
 CONSTRAINT artifact_gc_sweep_singleton CHECK(id=1));
CREATE TABLE IF NOT EXISTS artifact_base_retention(
 artifact_id BIGINT CONSTRAINT artifact_base_retention_pkey PRIMARY KEY,
 workspace TEXT NOT NULL,repo TEXT NOT NULL,format_version BIGINT NOT NULL,
 head_rank SMALLINT,pair_rank SMALLINT,
 CONSTRAINT artifact_base_retention_artifact FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE,
 CONSTRAINT artifact_base_retention_ranks CHECK((head_rank IS NULL OR head_rank BETWEEN 1 AND 8) AND (pair_rank IS NULL OR pair_rank BETWEEN 1 AND 8) AND (head_rank IS NOT NULL OR pair_rank IS NOT NULL)));
CREATE INDEX IF NOT EXISTS artifact_base_retention_scope ON artifact_base_retention(workspace,repo,format_version);
CREATE TABLE IF NOT EXISTS scheduler_state(
 id SMALLINT CONSTRAINT scheduler_state_pkey PRIMARY KEY,
 fairness_cursor BIGINT NOT NULL,
 workspace_cursor TEXT NOT NULL DEFAULT '',config_fingerprint TEXT NOT NULL DEFAULT '',
 CONSTRAINT scheduler_state_singleton CHECK(id=1),
 CONSTRAINT scheduler_state_fairness CHECK(fairness_cursor BETWEEN 0 AND 3));
INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0) ON CONFLICT(id) DO NOTHING;
CREATE TABLE IF NOT EXISTS ready_publication_fence_sequence(
 id SMALLINT PRIMARY KEY CHECK(id=1),generation BIGINT NOT NULL CHECK(generation>=0));
INSERT INTO ready_publication_fence_sequence(id,generation) VALUES(1,0) ON CONFLICT(id) DO NOTHING;
CREATE TABLE IF NOT EXISTS ready_publication_fences(
 token TEXT PRIMARY KEY,generation BIGINT NOT NULL UNIQUE CHECK(generation>0),operation_id TEXT NOT NULL UNIQUE,
 workspace TEXT NOT NULL,repo TEXT NOT NULL,branch TEXT NOT NULL,target TEXT NOT NULL,attempt_id TEXT NOT NULL,
 expires_at BIGINT NOT NULL,state TEXT NOT NULL CHECK(state IN('held','activation_unknown')),UNIQUE(token,generation));
CREATE TABLE IF NOT EXISTS ready_publication_fence_members(
 token TEXT NOT NULL,generation BIGINT NOT NULL CHECK(generation>0),artifact_id BIGINT NOT NULL,
 manifest TEXT NOT NULL CHECK(length(trim(manifest))>0),PRIMARY KEY(token,artifact_id),
 FOREIGN KEY(token,generation) REFERENCES ready_publication_fences(token,generation) ON DELETE CASCADE,
 FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS ready_publication_fences_recovery ON ready_publication_fences(state,generation,token);
"#;

#[derive(Clone)]
pub struct PostgresArtifactScheduler {
    pool: PgPool,
    limits: SchedulerLimits,
    verifier: Arc<dyn CompletionVerifier>,
    completion_sealer: Arc<CompletionSealAuthority>,
}
struct PostgresGcDeleteFence(Option<Transaction<'static, Postgres>>);

fn validate_fence_request(
    expected: &[(i64, Option<String>)],
    p: &ActivationFenceProvenance,
    ttl: i64,
) -> Result<()> {
    if expected.len() != 2
        || expected[0].0 == expected[1].0
        || expected
            .iter()
            .any(|(id, m)| *id <= 0 || m.as_deref().is_none_or(|v| v.trim().is_empty()))
        || [&p.workspace, &p.repo, &p.branch, &p.target, &p.attempt_id]
            .iter()
            .any(|v| v.trim().is_empty())
        || !(1..=3600).contains(&ttl)
    {
        bail!("invalid Ready publication fence")
    }
    Ok(())
}
fn sorted_expected(expected: &[(i64, Option<String>)]) -> Vec<(i64, Option<String>)> {
    let mut v = expected.to_vec();
    v.sort_by_key(|x| x.0);
    v
}
async fn exact_ready_pair_pg(
    tx: &mut Transaction<'_, Postgres>,
    expected: &[(i64, Option<String>)],
    p: &ActivationFenceProvenance,
) -> Result<bool> {
    let rows:Vec<(i64,String,String,String,String,i64,Option<String>)>=sqlx::query_as("SELECT id,workspace,repo,commit_oid,kind,format_version,manifest FROM artifact_jobs WHERE id IN($1,$2) AND state='ready' FOR UPDATE").bind(expected[0].0).bind(expected[1].0).fetch_all(&mut **tx).await?;
    if rows.len() != 2 {
        return Ok(false);
    }
    let mut kinds = Vec::new();
    let mut format = None;
    for (id, w, r, c, k, v, m) in rows {
        let wanted = expected.iter().find(|x| x.0 == id).unwrap();
        if w != p.workspace
            || r != p.repo
            || c != p.target
            || m != wanted.1
            || m.as_deref().is_none_or(|x| x.trim().is_empty())
        {
            return Ok(false);
        }
        if format.is_some_and(|x| x != v) {
            return Ok(false);
        }
        format = Some(v);
        kinds.push(k);
    }
    kinds.sort();
    Ok(kinds == ["full_history", "head"])
}
async fn recover_pg(
    pool: &PgPool,
    p: &ActivationFenceProvenance,
) -> Result<Option<ReadyPublicationFence>> {
    let op = p.operation_id();
    let row:Option<(String,i64)>=sqlx::query_as("SELECT token,generation FROM ready_publication_fences WHERE operation_id=$1 AND workspace=$2 AND repo=$3 AND branch=$4 AND target=$5 AND attempt_id=$6 AND state='activation_unknown'").bind(&op).bind(&p.workspace).bind(&p.repo).bind(&p.branch).bind(&p.target).bind(&p.attempt_id).fetch_optional(pool).await?;
    let Some((token, generation)) = row else {
        return Ok(None);
    };
    let expected:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=$1 AND generation=$2 ORDER BY artifact_id").bind(&token).bind(generation).fetch_all(pool).await?;
    if expected.len() != 2 {
        bail!("activation recovery fence is not an exact pair")
    }
    Ok(Some(ReadyPublicationFence::new(
        token,
        generation as u64,
        op,
        p.clone(),
        expected,
    )))
}
async fn unknown_page_pg(
    pool: &PgPool,
    after: Option<u64>,
    limit: usize,
) -> Result<UnknownActivationFencePage> {
    if !(1..=128).contains(&limit) || after.unwrap_or(0) > i64::MAX as u64 {
        bail!("unknown activation fence page is invalid")
    };
    let rows:Vec<(String,i64,String,String,String,String,String,String)>=sqlx::query_as("SELECT token,generation,operation_id,workspace,repo,branch,target,attempt_id FROM ready_publication_fences WHERE state='activation_unknown' AND generation>$1 ORDER BY generation LIMIT $2").bind(after.unwrap_or(0) as i64).bind(limit as i64).fetch_all(pool).await?;
    let mut fences = Vec::new();
    for (token, generation, op, w, r, b, target, attempt) in rows {
        let p = ActivationFenceProvenance {
            workspace: w,
            repo: r,
            branch: b,
            target,
            attempt_id: attempt,
        };
        if p.operation_id() != op {
            bail!("unknown activation fence provenance is invalid")
        };
        let expected:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=$1 AND generation=$2 ORDER BY artifact_id").bind(&token).bind(generation).fetch_all(pool).await?;
        if expected.len() != 2 {
            bail!("unknown activation fence is not an exact pair")
        };
        fences.push(ReadyPublicationFence::new(
            token,
            generation as u64,
            op,
            p,
            expected,
        ));
    }
    let next = (fences.len() == limit).then(|| fences.last().unwrap().parts().1);
    Ok(UnknownActivationFencePage {
        fences,
        next_generation: next,
    })
}
#[async_trait]
impl GcDeleteFence for PostgresGcDeleteFence {
    async fn release(mut self: Box<Self>) -> Result<()> {
        if let Some(tx) = self.0.take() {
            tx.commit().await?;
        }
        Ok(())
    }
}

async fn preflight_postgres_schema(tx: &mut Transaction<'_, Postgres>, version: i64) -> Result<()> {
    if version < 2 {
        return Ok(());
    }
    let fence_tables:i64=sqlx::query_scalar("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r' AND c.relname IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')").fetch_one(&mut **tx).await?;
    if version == 6 || fence_tables == 3 {
        let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='ready_publication_fence_sequence' AND column_name IN('id','generation')) OR (table_name='ready_publication_fences' AND column_name IN('token','generation','operation_id','workspace','repo','branch','target','attempt_id','expires_at','state')) OR (table_name='ready_publication_fence_members' AND column_name IN('token','generation','artifact_id','manifest')))").fetch_one(&mut **tx).await?;
        let index:i64=sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname='ready_publication_fences_recovery' AND indexdef LIKE '%(state, generation, token)%'").fetch_one(&mut **tx).await?;
        if fence_tables != 3 || columns != 16 || index != 1 {
            bail!("postgres v6 Ready fence schema differs from its marker")
        }
    } else if version == 5 && fence_tables != 0 {
        bail!("postgres v5 marker contains a partial v6 Ready fence schema")
    }
    let tables:i64=sqlx::query_scalar("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r' AND c.relname IN('artifact_scheduler_schema','artifact_jobs','branch_observations','artifact_observations','artifact_consumers','artifact_transport_leases','artifact_gc_sweep','artifact_base_retention','scheduler_state')").fetch_one(&mut **tx).await?;
    if tables != if version == 2 { 7 } else { 9 } {
        bail!("postgres artifact scheduler table inventory does not match schema marker")
    }
    let additions:i64=sqlx::query_scalar("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r' AND c.relname IN('artifact_gc_sweep','artifact_base_retention')").fetch_one(&mut **tx).await?;
    if version == 2 {
        if additions != 0 {
            bail!("postgres v2 scheduler contains unversioned v3 additions")
        }
        return Ok(());
    }
    let base_constraints:i64=sqlx::query_scalar("SELECT count(*) FROM pg_constraint c WHERE c.conrelid='artifact_base_retention'::regclass AND ((c.conname='artifact_base_retention_pkey' AND c.contype='p' AND pg_get_constraintdef(c.oid)='PRIMARY KEY (artifact_id)') OR (c.conname='artifact_base_retention_artifact' AND c.contype='f' AND c.confrelid='artifact_jobs'::regclass AND c.confdeltype='c' AND pg_get_constraintdef(c.oid)='FOREIGN KEY (artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE') OR (c.conname='artifact_base_retention_ranks' AND c.contype='c' AND pg_get_constraintdef(c.oid) ILIKE '%head_rank%' AND pg_get_constraintdef(c.oid) ILIKE '%pair_rank%' AND pg_get_constraintdef(c.oid) LIKE '%8%'))").fetch_one(&mut **tx).await?;
    let base_constraint_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_constraint WHERE conrelid='artifact_base_retention'::regclass",
    )
    .fetch_one(&mut **tx)
    .await?;
    let base_indexes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_index WHERE indrelid='artifact_base_retention'::regclass",
    )
    .fetch_one(&mut **tx)
    .await?;
    let base_scope:i64=sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname='artifact_base_retention_scope' AND indexdef LIKE '%(workspace, repo, format_version)%'").fetch_one(&mut **tx).await?;
    let gc_constraints:i64=sqlx::query_scalar("SELECT count(*) FROM pg_constraint c WHERE c.conrelid='artifact_gc_sweep'::regclass AND ((c.conname='artifact_gc_sweep_pkey' AND c.contype='p' AND pg_get_constraintdef(c.oid)='PRIMARY KEY (id)') OR (c.conname='artifact_gc_sweep_singleton' AND c.contype='c' AND pg_get_constraintdef(c.oid)='CHECK ((id = 1))'))").fetch_one(&mut **tx).await?;
    let gc_constraint_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_constraint WHERE conrelid='artifact_gc_sweep'::regclass",
    )
    .fetch_one(&mut **tx)
    .await?;
    if additions != 2
        || base_constraints != 3
        || base_constraint_count != 3
        || base_indexes != 2
        || base_scope != 1
        || gc_constraints != 2
        || gc_constraint_count != 2
    {
        bail!("postgres retention/GC schema differs from its v3/v4 marker")
    }
    Ok(())
}

impl PostgresArtifactScheduler {
    pub async fn from_pool(
        pool: PgPool,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        validate_limits(&limits)?;
        let verifier_id = verifier.identity().trim();
        if verifier_id.is_empty() {
            bail!("completion verifier identity is empty")
        }
        let mut migration = pool.begin().await?;
        // PostgreSQL's concurrent CREATE TABLE IF NOT EXISTS can still race on
        // the implicit composite type. Serialize only startup migrations; the
        // lock is transaction-scoped and never participates in runtime work.
        sqlx::query("SELECT pg_advisory_xact_lock(731904219)")
            .execute(&mut *migration)
            .await?;
        let marker_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass(current_schema()||'.artifact_scheduler_schema') IS NOT NULL",
        )
        .fetch_one(&mut *migration)
        .await?;
        let version: i64 = if marker_exists {
            let version: i64 = sqlx::query_scalar(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1 FOR UPDATE",
            )
            .fetch_one(&mut *migration)
            .await?;
            if version > SCHEMA_VERSION {
                bail!("artifact scheduler database is newer than this binary")
            }
            if ![1, 2, 3, 4, 5, SCHEMA_VERSION].contains(&version) {
                bail!("unsupported postgres artifact scheduler schema {version}")
            }
            preflight_postgres_schema(&mut migration, version).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut *migration).await?;
            version
        } else {
            let partial:i64=sqlx::query_scalar("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r' AND c.relname IN('artifact_jobs','branch_observations','artifact_observations','artifact_consumers','artifact_transport_leases','artifact_gc_sweep','artifact_base_retention','scheduler_state')").fetch_one(&mut *migration).await?;
            if partial != 0 {
                bail!("unmarked partial postgres artifact scheduler schema")
            }
            sqlx::raw_sql(SCHEMA).execute(&mut *migration).await?;
            sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
                .execute(&mut *migration)
                .await?;
            3
        };
        if version > SCHEMA_VERSION {
            bail!("artifact scheduler database is newer than this binary")
        }
        if ![1, 2, 3, 4, 5, SCHEMA_VERSION].contains(&version) {
            bail!("unsupported postgres artifact scheduler schema {version}")
        }
        let missing_columns: i64 = sqlx::query_scalar(
            "WITH expected(table_name,column_name) AS (VALUES
              ('artifact_scheduler_schema','id'),('artifact_scheduler_schema','version'),
              ('artifact_jobs','id'),('artifact_jobs','workspace'),('artifact_jobs','repo'),
              ('artifact_jobs','commit_oid'),('artifact_jobs','kind'),('artifact_jobs','format_version'),
              ('artifact_jobs','state'),('artifact_jobs','owner'),('artifact_jobs','heartbeat_at'),
              ('artifact_jobs','lease_expires_at'),('artifact_jobs','lease_generation'),
              ('artifact_jobs','claim_attempts'),('artifact_jobs','retry_count'),
              ('artifact_jobs','manifest'),('artifact_jobs','error'),('artifact_jobs','failure_class'),
              ('artifact_jobs','created_at'),('artifact_jobs','updated_at'),
              ('branch_observations','workspace'),('branch_observations','repo'),
              ('branch_observations','branch'),('branch_observations','generation'),
              ('branch_observations','desired_commit'),('branch_observations','updated_at'),
              ('artifact_observations','workspace'),('artifact_observations','repo'),
              ('artifact_observations','branch'),('artifact_observations','kind'),
              ('artifact_observations','desired_commit'),('artifact_observations','desired_artifact_id'),
              ('artifact_observations','desired_generation'),
              ('artifact_observations','published_artifact_id'),
              ('artifact_observations','format_version'),('artifact_observations','observed_at'),
              ('artifact_consumers','artifact_id'),('artifact_consumers','consumer_id'),
              ('artifact_consumers','expires_at'),
              ('artifact_transport_leases','root_hash'),('artifact_transport_leases','session_id'),
              ('artifact_transport_leases','workspace'),('artifact_transport_leases','repo'),
              ('artifact_transport_leases','expires_at'),('scheduler_state','id'),
              ('scheduler_state','fairness_cursor'),('scheduler_state','workspace_cursor'),
              ('scheduler_state','config_fingerprint'))
             SELECT count(*) FROM expected e LEFT JOIN information_schema.columns c
               ON c.table_schema=current_schema() AND c.table_name=e.table_name
              AND c.column_name=e.column_name WHERE c.column_name IS NULL",
        )
        .fetch_one(&mut *migration)
        .await?;
        if missing_columns != 0 {
            bail!("postgres artifact scheduler schema is missing required columns")
        }
        let invalid_column_shape: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.columns c
             WHERE c.table_schema=current_schema() AND c.table_name IN(
               'artifact_scheduler_schema','artifact_jobs','branch_observations',
               'artifact_observations','artifact_consumers','artifact_transport_leases','scheduler_state') AND (
               c.data_type <> CASE
                 WHEN c.table_name IN('artifact_scheduler_schema','scheduler_state')
                      AND c.column_name='id' THEN 'smallint'
                 WHEN (c.table_name='artifact_scheduler_schema' AND c.column_name='version')
                   OR (c.table_name='artifact_jobs' AND c.column_name IN(
                     'id','format_version','heartbeat_at','lease_expires_at','lease_generation',
                     'claim_attempts','retry_count','created_at','updated_at'))
                   OR (c.table_name='branch_observations' AND c.column_name IN('generation','updated_at'))
                   OR (c.table_name='artifact_observations' AND c.column_name IN(
                     'desired_artifact_id','desired_generation','published_artifact_id',
                     'format_version','observed_at'))
                   OR (c.table_name='artifact_consumers' AND c.column_name IN('artifact_id','expires_at'))
                   OR (c.table_name='artifact_transport_leases' AND c.column_name='expires_at')
                   OR (c.table_name='scheduler_state' AND c.column_name='fairness_cursor')
                   THEN 'bigint' ELSE 'text' END
               OR c.is_nullable <> CASE
                 WHEN c.table_name='artifact_jobs' AND c.column_name IN(
                   'owner','heartbeat_at','lease_expires_at','manifest','error','failure_class')
                   THEN 'YES'
                 WHEN c.table_name='artifact_observations' AND c.column_name='published_artifact_id'
                   THEN 'YES' ELSE 'NO' END
               OR (c.table_name='artifact_jobs' AND c.column_name='id'
                   AND c.column_default IS DISTINCT FROM $d$nextval('artifact_jobs_id_seq'::regclass)$d$)
               OR (c.table_name='artifact_jobs' AND c.column_name IN(
                     'lease_generation','claim_attempts','retry_count')
                   AND c.column_default IS DISTINCT FROM '0')
               OR (c.table_name='scheduler_state' AND c.column_name IN(
                     'workspace_cursor','config_fingerprint')
                   AND c.column_default IS DISTINCT FROM $d$''::text$d$)
               OR (NOT (
                     (c.table_name='artifact_jobs' AND c.column_name IN(
                       'id','lease_generation','claim_attempts','retry_count'))
                     OR (c.table_name='scheduler_state' AND c.column_name IN(
                       'workspace_cursor','config_fingerprint')))
                   AND c.column_default IS NOT NULL))",
        )
        .fetch_one(&mut *migration)
        .await?;
        let scheduler_column_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name IN(
               'artifact_scheduler_schema','artifact_jobs','branch_observations',
               'artifact_observations','artifact_consumers','artifact_transport_leases','scheduler_state')",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_column_shape != 0 || scheduler_column_count != 48 {
            bail!("postgres artifact scheduler column shape differs from schema version")
        }
        let required_constraints: i64 = sqlx::query_scalar(
            "WITH expected(table_name,constraint_name,constraint_type) AS (VALUES
              ('artifact_scheduler_schema','artifact_scheduler_schema_pkey','p'),
              ('artifact_scheduler_schema','artifact_scheduler_schema_singleton','c'),
              ('artifact_jobs','artifact_jobs_pkey','p'),
              ('artifact_jobs','artifact_jobs_identity','u'),
              ('artifact_jobs','artifact_jobs_format','c'),
              ('artifact_jobs','artifact_jobs_state','c'),
              ('artifact_jobs','artifact_jobs_kind','c'),
              ('artifact_jobs','artifact_jobs_lease_generation','c'),
              ('artifact_jobs','artifact_jobs_claim_attempts','c'),
              ('artifact_jobs','artifact_jobs_retry_count','c'),
              ('artifact_jobs','artifact_jobs_failure_class','c'),
              ('branch_observations','branch_observations_pkey','p'),
              ('branch_observations','branch_observations_generation','c'),
              ('artifact_observations','artifact_observations_pkey','p'),
              ('artifact_observations','artifact_observations_generation','c'),
              ('artifact_observations','artifact_observations_format','c'),
              ('artifact_consumers','artifact_consumers_pkey','p'),
              ('artifact_transport_leases','artifact_transport_leases_pkey','p'),
              ('scheduler_state','scheduler_state_pkey','p'),
              ('scheduler_state','scheduler_state_singleton','c'),
              ('scheduler_state','scheduler_state_fairness','c'))
             SELECT count(*) FROM expected e
             JOIN pg_class r ON r.relname=e.table_name
             JOIN pg_namespace n ON n.oid=r.relnamespace AND n.nspname=current_schema()
             JOIN pg_constraint c ON c.conrelid=r.oid AND c.conname=e.constraint_name
                                  AND c.contype::text=e.constraint_type",
        )
        .fetch_one(&mut *migration)
        .await?;
        if required_constraints != 21 {
            bail!("postgres artifact scheduler schema is missing required constraints")
        }
        let invalid_constraint_definitions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
             JOIN pg_namespace n ON n.oid=r.relnamespace
             WHERE n.nspname=current_schema() AND (
               (c.conname='artifact_scheduler_schema_pkey' AND pg_get_constraintdef(c.oid)<>'PRIMARY KEY (id)')
               OR (c.conname='artifact_scheduler_schema_singleton' AND pg_get_constraintdef(c.oid) NOT ILIKE '%id%1%')
               OR (c.conname='artifact_jobs_pkey' AND pg_get_constraintdef(c.oid)<>'PRIMARY KEY (id)')
               OR (c.conname='artifact_jobs_identity' AND pg_get_constraintdef(c.oid)<>'UNIQUE (workspace, repo, commit_oid, kind, format_version)')
               OR (c.conname='artifact_jobs_format' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%format_version%' AND pg_get_constraintdef(c.oid) LIKE '%4294967295%'))
               OR (c.conname='artifact_jobs_state' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%state%' AND pg_get_constraintdef(c.oid) ILIKE '%queued%' AND pg_get_constraintdef(c.oid) ILIKE '%running%' AND pg_get_constraintdef(c.oid) ILIKE '%ready%' AND pg_get_constraintdef(c.oid) ILIKE '%failed%'))
               OR (c.conname='artifact_jobs_kind' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%kind%' AND pg_get_constraintdef(c.oid) ILIKE '%head%' AND pg_get_constraintdef(c.oid) ILIKE '%full_history%' AND pg_get_constraintdef(c.oid) ILIKE '%files%'))
               OR (c.conname='artifact_jobs_lease_generation' AND pg_get_constraintdef(c.oid) NOT ILIKE '%lease_generation%')
               OR (c.conname='artifact_jobs_claim_attempts' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%claim_attempts%' AND pg_get_constraintdef(c.oid) LIKE '%4294967295%'))
               OR (c.conname='artifact_jobs_retry_count' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%retry_count%' AND pg_get_constraintdef(c.oid) LIKE '%4294967295%'))
               OR (c.conname='artifact_jobs_failure_class' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%failure_class%' AND pg_get_constraintdef(c.oid) ILIKE '%retryable%' AND pg_get_constraintdef(c.oid) ILIKE '%permanent%' AND pg_get_constraintdef(c.oid) ILIKE '%dead_letter%'))
               OR (c.conname='branch_observations_pkey' AND pg_get_constraintdef(c.oid)<>'PRIMARY KEY (workspace, repo, branch)')
               OR (c.conname='branch_observations_generation' AND pg_get_constraintdef(c.oid) NOT ILIKE '%generation%')
               OR (c.conname='artifact_observations_pkey' AND pg_get_constraintdef(c.oid)<>'PRIMARY KEY (workspace, repo, branch, kind)')
               OR (c.conname='artifact_observations_generation' AND pg_get_constraintdef(c.oid) NOT ILIKE '%desired_generation%')
               OR (c.conname='artifact_observations_format' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%format_version%' AND pg_get_constraintdef(c.oid) LIKE '%4294967295%'))
               OR (c.conname='artifact_consumers_pkey' AND pg_get_constraintdef(c.oid)<>'PRIMARY KEY (artifact_id, consumer_id)')
               OR (c.conname='artifact_transport_leases_pkey' AND pg_get_constraintdef(c.oid)<>'PRIMARY KEY (root_hash, session_id)')
               OR (c.conname='scheduler_state_pkey' AND pg_get_constraintdef(c.oid)<>'PRIMARY KEY (id)')
               OR (c.conname='scheduler_state_singleton' AND pg_get_constraintdef(c.oid) NOT ILIKE '%id%1%')
               OR (c.conname='scheduler_state_fairness' AND NOT (pg_get_constraintdef(c.oid) ILIKE '%fairness_cursor%' AND pg_get_constraintdef(c.oid) LIKE '%3%')))",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_constraint_definitions != 0 {
            bail!("postgres artifact scheduler constraint definitions differ from schema version")
        }
        let exact_constraint_definitions: i64 = sqlx::query_scalar(
            "WITH expected(constraint_name,definition) AS (VALUES
              ('artifact_scheduler_schema_pkey',$d$PRIMARY KEY (id)$d$),
              ('artifact_scheduler_schema_singleton',$d$CHECK ((id = 1))$d$),
              ('artifact_jobs_pkey',$d$PRIMARY KEY (id)$d$),
              ('artifact_jobs_identity',$d$UNIQUE (workspace, repo, commit_oid, kind, format_version)$d$),
              ('artifact_jobs_format',$d$CHECK (((format_version >= 1) AND (format_version <= '4294967295'::bigint)))$d$),
              ('artifact_jobs_state',$d$CHECK ((state = ANY (ARRAY['queued'::text, 'running'::text, 'ready'::text, 'failed'::text])))$d$),
              ('artifact_jobs_kind',$d$CHECK ((kind = ANY (ARRAY['head'::text, 'full_history'::text, 'files'::text])))$d$),
              ('artifact_jobs_lease_generation',$d$CHECK (((lease_generation >= 0) AND (lease_generation <= '9223372036854775807'::bigint)))$d$),
              ('artifact_jobs_claim_attempts',$d$CHECK (((claim_attempts >= 0) AND (claim_attempts <= '4294967295'::bigint)))$d$),
              ('artifact_jobs_retry_count',$d$CHECK (((retry_count >= 0) AND (retry_count <= '4294967295'::bigint)))$d$),
              ('artifact_jobs_failure_class',$d$CHECK (((failure_class IS NULL) OR (failure_class = ANY (ARRAY['retryable'::text, 'permanent'::text, 'dead_letter'::text]))))$d$),
              ('branch_observations_pkey',$d$PRIMARY KEY (workspace, repo, branch)$d$),
              ('branch_observations_generation',$d$CHECK ((generation >= 1))$d$),
              ('artifact_observations_pkey',$d$PRIMARY KEY (workspace, repo, branch, kind)$d$),
              ('artifact_observations_generation',$d$CHECK ((desired_generation >= 1))$d$),
              ('artifact_observations_format',$d$CHECK (((format_version >= 1) AND (format_version <= '4294967295'::bigint)))$d$),
              ('artifact_consumers_pkey',$d$PRIMARY KEY (artifact_id, consumer_id)$d$),
              ('artifact_transport_leases_pkey',$d$PRIMARY KEY (root_hash, session_id)$d$),
              ('scheduler_state_pkey',$d$PRIMARY KEY (id)$d$),
              ('scheduler_state_singleton',$d$CHECK ((id = 1))$d$),
              ('scheduler_state_fairness',$d$CHECK (((fairness_cursor >= 0) AND (fairness_cursor <= 3)))$d$))
             SELECT count(*) FROM expected e JOIN pg_constraint c
               ON c.conname=e.constraint_name AND pg_get_constraintdef(c.oid)=e.definition
             JOIN pg_namespace n ON n.oid=c.connamespace AND n.nspname=current_schema()",
        )
        .fetch_one(&mut *migration)
        .await?;
        if exact_constraint_definitions != 21 {
            bail!(
                "postgres artifact scheduler exact constraint definitions differ from schema version ({exact_constraint_definitions}/21 matched)"
            )
        }
        let required_indexes: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname IN(
              'artifact_jobs_claim','artifact_jobs_lease','artifact_observations_desired',
              'artifact_observations_published','artifact_consumers_expiry',
              'artifact_transport_leases_expiry')",
        )
        .fetch_one(&mut *migration)
        .await?;
        if required_indexes != 6 {
            bail!("postgres artifact scheduler schema is missing required indexes")
        }
        let invalid_index_definitions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND (
               (indexname='artifact_jobs_claim' AND indexdef NOT LIKE '%(state, kind, created_at, id)%')
               OR (indexname='artifact_jobs_lease' AND indexdef NOT LIKE '%(state, lease_expires_at)%')
               OR (indexname='artifact_observations_desired' AND indexdef NOT LIKE '%(desired_artifact_id)%')
               OR (indexname='artifact_observations_published' AND indexdef NOT LIKE '%(published_artifact_id)%')
               OR (indexname='artifact_consumers_expiry' AND indexdef NOT LIKE '%(expires_at)%')
               OR (indexname='artifact_transport_leases_expiry' AND indexdef NOT LIKE '%(expires_at)%'))",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_index_definitions != 0 {
            bail!("postgres artifact scheduler index definitions differ from schema version")
        }
        let exact_index_definitions: i64 = sqlx::query_scalar(
            "WITH expected(index_name,keys) AS (VALUES
              ('artifact_jobs_claim',ARRAY['state','kind','created_at','id']::text[]),
              ('artifact_jobs_lease',ARRAY['state','lease_expires_at']::text[]),
              ('artifact_observations_desired',ARRAY['desired_artifact_id']::text[]),
              ('artifact_observations_published',ARRAY['published_artifact_id']::text[]),
              ('artifact_consumers_expiry',ARRAY['expires_at']::text[]),
              ('artifact_transport_leases_expiry',ARRAY['expires_at']::text[]))
             SELECT count(*) FROM expected e JOIN pg_class i ON i.relname=e.index_name
             JOIN pg_namespace n ON n.oid=i.relnamespace AND n.nspname=current_schema()
             JOIN pg_index x ON x.indexrelid=i.oid
             WHERE x.indisvalid AND NOT x.indisunique AND x.indpred IS NULL
               AND x.indexprs IS NULL AND x.indnkeyatts=array_length(e.keys,1)
               AND ARRAY(SELECT pg_get_indexdef(i.oid,s,true)
                         FROM generate_series(1,x.indnkeyatts) s ORDER BY s)=e.keys",
        )
        .fetch_one(&mut *migration)
        .await?;
        if exact_index_definitions != 6 {
            bail!("postgres artifact scheduler exact index definitions differ from schema version")
        }
        let transport_index_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_index x JOIN pg_class r ON r.oid=x.indrelid
             JOIN pg_namespace n ON n.oid=r.relnamespace
             WHERE n.nspname=current_schema() AND r.relname='artifact_transport_leases'",
        )
        .fetch_one(&mut *migration)
        .await?;
        let transport_foreign_keys: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_constraint c
             JOIN pg_class owned ON owned.oid=c.conrelid
             JOIN pg_namespace n ON n.oid=owned.relnamespace
             WHERE n.nspname=current_schema() AND c.contype='f'
               AND (owned.relname='artifact_transport_leases'
                    OR c.confrelid='artifact_transport_leases'::regclass)",
        )
        .fetch_one(&mut *migration)
        .await?;
        if transport_index_count != 2 || transport_foreign_keys != 0 {
            bail!("postgres artifact transport schema has unexpected indexes or foreign keys")
        }
        let retention_columns: i64 = sqlx::query_scalar("WITH expected(column_name,data_type,is_nullable) AS (VALUES ('artifact_id','bigint','NO'),('workspace','text','NO'),('repo','text','NO'),('format_version','bigint','NO'),('head_rank','smallint','YES'),('pair_rank','smallint','YES')) SELECT count(*) FROM expected e JOIN information_schema.columns c ON c.table_schema=current_schema() AND c.table_name='artifact_base_retention' AND c.column_name=e.column_name AND c.data_type=e.data_type AND c.is_nullable=e.is_nullable WHERE c.column_default IS NULL").fetch_one(&mut *migration).await?;
        let retention_column_count: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='artifact_base_retention'").fetch_one(&mut *migration).await?;
        let retention_constraints: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_constraint c WHERE c.conrelid='artifact_base_retention'::regclass AND ((c.conname='artifact_base_retention_pkey' AND c.contype='p' AND pg_get_constraintdef(c.oid)='PRIMARY KEY (artifact_id)') OR (c.conname='artifact_base_retention_artifact' AND c.contype='f' AND c.confrelid='artifact_jobs'::regclass AND c.confdeltype='c' AND pg_get_constraintdef(c.oid)='FOREIGN KEY (artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE') OR (c.conname='artifact_base_retention_ranks' AND c.contype='c' AND pg_get_constraintdef(c.oid) ILIKE '%head_rank%' AND pg_get_constraintdef(c.oid) ILIKE '%pair_rank%' AND pg_get_constraintdef(c.oid) LIKE '%8%'))").fetch_one(&mut *migration).await?;
        let retention_constraint_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_constraint WHERE conrelid='artifact_base_retention'::regclass",
        )
        .fetch_one(&mut *migration)
        .await?;
        let retention_indexes: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_index WHERE indrelid='artifact_base_retention'::regclass",
        )
        .fetch_one(&mut *migration)
        .await?;
        let retention_scope_exact: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname='artifact_base_retention_scope' AND indexdef LIKE '%(workspace, repo, format_version)%'").fetch_one(&mut *migration).await?;
        if retention_columns != 6
            || retention_column_count != 6
            || retention_constraints != 3
            || retention_constraint_count != 3
            || retention_indexes != 2
            || retention_scope_exact != 1
        {
            bail!("postgres artifact base retention schema differs from schema version")
        }
        let invalid_retention: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact_base_retention r LEFT JOIN artifact_jobs j ON j.id=r.artifact_id WHERE j.id IS NULL OR j.workspace<>r.workspace OR j.repo<>r.repo OR j.format_version<>r.format_version OR (r.head_rank IS NULL AND r.pair_rank IS NULL) OR (r.head_rank IS NOT NULL AND r.head_rank NOT BETWEEN 1 AND 8) OR (r.pair_rank IS NOT NULL AND r.pair_rank NOT BETWEEN 1 AND 8)").fetch_one(&mut *migration).await?;
        if invalid_retention != 0 {
            bail!("postgres artifact scheduler contains invalid base retention")
        }
        let gc_columns: i64 = sqlx::query_scalar("WITH expected(column_name,data_type,is_nullable) AS (VALUES ('id','smallint','NO'),('owner','text','NO'),('expires_at','bigint','NO')) SELECT count(*) FROM expected e JOIN information_schema.columns c ON c.table_schema=current_schema() AND c.table_name='artifact_gc_sweep' AND c.column_name=e.column_name AND c.data_type=e.data_type AND c.is_nullable=e.is_nullable WHERE c.column_default IS NULL").fetch_one(&mut *migration).await?;
        let gc_column_count: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='artifact_gc_sweep'").fetch_one(&mut *migration).await?;
        let gc_constraints: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_constraint c WHERE c.conrelid='artifact_gc_sweep'::regclass AND ((c.conname='artifact_gc_sweep_pkey' AND c.contype='p' AND pg_get_constraintdef(c.oid)='PRIMARY KEY (id)') OR (c.conname='artifact_gc_sweep_singleton' AND c.contype='c' AND pg_get_constraintdef(c.oid)='CHECK ((id = 1))'))").fetch_one(&mut *migration).await?;
        let gc_constraint_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_constraint WHERE conrelid='artifact_gc_sweep'::regclass",
        )
        .fetch_one(&mut *migration)
        .await?;
        let gc_index_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_index WHERE indrelid='artifact_gc_sweep'::regclass",
        )
        .fetch_one(&mut *migration)
        .await?;
        let invalid_gc: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact_gc_sweep WHERE id<>1 OR length(trim(owner))=0 OR length(owner)>200").fetch_one(&mut *migration).await?;
        if gc_columns != 3
            || gc_column_count != 3
            || gc_constraints != 2
            || gc_constraint_count != 2
            || gc_index_count != 1
            || invalid_gc != 0
        {
            bail!("postgres artifact GC sweep schema differs from schema version")
        }
        let invalid_jobs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE
               state NOT IN('queued','running','ready','failed')
               OR state IS NULL OR kind IS NULL OR format_version IS NULL
               OR kind NOT IN('head','full_history','files')
               OR format_version NOT BETWEEN 1 AND 4294967295
               OR lease_generation<0 OR claim_attempts NOT BETWEEN 0 AND 4294967295
               OR retry_count NOT BETWEEN 0 AND 4294967295
               OR (failure_class IS NOT NULL AND failure_class NOT IN('retryable','permanent','dead_letter'))
               OR (state='running' AND (owner IS NULL OR length(trim(owner))=0
                                        OR lease_expires_at IS NULL))
               OR (state='ready' AND (manifest IS NULL OR length(trim(manifest))=0))",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_jobs != 0 {
            bail!("postgres artifact scheduler contains invalid artifact jobs")
        }
        let invalid_observations: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_observations a
             LEFT JOIN artifact_jobs d ON d.id=a.desired_artifact_id
               AND d.workspace=a.workspace AND d.repo=a.repo AND d.kind=a.kind
               AND d.commit_oid=a.desired_commit AND d.format_version=a.format_version
               AND d.format_version BETWEEN 1 AND 4294967295
             LEFT JOIN artifact_jobs p ON p.id=a.published_artifact_id
               AND p.workspace=a.workspace AND p.repo=a.repo AND p.kind=a.kind
               AND p.format_version=a.format_version AND p.state='ready'
               AND p.manifest IS NOT NULL AND length(trim(p.manifest))>0
             WHERE a.desired_generation IS NULL OR a.format_version IS NULL
                OR a.desired_generation<1 OR a.format_version NOT BETWEEN 1 AND 4294967295
                OR d.id IS NULL OR (a.published_artifact_id IS NOT NULL AND p.id IS NULL)",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_observations != 0 {
            bail!("postgres artifact scheduler contains invalid artifact observations")
        }
        let invalid_branches: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM branch_observations WHERE generation IS NULL OR generation<1",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_branches != 0 {
            bail!("postgres artifact scheduler contains invalid branch observations")
        }
        let invalid_transport: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_transport_leases
             WHERE root_hash !~ '^[0-9a-f]{64}$' OR session_id !~ '^[0-9a-f]{64}$'
                OR length(trim(workspace))=0 OR length(trim(repo))=0",
        )
        .fetch_one(&mut *migration)
        .await?;
        let conflicting_sessions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT session_id FROM artifact_transport_leases
             GROUP BY session_id HAVING count(DISTINCT (workspace,repo))>1) conflicts",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_transport != 0 || conflicting_sessions != 0 {
            bail!("postgres artifact scheduler contains invalid transport leases")
        }
        let invalid_control: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM scheduler_state
             WHERE id<>1 OR fairness_cursor NOT BETWEEN 0 AND 3",
        )
        .fetch_one(&mut *migration)
        .await?;
        if invalid_control != 0 {
            bail!("postgres artifact scheduler contains invalid control state")
        }
        if version < 3 {
            let scopes: Vec<(String,String,i64)> = sqlx::query_as("SELECT DISTINCT workspace,repo,format_version FROM artifact_jobs WHERE state='ready' AND kind IN('head','full_history')").fetch_all(&mut *migration).await?;
            for (w, r, v) in scopes {
                refresh_base_retention(&mut migration, &w, &r, v).await?;
            }
        }
        if version < SCHEMA_VERSION {
            sqlx::query(
                "UPDATE artifact_scheduler_schema SET version=$1 WHERE id=1 AND version=$2",
            )
            .bind(SCHEMA_VERSION)
            .bind(version)
            .execute(&mut *migration)
            .await?;
        }
        let fingerprint = scheduler_fingerprint(&limits, verifier_id);
        let stored: String = sqlx::query_scalar(
            "SELECT config_fingerprint FROM scheduler_state WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *migration)
        .await?;
        if stored.is_empty() {
            let existing_state: i64 = sqlx::query_scalar(
                "SELECT
                   (SELECT count(*) FROM artifact_jobs)
                 + (SELECT count(*) FROM branch_observations)
                 + (SELECT count(*) FROM artifact_observations)
                 + (SELECT count(*) FROM artifact_consumers)
                 + (SELECT count(*) FROM artifact_transport_leases)",
            )
            .fetch_one(&mut *migration)
            .await?;
            if existing_state != 0 {
                bail!("cannot establish scheduler verifier/config fingerprint over existing state")
            }
            let adopted = sqlx::query(
                "UPDATE scheduler_state SET config_fingerprint=$1
                 WHERE id=1 AND config_fingerprint=''",
            )
            .bind(&fingerprint)
            .execute(&mut *migration)
            .await?
            .rows_affected();
            if adopted != 1 {
                bail!("scheduler configuration fingerprint CAS failed")
            }
        } else if stored != fingerprint {
            bail!("scheduler running-limit configuration differs from existing fleet")
        }
        migration.commit().await?;
        let completion_sealer = Arc::new(CompletionSealAuthority::new(verifier.identity())?);
        Ok(Self {
            pool,
            limits,
            verifier,
            completion_sealer,
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn controlled(&self) -> Result<(Transaction<'_, Postgres>, i64)> {
        let mut tx = self.pool.begin().await?;
        // All cap/fairness/admission decisions serialize on this tiny row. The
        // jobs themselves remain normalized and heartbeat/settlement bypass it.
        sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        Ok((tx, now))
    }

    async fn get_tx(tx: &mut Transaction<'_, Postgres>, id: i64) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,
                    lease_expires_at,lease_generation,claim_attempts,retry_count,
                    manifest,error,failure_class FROM artifact_jobs WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;
        row.map(row_record).transpose()
    }

    async fn get_key_tx(
        tx: &mut Transaction<'_, Postgres>,
        key: &ArtifactKey,
    ) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,
                    lease_expires_at,lease_generation,claim_attempts,retry_count,
                    manifest,error,failure_class FROM artifact_jobs
             WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND kind=$4 AND format_version=$5",
        )
        .bind(&key.workspace)
        .bind(&key.repo)
        .bind(&key.commit)
        .bind(key.kind.as_str())
        .bind(key.format_version as i64)
        .fetch_optional(&mut **tx)
        .await?;
        row.map(row_record).transpose()
    }

    fn running_limit(&self, kind: ArtifactKind) -> usize {
        match kind {
            ArtifactKind::Head => self.limits.head_running,
            ArtifactKind::FullHistory => self.limits.full_history_running,
            ArtifactKind::Files => self.limits.files_running,
        }
    }

    fn backlog_limit(&self, kind: ArtifactKind) -> usize {
        match kind {
            ArtifactKind::Head => self.limits.head_backlog,
            ArtifactKind::FullHistory => self.limits.full_history_backlog,
            ArtifactKind::Files => self.limits.files_backlog,
        }
    }

    async fn preflight_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        workspace: &str,
        repo: &str,
        commit: &str,
        kinds: &[ArtifactKind],
        format_version: u32,
    ) -> Result<()> {
        let mut additions = [0usize; 3];
        for &kind in kinds {
            let key = ArtifactKey {
                workspace: workspace.into(),
                repo: repo.into(),
                commit: commit.into(),
                kind,
                format_version,
            };
            if Self::get_key_tx(tx, &key).await?.is_none() {
                additions[kind_index(kind)] += 1;
            }
        }
        let add_total: usize = additions.iter().sum();
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')",
        )
        .fetch_one(&mut **tx)
        .await?;
        let workspace_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE state IN('queued','running') AND workspace=$1",
        )
        .bind(workspace)
        .fetch_one(&mut **tx)
        .await?;
        let active_expensive: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE state IN('queued','running') AND kind IN('full_history','files')",
        )
        .fetch_one(&mut **tx)
        .await?;
        let expensive_add = additions[kind_index(ArtifactKind::FullHistory)]
            + additions[kind_index(ArtifactKind::Files)];
        if total as usize + add_total > self.limits.total_backlog
            || workspace_count as usize + add_total > self.limits.workspace_backlog
            || active_expensive as usize + expensive_add
                > self
                    .limits
                    .total_backlog
                    .saturating_sub(self.limits.head_reserved)
        {
            bail!("artifact queue capacity exhausted for atomic observation batch")
        }
        for kind in [
            ArtifactKind::Head,
            ArtifactKind::FullHistory,
            ArtifactKind::Files,
        ] {
            let add = additions[kind_index(kind)];
            if add > 0 {
                self.preflight_capacity(tx, kind, workspace, add).await?;
            }
        }
        Ok(())
    }

    async fn preflight_capacity(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        kind: ArtifactKind,
        workspace: &str,
        add: usize,
    ) -> Result<()> {
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')",
        )
        .fetch_one(&mut **tx)
        .await?;
        let workspace_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE state IN('queued','running') AND workspace=$1",
        )
        .bind(workspace)
        .fetch_one(&mut **tx)
        .await?;
        let per_kind: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE state IN('queued','running') AND kind=$1",
        )
        .bind(kind.as_str())
        .fetch_one(&mut **tx)
        .await?;
        let active_expensive: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE state IN('queued','running') AND kind IN('full_history','files')",
        )
        .fetch_one(&mut **tx)
        .await?;
        let reserve_exhausted = kind.expensive()
            && active_expensive as usize + add
                > self
                    .limits
                    .total_backlog
                    .saturating_sub(self.limits.head_reserved);
        if total as usize + add > self.limits.total_backlog
            || workspace_count as usize + add > self.limits.workspace_backlog
            || per_kind as usize + add > self.backlog_limit(kind)
            || reserve_exhausted
        {
            bail!("artifact queue capacity exhausted for {}", kind.as_str())
        }
        Ok(())
    }

    async fn schedule_unchecked(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        key: &ArtifactKey,
        now: i64,
    ) -> Result<ScheduleOutcome> {
        if let Some(record) = Self::get_key_tx(tx, key).await? {
            return Ok(existing_outcome(record));
        }
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO artifact_jobs(
                workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at)
             VALUES($1,$2,$3,$4,$5,'queued',$6,$6) RETURNING id",
        )
        .bind(&key.workspace)
        .bind(&key.repo)
        .bind(&key.commit)
        .bind(key.kind.as_str())
        .bind(key.format_version as i64)
        .bind(now)
        .fetch_one(&mut **tx)
        .await?;
        Ok(ScheduleOutcome::Enqueued(id))
    }
}

fn kind_index(kind: ArtifactKind) -> usize {
    match kind {
        ArtifactKind::Head => 0,
        ArtifactKind::FullHistory => 1,
        ArtifactKind::Files => 2,
    }
}

async fn refresh_base_retention(
    tx: &mut Transaction<'_, Postgres>,
    w: &str,
    r: &str,
    v: i64,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM artifact_base_retention WHERE workspace=$1 AND repo=$2 AND format_version=$3",
    )
    .bind(w)
    .bind(r)
    .bind(v)
    .execute(&mut **tx)
    .await?;
    sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank) SELECT id,workspace,repo,format_version,rank FROM (SELECT id,workspace,repo,format_version,row_number() OVER(ORDER BY updated_at DESC,id DESC)::SMALLINT rank FROM artifact_jobs WHERE workspace=$1 AND repo=$2 AND format_version=$3 AND kind='head' AND state='ready' AND manifest IS NOT NULL AND length(trim(manifest))>0) ranked WHERE rank<=8").bind(w).bind(r).bind(v).execute(&mut **tx).await?;
    for history in [false, true] {
        let id = if history { "history_id" } else { "head_id" };
        let sql = format!(
            "INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,pair_rank) SELECT {id},workspace,repo,format_version,rank FROM (SELECT h.id head_id,f.id history_id,h.workspace,h.repo,h.format_version,row_number() OVER(ORDER BY GREATEST(h.updated_at,f.updated_at) DESC,GREATEST(h.id,f.id) DESC)::SMALLINT rank FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' AND f.state='ready' AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 WHERE h.workspace=$1 AND h.repo=$2 AND h.format_version=$3 AND h.kind='head' AND h.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0) ranked WHERE rank<=8 ON CONFLICT(artifact_id) DO UPDATE SET pair_rank=EXCLUDED.pair_rank"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(w)
            .bind(r)
            .bind(v)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
fn outcome_id(outcome: &ScheduleOutcome) -> i64 {
    match outcome {
        ScheduleOutcome::Enqueued(id)
        | ScheduleOutcome::Subscribed(id)
        | ScheduleOutcome::AlreadyReady(id)
        | ScheduleOutcome::Failed(id, _) => *id,
    }
}
fn existing_outcome(record: ArtifactRecord) -> ScheduleOutcome {
    match record.state {
        ArtifactState::Ready => ScheduleOutcome::AlreadyReady(record.id),
        ArtifactState::Failed => ScheduleOutcome::Failed(
            record.id,
            record.failure_class.unwrap_or(FailureClass::Permanent),
        ),
        ArtifactState::Queued | ArtifactState::Running => ScheduleOutcome::Subscribed(record.id),
    }
}

fn validate_gc_sweep_args(owner: &str, ttl: i64) -> Result<()> {
    if owner.trim().is_empty() || owner.len() > 200 || !(1..=600).contains(&ttl) {
        bail!("GC sweep owner or TTL is invalid")
    }
    Ok(())
}

async fn ensure_gc_unfenced_pg(tx: &mut Transaction<'_, Postgres>, now: i64) -> Result<()> {
    let fence: Option<(String, i64)> =
        sqlx::query_as("SELECT owner,expires_at FROM artifact_gc_sweep WHERE id=1 FOR UPDATE")
            .fetch_optional(&mut **tx)
            .await?;
    if fence.is_some_and(|(_, expires_at)| expires_at > now) {
        bail!("artifact publication is temporarily fenced by remote GC")
    }
    Ok(())
}

#[async_trait]
impl ArtifactSchedulerPersistence for PostgresArtifactScheduler {
    fn completion_verifier(&self) -> Arc<dyn CompletionVerifier> {
        self.verifier.clone()
    }
    fn completion_sealer(&self) -> Arc<CompletionSealAuthority> {
        self.completion_sealer.clone()
    }
    fn full_admission_recovery_protocol_supported(&self) -> bool {
        true
    }

    async fn fence_ready_publications(
        &self,
        expected: &[(i64, Option<String>)],
        provenance: &ActivationFenceProvenance,
        ttl: i64,
    ) -> Result<Option<ReadyPublicationFence>> {
        validate_fence_request(expected, provenance, ttl)?;
        let operation = provenance.operation_id();
        let token = hex::encode(rand::random::<[u8; 32]>());
        let (mut tx, now) = self.controlled().await?;
        let existing:Option<(String,i64,i64,String,String,String,String,String,String)>=sqlx::query_as("SELECT token,generation,expires_at,state,workspace,repo,branch,target,attempt_id FROM ready_publication_fences WHERE operation_id=$1 FOR UPDATE").bind(&operation).fetch_optional(&mut *tx).await?;
        if let Some((
            ref old,
            generation,
            expires,
            ref state,
            ref w,
            ref r,
            ref b,
            ref target,
            ref attempt,
        )) = existing
        {
            if w != &provenance.workspace
                || r != &provenance.repo
                || b != &provenance.branch
                || target != &provenance.target
                || attempt != &provenance.attempt_id
            {
                bail!("activation operation provenance mismatch")
            }
            if state == "held" && expires > now {
                tx.rollback().await?;
                return Ok(None);
            }
            if state == "activation_unknown" {
                let members:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=$1 AND generation=$2 ORDER BY artifact_id").bind(old).bind(generation).fetch_all(&mut *tx).await?;
                if sorted_expected(expected) != members {
                    bail!("activation recovery fence membership does not match operation")
                }
                tx.commit().await?;
                return Ok(Some(ReadyPublicationFence::new(
                    old.clone(),
                    generation as u64,
                    operation,
                    provenance.clone(),
                    expected.to_vec(),
                )));
            }
            sqlx::query("DELETE FROM ready_publication_fences WHERE token=$1 AND generation=$2")
                .bind(old)
                .bind(generation)
                .execute(&mut *tx)
                .await?;
        }
        if !exact_ready_pair_pg(&mut tx, expected, provenance).await? {
            tx.rollback().await?;
            return Ok(None);
        }
        let prior: i64 = sqlx::query_scalar(
            "SELECT generation FROM ready_publication_fence_sequence WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let generation = prior
            .checked_add(1)
            .context("Ready publication fence generation exhausted")?;
        sqlx::query("UPDATE ready_publication_fence_sequence SET generation=$1 WHERE id=1 AND generation=$2").bind(generation).bind(prior).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO ready_publication_fences(token,generation,operation_id,workspace,repo,branch,target,attempt_id,expires_at,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'held')")
          .bind(&token).bind(generation).bind(&operation).bind(&provenance.workspace).bind(&provenance.repo).bind(&provenance.branch).bind(&provenance.target).bind(&provenance.attempt_id).bind(now.saturating_add(ttl)).execute(&mut *tx).await?;
        for (id, manifest) in expected {
            sqlx::query("INSERT INTO ready_publication_fence_members(token,generation,artifact_id,manifest) VALUES($1,$2,$3,$4)").bind(&token).bind(generation).bind(id).bind(manifest).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(Some(ReadyPublicationFence::new(
            token,
            generation as u64,
            operation,
            provenance.clone(),
            expected.to_vec(),
        )))
    }
    async fn release_ready_publication_fence(&self, fence: ReadyPublicationFence) -> Result<()> {
        let (token, generation, operation, expected) = fence.parts();
        let mut tx = self.pool.begin().await?;
        let members:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=$1 AND generation=$2 ORDER BY artifact_id FOR UPDATE").bind(token).bind(generation as i64).fetch_all(&mut *tx).await?;
        if members == sorted_expected(expected) {
            sqlx::query("DELETE FROM ready_publication_fences WHERE token=$1 AND generation=$2 AND operation_id=$3").bind(token).bind(generation as i64).bind(operation).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn mark_activation_unknown(
        &self,
        fence: &ReadyPublicationFence,
        ttl: i64,
    ) -> Result<bool> {
        if !(1..=3600).contains(&ttl) {
            bail!("activation fence TTL is invalid")
        };
        let (token, generation, operation, expected) = fence.parts();
        let (mut tx, now) = self.controlled().await?;
        let members:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=$1 AND generation=$2 ORDER BY artifact_id").bind(token).bind(generation as i64).fetch_all(&mut *tx).await?;
        let changed=members==sorted_expected(expected) && sqlx::query("UPDATE ready_publication_fences SET state='activation_unknown',expires_at=$1 WHERE token=$2 AND generation=$3 AND operation_id=$4").bind(now.saturating_add(ttl)).bind(token).bind(generation as i64).bind(operation).execute(&mut *tx).await?.rows_affected()==1;
        tx.commit().await?;
        Ok(changed)
    }
    async fn recover_activation_fence(
        &self,
        p: &ActivationFenceProvenance,
    ) -> Result<Option<ReadyPublicationFence>> {
        recover_pg(&self.pool, p).await
    }
    async fn unknown_activation_fences_page(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<UnknownActivationFencePage> {
        unknown_page_pg(&self.pool, after, limit).await
    }

    async fn acquire_gc_sweep(&self, owner: &str, ttl: i64) -> Result<bool> {
        validate_gc_sweep_args(owner, ttl)?;
        let (mut tx, now) = self.controlled().await?;
        let won = sqlx::query("INSERT INTO artifact_gc_sweep(id,owner,expires_at) VALUES(1,$1,$2) ON CONFLICT(id) DO UPDATE SET owner=EXCLUDED.owner,expires_at=EXCLUDED.expires_at WHERE artifact_gc_sweep.expires_at<=$3 OR artifact_gc_sweep.owner=EXCLUDED.owner")
            .bind(owner).bind(now + ttl).bind(now).execute(&mut *tx).await?.rows_affected() == 1;
        tx.commit().await?;
        Ok(won)
    }
    async fn renew_gc_sweep(&self, owner: &str, ttl: i64) -> Result<bool> {
        validate_gc_sweep_args(owner, ttl)?;
        let (mut tx, now) = self.controlled().await?;
        let won = sqlx::query(
            "UPDATE artifact_gc_sweep SET expires_at=$1 WHERE id=1 AND owner=$2 AND expires_at>$3",
        )
        .bind(now + ttl)
        .bind(owner)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(won)
    }
    async fn release_gc_sweep(&self, owner: &str) -> Result<()> {
        validate_gc_sweep_args(owner, 1)?;
        sqlx::query("DELETE FROM artifact_gc_sweep WHERE id=1 AND owner=$1")
            .bind(owner)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn lock_gc_delete_batch(&self, owner: &str) -> Result<Box<dyn GcDeleteFence>> {
        validate_gc_sweep_args(owner, 1)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
            .execute(&mut *tx)
            .await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        let held: Option<(String, i64)> =
            sqlx::query_as("SELECT owner,expires_at FROM artifact_gc_sweep WHERE id=1 FOR UPDATE")
                .fetch_optional(&mut *tx)
                .await?;
        if !held.is_some_and(|(held_owner, expires_at)| held_owner == owner && expires_at > now) {
            tx.rollback().await?;
            bail!("remote GC does not own the live publication fence")
        }
        Ok(Box::new(PostgresGcDeleteFence(Some(tx))))
    }
    async fn register_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
        ttl: i64,
    ) -> Result<()> {
        validate_transport_lease_identity(root, session, workspace, repo, ttl)?;
        let (mut tx, now) = self.controlled().await?;
        ensure_gc_unfenced_pg(&mut tx, now).await?;
        let foreign: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact_transport_leases WHERE session_id=$1 AND (workspace<>$2 OR repo<>$3)")
            .bind(session).bind(workspace).bind(repo).fetch_one(&mut *tx).await?;
        if foreign != 0 {
            bail!("transport session is already bound to another repository")
        }
        let changed = sqlx::query("INSERT INTO artifact_transport_leases(root_hash,session_id,workspace,repo,expires_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(root_hash,session_id) DO UPDATE SET expires_at=EXCLUDED.expires_at WHERE artifact_transport_leases.workspace=EXCLUDED.workspace AND artifact_transport_leases.repo=EXCLUDED.repo")
            .bind(root).bind(session).bind(workspace).bind(repo).bind(now + ttl).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            bail!("transport root identity conflict")
        }
        tx.commit().await?;
        Ok(())
    }

    async fn renew_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
        ttl: i64,
    ) -> Result<bool> {
        validate_transport_lease_identity(root, session, workspace, repo, ttl)?;
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        let won = sqlx::query("UPDATE artifact_transport_leases SET expires_at=$1 WHERE root_hash=$2 AND session_id=$3 AND workspace=$4 AND repo=$5 AND expires_at>$6")
            .bind(now + ttl).bind(root).bind(session).bind(workspace).bind(repo).bind(now).execute(&mut *tx).await?.rows_affected() == 1;
        tx.commit().await?;
        Ok(won)
    }

    async fn release_transport_root(
        &self,
        root: &str,
        session: &str,
        workspace: &str,
        repo: &str,
    ) -> Result<bool> {
        validate_transport_lease_identity(root, session, workspace, repo, 1)?;
        Ok(sqlx::query("DELETE FROM artifact_transport_leases WHERE root_hash=$1 AND session_id=$2 AND workspace=$3 AND repo=$4")
            .bind(root).bind(session).bind(workspace).bind(repo).execute(&self.pool).await?.rows_affected() == 1)
    }

    async fn live_transport_roots_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<TransportRootLease>> {
        if limit == 0 || limit > TRANSPORT_ROOT_PAGE_MAX {
            bail!("transport root page limit is invalid")
        }
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        let rows = if let Some((root, session)) = after {
            validate_transport_lease_identity(root, session, "cursor", "cursor", 1)?;
            sqlx::query("SELECT root_hash,session_id,workspace,repo,expires_at FROM artifact_transport_leases WHERE expires_at>$1 AND (root_hash>$2 OR (root_hash=$2 AND session_id>$3)) ORDER BY root_hash,session_id LIMIT $4")
                .bind(now).bind(root).bind(session).bind(limit as i64).fetch_all(&mut *tx).await?
        } else {
            sqlx::query("SELECT root_hash,session_id,workspace,repo,expires_at FROM artifact_transport_leases WHERE expires_at>$1 ORDER BY root_hash,session_id LIMIT $2")
                .bind(now).bind(limit as i64).fetch_all(&mut *tx).await?
        };
        tx.commit().await?;
        rows.into_iter()
            .map(|r| {
                Ok(TransportRootLease {
                    root_hash: r.try_get("root_hash")?,
                    session_id: r.try_get("session_id")?,
                    workspace: r.try_get("workspace")?,
                    repo: r.try_get("repo")?,
                    expires_at: r.try_get("expires_at")?,
                })
            })
            .collect()
    }

    async fn live_scheduler_roots_page(
        &self,
        after_artifact_id: Option<i64>,
        limit: u32,
    ) -> Result<Vec<SchedulerGcRoot>> {
        if limit == 0
            || limit > TRANSPORT_ROOT_PAGE_MAX
            || after_artifact_id.is_some_and(|id| id < 0)
        {
            bail!("scheduler GC root page cursor or limit is invalid")
        }
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        let rows = sqlx::query(
            "WITH candidates(id) AS ((SELECT published_artifact_id FROM artifact_observations WHERE published_artifact_id>$1 ORDER BY published_artifact_id LIMIT $3) UNION ALL (SELECT artifact_id FROM artifact_consumers WHERE artifact_id>$1 AND expires_at>$2 ORDER BY artifact_id LIMIT $3) UNION ALL (SELECT artifact_id FROM artifact_base_retention WHERE artifact_id>$1 ORDER BY artifact_id LIMIT $3) UNION ALL (SELECT m.artifact_id FROM ready_publication_fence_members m JOIN ready_publication_fences f ON f.token=m.token AND f.generation=m.generation WHERE m.artifact_id>$1 AND (f.state='activation_unknown' OR f.expires_at>$2) ORDER BY m.artifact_id LIMIT $3)), page_ids(id) AS (SELECT DISTINCT id FROM candidates ORDER BY id LIMIT $3) SELECT j.id,j.workspace,j.repo,j.commit_oid,j.kind,j.format_version,j.manifest FROM page_ids p JOIN artifact_jobs j ON j.id=p.id WHERE j.state='ready' AND j.manifest IS NOT NULL AND length(trim(j.manifest))>0 ORDER BY j.id",
        )
        .bind(after_artifact_id.unwrap_or(0)).bind(now).bind(limit as i64)
        .fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows.into_iter()
            .map(|row| {
                let version = u32::try_from(row.try_get::<i64, _>("format_version")?)
                    .context("scheduler GC root format")?;
                Ok(SchedulerGcRoot {
                    artifact_id: row.try_get("id")?,
                    key: ArtifactKey {
                        workspace: row.try_get("workspace")?,
                        repo: row.try_get("repo")?,
                        commit: row.try_get("commit_oid")?,
                        kind: ArtifactKind::parse(row.try_get("kind")?)?,
                        format_version: version,
                    },
                    manifest: row.try_get("manifest")?,
                })
            })
            .collect()
    }

    async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome> {
        validate_format_version(key.format_version)?;
        let (mut tx, now) = self.controlled().await?;
        ensure_gc_unfenced_pg(&mut tx, now).await?;
        self.preflight_batch(
            &mut tx,
            &key.workspace,
            &key.repo,
            &key.commit,
            &[key.kind],
            key.format_version,
        )
        .await?;
        let outcome = self.schedule_unchecked(&mut tx, key, now).await?;
        tx.commit().await?;
        Ok(outcome)
    }

    async fn subscribe_consumer(
        &self,
        key: &ArtifactKey,
        consumer_id: &str,
        ttl_secs: i64,
    ) -> Result<ScheduleOutcome> {
        validate_format_version(key.format_version)?;
        if consumer_id.trim().is_empty() {
            bail!("artifact consumer id is empty")
        }
        if !(2..=86400).contains(&ttl_secs) {
            bail!("consumer subscription TTL is invalid")
        }
        let (mut tx, now) = self.controlled().await?;
        ensure_gc_unfenced_pg(&mut tx, now).await?;
        self.preflight_batch(
            &mut tx,
            &key.workspace,
            &key.repo,
            &key.commit,
            &[key.kind],
            key.format_version,
        )
        .await?;
        let outcome = self.schedule_unchecked(&mut tx, key, now).await?;
        sqlx::query(
            "INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at)
             VALUES($1,$2,$3)
             ON CONFLICT(artifact_id,consumer_id)
             DO UPDATE SET expires_at=excluded.expires_at",
        )
        .bind(outcome_id(&outcome))
        .bind(consumer_id)
        .bind(now + ttl_secs)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(outcome)
    }

    async fn release_consumer(&self, artifact_id: i64, consumer_id: &str) -> Result<()> {
        let (mut tx, _) = self.controlled().await?;
        sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=$1 AND consumer_id=$2")
            .bind(artifact_id)
            .bind(consumer_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "DELETE FROM artifact_jobs WHERE id=$1 AND state='queued'
             AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations)
             AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",
        )
        .bind(artifact_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn observe(
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
        if kinds.is_empty() {
            bail!("observation requests no artifact kinds")
        }
        validate_format_version(format_version)?;
        let mut unique = Vec::new();
        for &kind in kinds {
            if !unique.contains(&kind) {
                unique.push(kind);
            }
        }
        let (mut tx, now) = self.controlled().await?;
        ensure_gc_unfenced_pg(&mut tx, now).await?;
        let current: Option<(i64, String)> = sqlx::query_as(
            "SELECT generation,desired_commit FROM branch_observations
             WHERE workspace=$1 AND repo=$2 AND branch=$3",
        )
        .bind(workspace)
        .bind(repo)
        .bind(branch)
        .fetch_optional(&mut *tx)
        .await?;
        let current_generation = current.as_ref().map(|(value, _)| *value as u64);
        let same_commit = current
            .as_ref()
            .is_some_and(|(_, current_commit)| current_commit == commit);
        let mut fully_observed = same_commit;
        if same_commit {
            for kind in &unique {
                let present: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM artifact_observations WHERE workspace=$1 AND repo=$2 AND branch=$3 AND kind=$4 AND desired_commit=$5 AND format_version=$6",
                )
                .bind(workspace).bind(repo).bind(branch).bind(kind.as_str()).bind(commit)
                .bind(format_version as i64).fetch_one(&mut *tx).await?;
                fully_observed &= present == 1;
            }
        }
        if fully_observed {
            tx.rollback().await?;
            return Ok(ObservationOutcome::Unchanged {
                generation: current_generation.context("existing observation has no generation")?,
            });
        }
        let current = current_generation;
        if current != expected_generation {
            tx.rollback().await?;
            return Ok(ObservationOutcome::Stale {
                current_generation: current.unwrap_or(0),
            });
        }
        let generation = current
            .unwrap_or(0)
            .checked_add(1)
            .context("observation generation overflow")?;

        for kind in &unique {
            sqlx::query(
                "DELETE FROM artifact_jobs WHERE state='queued'
                 AND id IN(SELECT desired_artifact_id FROM artifact_observations
                           WHERE workspace=$1 AND repo=$2 AND branch=$3 AND kind=$4)
                 AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations
                               WHERE NOT(workspace=$1 AND repo=$2 AND branch=$3 AND kind=$4))
                 AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",
            )
            .bind(workspace)
            .bind(repo)
            .bind(branch)
            .bind(kind.as_str())
            .execute(&mut *tx)
            .await?;
        }
        self.preflight_batch(&mut tx, workspace, repo, commit, &unique, format_version)
            .await?;
        let mut artifacts = Vec::with_capacity(unique.len());
        for kind in unique {
            let key = ArtifactKey {
                workspace: workspace.into(),
                repo: repo.into(),
                commit: commit.into(),
                kind,
                format_version,
            };
            let outcome = self.schedule_unchecked(&mut tx, &key, now).await?;
            let id = outcome_id(&outcome);
            sqlx::query(
                "INSERT INTO artifact_observations(
                    workspace,repo,branch,kind,desired_commit,desired_artifact_id,
                    desired_generation,published_artifact_id,format_version,observed_at)
                 VALUES($1,$2,$3,$4,$5,$6,$7,
                    CASE WHEN (SELECT state FROM artifact_jobs WHERE id=$6)='ready'
                         THEN $6 ELSE NULL END,$8,$9)
                 ON CONFLICT(workspace,repo,branch,kind) DO UPDATE SET
                    desired_commit=excluded.desired_commit,
                    desired_artifact_id=excluded.desired_artifact_id,
                    desired_generation=excluded.desired_generation,
                    published_artifact_id=CASE
                      WHEN (SELECT state FROM artifact_jobs WHERE id=excluded.desired_artifact_id)='ready'
                        THEN excluded.desired_artifact_id
                      WHEN artifact_observations.format_version=excluded.format_version
                        THEN artifact_observations.published_artifact_id
                      ELSE NULL END,
                    format_version=excluded.format_version,observed_at=excluded.observed_at",
            )
            .bind(workspace)
            .bind(repo)
            .bind(branch)
            .bind(kind.as_str())
            .bind(commit)
            .bind(id)
            .bind(generation as i64)
            .bind(format_version as i64)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            artifacts.push((kind, outcome));
        }
        sqlx::query(
            "INSERT INTO branch_observations(
                workspace,repo,branch,generation,desired_commit,updated_at)
             VALUES($1,$2,$3,$4,$5,$6)
             ON CONFLICT(workspace,repo,branch) DO UPDATE SET
                generation=excluded.generation,desired_commit=excluded.desired_commit,
                updated_at=excluded.updated_at",
        )
        .bind(workspace)
        .bind(repo)
        .bind(branch)
        .bind(generation as i64)
        .bind(commit)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM artifact_jobs WHERE workspace=$1 AND repo=$2 AND state='queued'
             AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations)
             AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",
        )
        .bind(workspace)
        .bind(repo)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(ObservationOutcome::Accepted {
            generation,
            artifacts,
        })
    }

    async fn retry_failed(&self, key: &ArtifactKey) -> Result<RetryOutcome> {
        let (mut tx, now) = self.controlled().await?;
        let row: Option<(i64, String, Option<String>, i64)> = sqlx::query_as(
            "SELECT id,state,failure_class,retry_count FROM artifact_jobs
             WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND kind=$4 AND format_version=$5
             FOR UPDATE",
        )
        .bind(&key.workspace)
        .bind(&key.repo)
        .bind(&key.commit)
        .bind(key.kind.as_str())
        .bind(key.format_version as i64)
        .fetch_optional(&mut *tx)
        .await?;
        let outcome = match row {
            None => RetryOutcome::NotFailed,
            Some((_, state, _, _)) if state != "failed" => RetryOutcome::NotFailed,
            Some((_, _, class, _))
                if FailureClass::parse(class.as_deref().unwrap_or("permanent"))?
                    != FailureClass::Retryable =>
            {
                RetryOutcome::NotRetryable(FailureClass::parse(
                    class.as_deref().unwrap_or("permanent"),
                )?)
            }
            Some((_, _, _, retries)) if retries as u32 >= self.limits.max_manual_retries => {
                RetryOutcome::Exhausted
            }
            Some((id, _, _, _)) => {
                self.preflight_capacity(&mut tx, key.kind, &key.workspace, 1)
                    .await?;
                let changed = sqlx::query(
                    "UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,
                        lease_expires_at=NULL,retry_count=retry_count+1,error=NULL,
                        failure_class=NULL,updated_at=$1 WHERE id=$2 AND state='failed'",
                )
                .bind(now)
                .bind(id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if changed != 1 {
                    bail!("locked failed artifact changed unexpectedly")
                }
                RetryOutcome::Requeued(id)
            }
        };
        tx.commit().await?;
        Ok(outcome)
    }

    async fn observation_snapshot(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<ObservationSnapshot> {
        validate_observation_identity(workspace, repo, branch, "snapshot")?;
        let row: Option<(i64, String)> = sqlx::query_as(
            "SELECT generation,desired_commit FROM branch_observations WHERE workspace=$1 AND repo=$2 AND branch=$3",
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

    async fn claim(&self, owner: &str, lease_secs: i64) -> Result<Option<ClaimedArtifact>> {
        validate_lease(owner, lease_secs)?;
        let (mut tx, now) = self.controlled().await?;
        let total: i64 =
            sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state='running'")
                .fetch_one(&mut *tx)
                .await?;
        if total as usize >= self.limits.total_running {
            tx.rollback().await?;
            return Ok(None);
        }
        let (cursor, workspace_cursor): (i64, String) = sqlx::query_as(
            "SELECT fairness_cursor,workspace_cursor FROM scheduler_state WHERE id=1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let lanes = [
            ArtifactKind::Head,
            ArtifactKind::Head,
            ArtifactKind::FullHistory,
            ArtifactKind::Files,
        ];
        for offset in 0..lanes.len() {
            let position = (cursor as usize + offset) % lanes.len();
            let kind = lanes[position];
            let running: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM artifact_jobs WHERE state='running' AND kind=$1",
            )
            .bind(kind.as_str())
            .fetch_one(&mut *tx)
            .await?;
            if running as usize >= self.running_limit(kind) {
                continue;
            }
            let id: Option<i64> = if kind.expensive() {
                sqlx::query_scalar(
                    "SELECT q.id FROM artifact_jobs q
                     WHERE q.state='queued' AND q.kind=$1
                       AND (SELECT count(*) FROM artifact_jobs wr
                            WHERE wr.state='running' AND wr.workspace=q.workspace) < $2
                       AND NOT EXISTS(SELECT 1 FROM artifact_jobs r
                           WHERE r.state='running' AND r.workspace=q.workspace AND r.repo=q.repo
                             AND r.kind=q.kind)
                     ORDER BY CASE WHEN q.workspace>$3 THEN 0 ELSE 1 END,
                              q.workspace,q.created_at,q.id
                     LIMIT 1 FOR UPDATE OF q SKIP LOCKED",
                )
                .bind(kind.as_str())
                .bind(self.limits.workspace_running as i64)
                .bind(&workspace_cursor)
                .fetch_optional(&mut *tx)
                .await?
            } else {
                sqlx::query_scalar(
                    "SELECT q.id FROM artifact_jobs q
                     WHERE q.state='queued' AND q.kind=$1
                       AND (SELECT count(*) FROM artifact_jobs wr
                            WHERE wr.state='running' AND wr.workspace=q.workspace) < $2
                     ORDER BY CASE WHEN q.workspace>$3 THEN 0 ELSE 1 END,
                              q.workspace,q.created_at,q.id
                     LIMIT 1 FOR UPDATE OF q SKIP LOCKED",
                )
                .bind(kind.as_str())
                .bind(self.limits.workspace_running as i64)
                .bind(&workspace_cursor)
                .fetch_optional(&mut *tx)
                .await?
            };
            let Some(id) = id else { continue };
            let won = sqlx::query(
                "UPDATE artifact_jobs SET state='running',owner=$1,heartbeat_at=$2,
                    lease_expires_at=$3,lease_generation=lease_generation+1,
                    claim_attempts=claim_attempts+1,updated_at=$2
                 WHERE id=$4 AND state='queued'",
            )
            .bind(owner)
            .bind(now)
            .bind(now + lease_secs)
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if won == 1 {
                let record = Self::get_tx(&mut tx, id)
                    .await?
                    .context("claimed artifact disappeared")?;
                sqlx::query(
                    "UPDATE scheduler_state SET fairness_cursor=$1,workspace_cursor=$2 WHERE id=1",
                )
                .bind(((position + 1) % lanes.len()) as i64)
                .bind(&record.key.workspace)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                return Ok(Some(ClaimedArtifact { record }));
            }
        }
        tx.commit().await?;
        Ok(None)
    }

    async fn heartbeat(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        lease_secs: i64,
    ) -> Result<bool> {
        validate_lease(owner, lease_secs)?;
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        let won = sqlx::query(
            "UPDATE artifact_jobs SET heartbeat_at=$1,lease_expires_at=$2,updated_at=$1
             WHERE id=$3 AND state='running' AND owner=$4 AND lease_generation=$5
               AND lease_expires_at>=$1",
        )
        .bind(now)
        .bind(now + lease_secs)
        .bind(claim.record.id)
        .bind(owner)
        .bind(claim.record.lease_generation as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(won)
    }

    async fn owns(&self, claim: &ClaimedArtifact, owner: &str) -> Result<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE id=$1 AND state='running' AND owner=$2 AND lease_generation=$3
               AND lease_expires_at>=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT",
        )
        .bind(claim.record.id)
        .bind(owner)
        .bind(claim.record.lease_generation as i64)
        .fetch_one(&self.pool)
        .await?;
        Ok(count == 1)
    }

    async fn complete_verified(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        verified: &VerifiedCompletionEvidence,
    ) -> Result<bool> {
        let evidence = self.completion_sealer.verify(claim, verified)?;
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        ensure_gc_unfenced_pg(&mut tx, now).await?;
        let won = sqlx::query(
            "UPDATE artifact_jobs SET state='ready',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,manifest=$1,error=NULL,failure_class=NULL,updated_at=$2
             WHERE id=$3 AND state='running' AND owner=$4 AND lease_generation=$5
               AND lease_expires_at>=$2",
        )
        .bind(evidence.manifest())
        .bind(now)
        .bind(claim.record.id)
        .bind(owner)
        .bind(claim.record.lease_generation as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        if won {
            // Exact desired identity and format were established atomically by
            // observe. The id predicate prevents an older completion from ever
            // repointing a branch that has advanced.
            sqlx::query(
                "UPDATE artifact_observations SET published_artifact_id=$1
                 WHERE desired_artifact_id=$1",
            )
            .bind(claim.record.id)
            .execute(&mut *tx)
            .await?;
            refresh_base_retention(
                &mut tx,
                &claim.record.key.workspace,
                &claim.record.key.repo,
                claim.record.key.format_version as i64,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(won)
    }

    async fn fail(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        class: FailureClass,
        error: &str,
    ) -> Result<bool> {
        if error.trim().is_empty() {
            bail!("artifact failure reason is empty")
        }
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM clock_timestamp())::BIGINT")
            .fetch_one(&mut *tx)
            .await?;
        let won = sqlx::query(
            "UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,error=$1,failure_class=$2,updated_at=$3
             WHERE id=$4 AND state='running' AND owner=$5 AND lease_generation=$6
               AND lease_expires_at>=$3",
        )
        .bind(error)
        .bind(class.as_str())
        .bind(now)
        .bind(claim.record.id)
        .bind(owner)
        .bind(claim.record.lease_generation as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(won)
    }

    async fn reconcile_expired(&self) -> Result<(u64, u64)> {
        let (mut tx, now) = self.controlled().await?;
        sqlx::query("DELETE FROM artifact_consumers WHERE expires_at<=$1")
            .bind(now)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "DELETE FROM artifact_transport_leases WHERE ctid IN
             (SELECT ctid FROM artifact_transport_leases WHERE expires_at<=$1
              ORDER BY expires_at,root_hash,session_id LIMIT 512)",
        )
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM artifact_jobs WHERE state='queued'
             AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations)
             AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",
        )
        .execute(&mut *tx)
        .await?;
        let failed = sqlx::query(
            "UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,error='lease expired after attempt limit',
                failure_class='dead_letter',updated_at=$1
             WHERE state='running' AND lease_expires_at<=$1 AND claim_attempts>=$2",
        )
        .bind(now)
        .bind(self.limits.max_claim_attempts as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let queued = sqlx::query(
            "UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,error='lease expired; reclaimed',updated_at=$1
             WHERE state='running' AND lease_expires_at<=$1 AND claim_attempts<$2",
        )
        .bind(now)
        .bind(self.limits.max_claim_attempts as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok((queued, failed))
    }

    async fn get(&self, id: i64) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,
                    lease_expires_at,lease_generation,claim_attempts,retry_count,
                    manifest,error,failure_class FROM artifact_jobs WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_record).transpose()
    }

    async fn get_by_key(&self, key: &ArtifactKey) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,
                    lease_expires_at,lease_generation,claim_attempts,retry_count,
                    manifest,error,failure_class FROM artifact_jobs
             WHERE workspace=$1 AND repo=$2 AND commit_oid=$3 AND kind=$4 AND format_version=$5",
        )
        .bind(&key.workspace)
        .bind(&key.repo)
        .bind(&key.commit)
        .bind(key.kind.as_str())
        .bind(key.format_version as i64)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_record).transpose()
    }

    async fn ready_page(&self, after_id: i64, limit: usize) -> Result<Vec<ArtifactRecord>> {
        if after_id < 0 || !(1..=1000).contains(&limit) {
            bail!("invalid ready scrub page");
        }
        sqlx::query("SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE state='ready' AND manifest IS NOT NULL AND id>$1 ORDER BY id LIMIT $2")
            .bind(after_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(row_record)
            .collect()
    }

    async fn quarantine_ready(
        &self,
        id: i64,
        manifest: Option<&str>,
        reason: &str,
    ) -> Result<QuarantineOutcome> {
        let Some(manifest) = manifest else {
            return Ok(QuarantineOutcome::LostRace);
        };
        if id <= 0 || manifest.trim().is_empty() || reason.trim().is_empty() {
            bail!("invalid ready quarantine request");
        }
        let (mut tx, now) = self.controlled().await?;
        let row:Option<i64>=sqlx::query_scalar("SELECT retry_count FROM artifact_jobs WHERE id=$1 AND state='ready' AND manifest=$2 FOR UPDATE").bind(id).bind(manifest).fetch_optional(&mut *tx).await?;
        let Some(retries) = row else {
            tx.rollback().await?;
            return Ok(QuarantineOutcome::LostRace);
        };
        let fenced:i64=sqlx::query_scalar("SELECT count(*) FROM ready_publication_fence_members m JOIN ready_publication_fences f ON f.token=m.token AND f.generation=m.generation WHERE m.artifact_id=$1 AND (f.state='activation_unknown' OR f.expires_at>$2)").bind(id).bind(now).fetch_one(&mut *tx).await?;
        if fenced != 0 {
            tx.rollback().await?;
            return Ok(QuarantineOutcome::LostRace);
        }
        let exhausted = retries as u32 >= self.limits.max_manual_retries;
        let changed=sqlx::query(if exhausted {"UPDATE artifact_jobs SET state='failed',manifest=NULL,owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error=$1,failure_class='permanent',updated_at=$2 WHERE id=$3 AND state='ready' AND manifest=$4"} else {"UPDATE artifact_jobs SET state='queued',manifest=NULL,owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,retry_count=retry_count+1,error=$1,failure_class=NULL,updated_at=$2 WHERE id=$3 AND state='ready' AND manifest=$4"})
            .bind(reason.chars().take(4096).collect::<String>()).bind(now).bind(id).bind(manifest).execute(&mut *tx).await?.rows_affected()==1;
        if changed {
            sqlx::query("UPDATE artifact_observations SET published_artifact_id=NULL WHERE published_artifact_id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(if !changed {
            QuarantineOutcome::LostRace
        } else if exhausted {
            QuarantineOutcome::Exhausted
        } else {
            QuarantineOutcome::Requeued(id)
        })
    }

    async fn ready_candidates(
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
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE workspace=$1 AND repo=$2 AND kind=$3 AND format_version=$4 AND state='ready' AND manifest IS NOT NULL AND length(trim(manifest))>0 ORDER BY updated_at DESC,id DESC LIMIT $5",
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

    async fn published(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
        kind: ArtifactKind,
        format_version: u32,
    ) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT j.id,j.workspace,j.repo,j.commit_oid,j.kind,j.format_version,j.state,j.owner,
                    j.lease_expires_at,j.lease_generation,j.claim_attempts,j.retry_count,
                    j.manifest,j.error,j.failure_class
             FROM artifact_observations a JOIN artifact_jobs j
               ON j.id=a.published_artifact_id AND j.workspace=a.workspace AND j.repo=a.repo
              AND j.kind=a.kind AND j.format_version=a.format_version
             WHERE a.workspace=$1 AND a.repo=$2 AND a.branch=$3 AND a.kind=$4
               AND a.format_version=$5 AND j.state='ready' AND j.manifest IS NOT NULL
               AND length(trim(j.manifest))>0",
        )
        .bind(workspace)
        .bind(repo)
        .bind(branch)
        .bind(kind.as_str())
        .bind(format_version as i64)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_record).transpose()
    }

    async fn complete_full_base_candidates(
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
        sqlx::query_scalar(
            "SELECT h.commit_oid FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' WHERE h.workspace=$1 AND h.repo=$2 AND h.format_version=$3 AND h.kind='head' AND h.state='ready' AND f.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0 AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 ORDER BY GREATEST(h.updated_at,f.updated_at) DESC,GREATEST(h.id,f.id) DESC LIMIT $4",
        )
        .bind(workspace)
        .bind(repo)
        .bind(format_version as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn quarantine_publication(
        &self,
        key: &ArtifactKey,
        expected_manifest: &str,
        reason: &str,
    ) -> Result<bool> {
        validate_format_version(key.format_version)?;
        crate::cas::Cas::validate_artifact_id(expected_manifest)?;
        let mut tx = self.pool.begin().await?;
        let id: Option<i64> = sqlx::query_scalar(
            "UPDATE artifact_jobs SET state='failed',manifest=NULL,error=$1,failure_class='retryable',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,updated_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE workspace=$2 AND repo=$3 AND commit_oid=$4 AND kind=$5 AND format_version=$6 AND state='ready' AND manifest=$7 RETURNING id",
        )
        .bind(reason)
        .bind(&key.workspace)
        .bind(&key.repo)
        .bind(&key.commit)
        .bind(key.kind.as_str())
        .bind(key.format_version as i64)
        .bind(expected_manifest)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(id) = id {
            sqlx::query("UPDATE artifact_observations SET published_artifact_id=NULL WHERE published_artifact_id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            refresh_base_retention(
                &mut tx,
                &key.workspace,
                &key.repo,
                key.format_version as i64,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(id.is_some())
    }

    async fn counts(&self) -> Result<Vec<(ArtifactKind, ArtifactState, u64)>> {
        let rows = sqlx::query(
            "SELECT kind,state,count(*) AS count FROM artifact_jobs GROUP BY kind,state ORDER BY kind,state",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let count: i64 = row.try_get("count")?;
                if count < 0 {
                    bail!("postgres returned a negative artifact count")
                }
                Ok((
                    ArtifactKind::parse(row.try_get("kind")?)?,
                    ArtifactState::parse(row.try_get("state")?)?,
                    count as u64,
                ))
            })
            .collect()
    }
}

fn row_record(row: sqlx::postgres::PgRow) -> Result<ArtifactRecord> {
    let format_version = row.try_get::<i64, _>("format_version")?;
    let lease_generation = row.try_get::<i64, _>("lease_generation")?;
    let claim_attempts = row.try_get::<i64, _>("claim_attempts")?;
    let retry_count = row.try_get::<i64, _>("retry_count")?;
    if !(1..=u32::MAX as i64).contains(&format_version)
        || lease_generation < 0
        || !(0..=u32::MAX as i64).contains(&claim_attempts)
        || !(0..=u32::MAX as i64).contains(&retry_count)
    {
        bail!("postgres artifact scheduler row contains an invalid unsigned value")
    }
    Ok(ArtifactRecord {
        id: row.try_get("id")?,
        key: ArtifactKey {
            workspace: row.try_get("workspace")?,
            repo: row.try_get("repo")?,
            commit: row.try_get("commit_oid")?,
            kind: ArtifactKind::parse(row.try_get("kind")?)?,
            format_version: format_version as u32,
        },
        state: ArtifactState::parse(row.try_get("state")?)?,
        owner: row.try_get("owner")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        lease_generation: lease_generation as u64,
        claim_attempts: claim_attempts as u32,
        retry_count: retry_count as u32,
        manifest: row.try_get("manifest")?,
        error: row.try_get("error")?,
        failure_class: row
            .try_get::<Option<String>, _>("failure_class")?
            .map(|value| FailureClass::parse(&value))
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler_backend::ArtifactSchedulerPersistence;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    struct Accept;
    impl CompletionVerifier for Accept {
        fn identity(&self) -> &'static str {
            "postgres-live-test-v1"
        }
        fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
            validate_evidence(claim, evidence)
        }
    }

    fn key(commit: &str, kind: ArtifactKind) -> ArtifactKey {
        ArtifactKey {
            workspace: "ws".into(),
            repo: "owner/repo".into(),
            commit: commit.into(),
            kind,
            format_version: 1,
        }
    }

    async fn reset(pool: &PgPool) {
        sqlx::raw_sql(
            "DROP TABLE IF EXISTS ready_publication_fence_members;
             DROP TABLE IF EXISTS ready_publication_fences;
             DROP TABLE IF EXISTS ready_publication_fence_sequence;
             DROP TABLE IF EXISTS artifact_base_retention;
             DROP TABLE IF EXISTS artifact_gc_sweep;
             DROP TABLE IF EXISTS artifact_transport_leases;
             DROP TABLE IF EXISTS artifact_consumers;
             DROP TABLE IF EXISTS artifact_observations;
             DROP TABLE IF EXISTS branch_observations;
             DROP TABLE IF EXISTS artifact_jobs;
             DROP TABLE IF EXISTS scheduler_state;
             DROP TABLE IF EXISTS artifact_scheduler_schema;",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn live_postgres_adversarial_conformance() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
            eprintln!("SKIP live_postgres_adversarial_conformance: RIPCLONE_TEST_PG_URL unset");
            return;
        };
        let control = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect postgres test database");
        let mut advisory = control.acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock(731904218)")
            .execute(&mut *advisory)
            .await
            .unwrap();
        reset(&control).await;
        // Exact released-v3 fixture: no v4 GC table and marker 3. Two v4
        // starters race the same transactional migration below.
        sqlx::raw_sql(SCHEMA).execute(&control).await.unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();

        let a_pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .unwrap();
        let b_pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .unwrap();
        let (a, b) = tokio::join!(
            PostgresArtifactScheduler::from_pool(a_pool, Default::default(), Arc::new(Accept)),
            PostgresArtifactScheduler::from_pool(b_pool, Default::default(), Arc::new(Accept))
        );
        let a = a.unwrap();
        let b = b.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(&control)
            .await
            .unwrap(),
            6
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='artifact_gc_sweep'").fetch_one(&control).await.unwrap(), 1);

        let target = "a".repeat(40);
        let head_id:i64=sqlx::query_scalar("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('admission-ws','admission/repo',$1,'head',1,'ready','head-manifest',0,0) RETURNING id").bind(&target).fetch_one(&control).await.unwrap();
        let full_id:i64=sqlx::query_scalar("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('admission-ws','admission/repo',$1,'full_history',1,'ready','full-manifest',0,0) RETURNING id").bind(&target).fetch_one(&control).await.unwrap();
        let provenance = ActivationFenceProvenance {
            workspace: "admission-ws".into(),
            repo: "admission/repo".into(),
            branch: "main".into(),
            target,
            attempt_id: "live-admission".into(),
        };
        let expected = vec![
            (head_id, Some("head-manifest".into())),
            (full_id, Some("full-manifest".into())),
        ];
        let fence = a
            .fence_ready_publications(&expected, &provenance, 60)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            a.quarantine_ready(head_id, Some("head-manifest"), "race")
                .await
                .unwrap(),
            QuarantineOutcome::LostRace
        );
        assert!(a.mark_activation_unknown(&fence, 1).await.unwrap());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            a.recover_activation_fence(&provenance)
                .await
                .unwrap()
                .is_some(),
            "activation_unknown must not expire"
        );
        let roots = a.live_scheduler_roots_page(None, 512).await.unwrap();
        assert!(roots.iter().any(|r| r.artifact_id == head_id));
        a.release_ready_publication_fence(fence).await.unwrap();
        assert!(
            matches!(a.quarantine_ready(head_id,Some("head-manifest"),"verified corrupt").await.unwrap(),QuarantineOutcome::Requeued(id) if id==head_id)
        );
        sqlx::query("DELETE FROM artifact_jobs WHERE id IN($1,$2)")
            .bind(head_id)
            .bind(full_id)
            .execute(&control)
            .await
            .unwrap();

        assert!(a.acquire_gc_sweep("collector", 60).await.unwrap());
        assert!(!b.acquire_gc_sweep("other", 60).await.unwrap());
        let delete_fence = a.lock_gc_delete_batch("collector").await.unwrap();
        let blocked = {
            let b = b.clone();
            tokio::spawn(async move {
                b.register_transport_root(&"f".repeat(64), &"e".repeat(64), "ws", "owner/repo", 60)
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !blocked.is_finished(),
            "postgres publication bypassed delete transaction lock"
        );
        delete_fence.release().await.unwrap();
        assert!(blocked.await.unwrap().is_err());
        a.release_gc_sweep("collector").await.unwrap();

        let root0 = format!("{:064x}", 1);
        let root1 = format!("{:064x}", 2);
        let session0 = format!("{:064x}", 10_001);
        let session1 = format!("{:064x}", 10_002);
        a.register_transport_root(&root0, &session0, "ws", "owner/repo", 60)
            .await
            .unwrap();
        a.register_transport_root(&root1, &session0, "ws", "owner/repo", 60)
            .await
            .unwrap();
        a.register_transport_root(&root0, &session1, "ws", "owner/repo", 60)
            .await
            .unwrap();
        assert!(
            b.register_transport_root(&format!("{:064x}", 3), &session0, "ws", "other/repo", 60)
                .await
                .is_err()
        );
        assert!(
            !a.renew_transport_root(&root0, &session0, "ws", "other/repo", 60)
                .await
                .unwrap()
        );
        assert!(
            !a.release_transport_root(&root0, &session0, "ws", "other/repo")
                .await
                .unwrap()
        );
        for i in 100..613 {
            a.register_transport_root(
                &format!("{i:064x}"),
                &format!("{:064x}", i + 20_000),
                "ws",
                "owner/repo",
                60,
            )
            .await
            .unwrap();
        }
        let page = a.live_transport_roots_page(None, 512).await.unwrap();
        assert_eq!(page.len(), 512);
        let cursor = page.last().unwrap();
        assert!(
            !a.live_transport_roots_page(Some((&cursor.root_hash, &cursor.session_id)), 512)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(a.live_transport_roots_page(None, 513).await.is_err());
        sqlx::query(
            "UPDATE artifact_transport_leases SET expires_at=0 WHERE root_hash BETWEEN $1 AND $2",
        )
        .bind(format!("{:064x}", 100))
        .bind(format!("{:064x}", 612))
        .execute(a.pool())
        .await
        .unwrap();
        a.reconcile_expired().await.unwrap();
        let bounded_left: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_transport_leases WHERE root_hash BETWEEN $1 AND $2",
        )
        .bind(format!("{:064x}", 100))
        .bind(format!("{:064x}", 612))
        .fetch_one(a.pool())
        .await
        .unwrap();
        assert_eq!(
            bounded_left, 1,
            "reconcile must prune at most 512 transport roots"
        );
        let race_root = format!("{:064x}", 900_001);
        let race_session = format!("{:064x}", 900_002);
        a.register_transport_root(&race_root, &race_session, "ws", "owner/repo", 60)
            .await
            .unwrap();
        let (renewed, released) = tokio::join!(
            a.renew_transport_root(&race_root, &race_session, "ws", "owner/repo", 60),
            b.release_transport_root(&race_root, &race_session, "ws", "owner/repo")
        );
        assert!(released.unwrap());
        let _ = renewed.unwrap();
        assert!(
            !a.renew_transport_root(&race_root, &race_session, "ws", "owner/repo", 60)
                .await
                .unwrap()
        );
        let expired_root = format!("{:064x}", 900_003);
        let expired_session = format!("{:064x}", 900_004);
        a.register_transport_root(&expired_root, &expired_session, "ws", "owner/repo", 1)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(
            !a.renew_transport_root(&expired_root, &expired_session, "ws", "owner/repo", 60)
                .await
                .unwrap()
        );

        let mut gc_ids = Vec::new();
        for (commit, manifest) in [
            ("gc-consumer", "gc-a"),
            ("gc-published", "gc-b"),
            ("gc-superseded", "gc-c"),
            ("gc-expired", "gc-d"),
        ] {
            let id: i64 = sqlx::query_scalar("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('gc-ws','gc/repo',$1,'files',1,'ready',$2,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT) RETURNING id").bind(commit).bind(manifest).fetch_one(a.pool()).await.unwrap();
            gc_ids.push(id);
        }
        sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES($1,'admission',EXTRACT(EPOCH FROM clock_timestamp())::BIGINT+60),($2,'expired',EXTRACT(EPOCH FROM clock_timestamp())::BIGINT-1)").bind(gc_ids[0]).bind(gc_ids[3]).execute(a.pool()).await.unwrap();
        sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES('gc-ws','gc/repo','main','files','gc-published',$1,1,$1,1,EXTRACT(EPOCH FROM clock_timestamp())::BIGINT)").bind(gc_ids[1]).execute(a.pool()).await.unwrap();
        let roots = a.live_scheduler_roots_page(None, 512).await.unwrap();
        assert!(roots.iter().any(|r| r.artifact_id == gc_ids[0]));
        assert!(roots.iter().any(|r| r.artifact_id == gc_ids[1]));
        assert!(
            !roots
                .iter()
                .any(|r| r.artifact_id == gc_ids[2] || r.artifact_id == gc_ids[3])
        );
        assert_eq!(
            a.live_scheduler_roots_page(Some(gc_ids[0]), 1)
                .await
                .unwrap()[0]
                .artifact_id,
            gc_ids[1]
        );
        a.release_consumer(gc_ids[0], "admission").await.unwrap();
        let roots = a.live_scheduler_roots_page(None, 512).await.unwrap();
        assert!(!roots.iter().any(|r| r.artifact_id == gc_ids[0]));
        assert!(roots.iter().any(|r| r.artifact_id == gc_ids[1]));
        sqlx::query("DELETE FROM artifact_observations WHERE workspace='gc-ws'")
            .execute(a.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=ANY($1)")
            .bind(&gc_ids)
            .execute(a.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM artifact_jobs WHERE workspace='gc-ws'")
            .execute(a.pool())
            .await
            .unwrap();

        let mut zero = key("zero", ArtifactKind::Head);
        zero.format_version = 0;
        assert!(a.schedule(&zero).await.is_err());
        assert!(
            a.observe(
                "ws",
                "owner/repo",
                "zero",
                "zero",
                &[ArtifactKind::Head],
                0,
                None
            )
            .await
            .is_err()
        );

        let duplicate = key("dedup", ArtifactKind::Head);
        let (scheduled_a, scheduled_b) =
            tokio::join!(a.schedule(&duplicate), b.schedule(&duplicate));
        let scheduled = [scheduled_a.unwrap(), scheduled_b.unwrap()];
        assert_eq!(
            scheduled
                .iter()
                .filter(|outcome| matches!(outcome, ScheduleOutcome::Enqueued(_)))
                .count(),
            1
        );
        assert_eq!(outcome_id(&scheduled[0]), outcome_id(&scheduled[1]));
        // An exact job without a branch/clone subscriber is reclaimed, keeping
        // the rest of this scenario focused on observed work.
        a.reconcile_expired().await.unwrap();

        // Two observers racing the same generation cannot both publish their
        // chosen upstream tip, and each accepted batch is all-or-nothing.
        let (one, two) = tokio::join!(
            a.observe(
                "ws",
                "owner/repo",
                "main",
                "tip-a",
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                None
            ),
            b.observe(
                "ws",
                "owner/repo",
                "main",
                "tip-b",
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                None
            )
        );
        let outcomes = [one.unwrap(), two.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ObservationOutcome::Accepted { .. }))
                .count(),
            1
        );
        let snapshot = a
            .observation_snapshot("ws", "owner/repo", "main")
            .await
            .unwrap();
        assert_eq!(snapshot.generation(), Some(1));
        assert_eq!(
            b.observe(
                "ws",
                "owner/repo",
                "main",
                snapshot.commit().unwrap(),
                &[ArtifactKind::Files, ArtifactKind::Head],
                1,
                None,
            )
            .await
            .unwrap(),
            ObservationOutcome::Unchanged { generation: 1 }
        );
        assert_eq!(
            a.counts().await.unwrap(),
            vec![
                (ArtifactKind::Files, ArtifactState::Queued, 1),
                (ArtifactKind::Head, ArtifactState::Queued, 1),
            ]
        );

        // Two workers cannot own one row. The two accepted kinds may be
        // claimed independently, with fleet-wide caps still enforced.
        let (first, second) = tokio::join!(a.claim("worker-a", 5), b.claim("worker-b", 5));
        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_ne!(first.record.id, second.record.id);
        assert!(a.owns(&first, "worker-a").await.unwrap());
        assert!(!b.owns(&first, "worker-b").await.unwrap());

        // Reclaim fences the old generation even when the successor uses the
        // same owner string (ABA), and database time drives expiry.
        sqlx::query("UPDATE artifact_jobs SET lease_expires_at=0 WHERE id=$1")
            .bind(first.record.id)
            .execute(a.pool())
            .await
            .unwrap();
        assert_eq!(a.reconcile_expired().await.unwrap(), (1, 0));
        let replacement = a.claim("worker-a", 5).await.unwrap().unwrap();
        assert_eq!(replacement.record.id, first.record.id);
        assert!(replacement.record.lease_generation > first.record.lease_generation);
        let stale_evidence = CompletionEvidence::new(first.record.key.clone(), "stale").unwrap();
        assert!(
            !a.complete(&first, "worker-a", &stale_evidence)
                .await
                .unwrap()
        );
        let evidence = CompletionEvidence::new(replacement.record.key.clone(), "manifest").unwrap();
        assert!(
            a.complete(&replacement, "worker-a", &evidence)
                .await
                .unwrap()
        );

        // Finish the other kind so it cannot interfere with the retarget test.
        let second_evidence = CompletionEvidence::new(second.record.key.clone(), "files").unwrap();
        assert!(
            b.complete(&second, "worker-b", &second_evidence)
                .await
                .unwrap()
        );

        let generation: i64 = sqlx::query_scalar(
            "SELECT generation FROM branch_observations
             WHERE workspace='ws' AND repo='owner/repo' AND branch='main'",
        )
        .fetch_one(a.pool())
        .await
        .unwrap();
        a.observe(
            "ws",
            "owner/repo",
            "main",
            "new-tip",
            &[ArtifactKind::Head],
            1,
            Some(generation as u64),
        )
        .await
        .unwrap();
        // Same-format retarget intentionally keeps the last ready base.
        let old = a
            .published("ws", "owner/repo", "main", ArtifactKind::Head, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.key.commit, replacement.record.key.commit);
        let new_claim = b.claim("worker-new", 5).await.unwrap().unwrap();
        assert_eq!(new_claim.record.key.commit, "new-tip");
        let new_evidence = CompletionEvidence::new(new_claim.record.key.clone(), "new").unwrap();
        assert!(
            b.complete(&new_claim, "worker-new", &new_evidence)
                .await
                .unwrap()
        );
        assert_eq!(
            a.published("ws", "owner/repo", "main", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .unwrap()
                .key
                .commit,
            "new-tip"
        );

        // A build finishing after its branch retargets may become a reusable
        // exact artifact, but must not publish through the newer alias.
        a.observe(
            "ws",
            "owner/repo",
            "race",
            "race-old",
            &[ArtifactKind::Head],
            1,
            None,
        )
        .await
        .unwrap();
        let race_old = a.claim("race-worker", 5).await.unwrap().unwrap();
        a.observe(
            "ws",
            "owner/repo",
            "race",
            "race-new",
            &[ArtifactKind::Head],
            1,
            Some(1),
        )
        .await
        .unwrap();
        let wrong = CompletionEvidence::new(key("wrong", ArtifactKind::Head), "wrong").unwrap();
        assert!(a.complete(&race_old, "race-worker", &wrong).await.is_err());
        let race_old_evidence =
            CompletionEvidence::new(race_old.record.key.clone(), "race-old").unwrap();
        assert!(
            a.complete(&race_old, "race-worker", &race_old_evidence)
                .await
                .unwrap()
        );
        assert!(
            a.published("ws", "owner/repo", "race", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .is_none()
        );
        let race_new = b.claim("race-new-worker", 5).await.unwrap().unwrap();
        assert_eq!(race_new.record.key.commit, "race-new");
        let race_new_evidence =
            CompletionEvidence::new(race_new.record.key.clone(), "race-new").unwrap();
        assert!(
            b.complete(&race_new, "race-new-worker", &race_new_evidence)
                .await
                .unwrap()
        );

        let owned_key = key("run-owned", ArtifactKind::Head);
        a.schedule(&owned_key).await.unwrap();
        let owned = a.claim("owned-worker", 5).await.unwrap().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let owned_outcome = ArtifactSchedulerPersistence::run_owned(
            &a,
            &owned,
            "owned-worker",
            vec![crate::artifact_scheduler::ArtifactTask::cooperative(
                |context| async move {
                    assert!(!context.cancelled.is_cancelled());
                    std::fs::write(context.scratch.join("proof"), b"ok")?;
                    Ok(())
                },
            )],
            CompletionEvidence::new(owned.record.key.clone(), "owned-manifest").unwrap(),
            5,
            scratch.path(),
        )
        .await
        .unwrap();
        assert_eq!(
            owned_outcome,
            crate::artifact_scheduler::ExecutionOutcome::Ready
        );

        let lease_race_key = key("lease-race", ArtifactKind::Head);
        a.schedule(&lease_race_key).await.unwrap();
        let lease_race = a.claim("lease-race-worker", 5).await.unwrap().unwrap();
        sqlx::query(
            "UPDATE artifact_jobs
             SET lease_expires_at=EXTRACT(EPOCH FROM clock_timestamp())::BIGINT WHERE id=$1",
        )
        .bind(lease_race.record.id)
        .execute(a.pool())
        .await
        .unwrap();
        let (heartbeat_won, reconciled) = tokio::join!(
            a.heartbeat(&lease_race, "lease-race-worker", 5),
            b.reconcile_expired()
        );
        let heartbeat_won = heartbeat_won.unwrap();
        let reconciled = reconciled.unwrap();
        assert!(
            (heartbeat_won && reconciled.0 == 0) || (!heartbeat_won && reconciled.0 == 1),
            "heartbeat/reconcile race had an impossible result: heartbeat={heartbeat_won}, reconcile={reconciled:?}"
        );
        let lease_race_current = a.get(lease_race.record.id).await.unwrap().unwrap();
        if heartbeat_won {
            a.fail(
                &lease_race,
                "lease-race-worker",
                FailureClass::Permanent,
                "test cleanup",
            )
            .await
            .unwrap();
        } else {
            assert_eq!(lease_race_current.state, ArtifactState::Queued);
            let successor = a.claim("lease-race-successor", 5).await.unwrap().unwrap();
            assert_eq!(successor.record.id, lease_race.record.id);
            a.fail(
                &successor,
                "lease-race-successor",
                FailureClass::Permanent,
                "test cleanup",
            )
            .await
            .unwrap();
        }

        // Consumer leases preserve superseded exact work until release.
        let held = key("held", ArtifactKind::Head);
        let held_id = outcome_id(&a.subscribe_consumer(&held, "clone-1", 60).await.unwrap());
        sqlx::query("UPDATE artifact_consumers SET expires_at=0 WHERE artifact_id=$1")
            .bind(held_id)
            .execute(a.pool())
            .await
            .unwrap();
        a.reconcile_expired().await.unwrap();
        assert!(a.get(held_id).await.unwrap().is_none());

        let permanent = key("permanent", ArtifactKind::Head);
        a.schedule(&permanent).await.unwrap();
        let permanent_claim = a.claim("failure-worker", 5).await.unwrap().unwrap();
        assert!(
            a.fail(
                &permanent_claim,
                "failure-worker",
                FailureClass::Permanent,
                "invalid repository"
            )
            .await
            .unwrap()
        );
        assert_eq!(
            a.retry_failed(&permanent).await.unwrap(),
            RetryOutcome::NotRetryable(FailureClass::Permanent)
        );

        let retryable = key("retryable", ArtifactKind::Head);
        a.schedule(&retryable).await.unwrap();
        let retry_claim = a.claim("retry-worker", 5).await.unwrap().unwrap();
        assert!(
            a.fail(
                &retry_claim,
                "retry-worker",
                FailureClass::Retryable,
                "provider unavailable"
            )
            .await
            .unwrap()
        );
        assert!(matches!(
            a.retry_failed(&retryable).await.unwrap(),
            RetryOutcome::Requeued(_)
        ));
        let dead = a.claim("dead-worker", 5).await.unwrap().unwrap();
        sqlx::query("UPDATE artifact_jobs SET claim_attempts=$1,lease_expires_at=0 WHERE id=$2")
            .bind(a.limits.max_claim_attempts as i64)
            .bind(dead.record.id)
            .execute(a.pool())
            .await
            .unwrap();
        assert_eq!(a.reconcile_expired().await.unwrap(), (0, 1));
        assert!(matches!(
            a.schedule(&retryable).await.unwrap(),
            ScheduleOutcome::Failed(_, FailureClass::DeadLetter)
        ));

        struct RejectSameIdentity;
        impl CompletionVerifier for RejectSameIdentity {
            fn identity(&self) -> &'static str {
                "postgres-live-test-v1"
            }
            fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
                bail!("rejected test evidence")
            }
        }
        let rejecting = PostgresArtifactScheduler::from_pool(
            PgPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(RejectSameIdentity),
        )
        .await
        .unwrap();
        let rejected_key = key("verifier-rejected", ArtifactKind::Head);
        rejecting.schedule(&rejected_key).await.unwrap();
        let rejected_claim = rejecting.claim("reject-worker", 5).await.unwrap().unwrap();
        let rejected_evidence =
            CompletionEvidence::new(rejected_claim.record.key.clone(), "rejected").unwrap();
        assert!(
            rejecting
                .complete(&rejected_claim, "reject-worker", &rejected_evidence)
                .await
                .is_err()
        );
        assert_eq!(
            rejecting
                .get(rejected_claim.record.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            ArtifactState::Running
        );
        rejecting
            .fail(
                &rejected_claim,
                "reject-worker",
                FailureClass::Permanent,
                "test cleanup",
            )
            .await
            .unwrap();

        // Same limits but a different verifier is a hard fleet mismatch.
        struct Other;
        impl CompletionVerifier for Other {
            fn identity(&self) -> &'static str {
                "other-verifier"
            }
            fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
                Ok(())
            }
        }
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Other)
            )
            .await
            .is_err()
        );

        let corrupt = key("corrupt-published", ArtifactKind::Head);
        let corrupt_id = outcome_id(
            &a.subscribe_consumer(&corrupt, "hold-corrupt", 60)
                .await
                .unwrap(),
        );
        sqlx::query(
            "UPDATE artifact_observations SET published_artifact_id=$1
             WHERE workspace='ws' AND repo='owner/repo' AND branch='main' AND kind='head'",
        )
        .bind(corrupt_id)
        .execute(a.pool())
        .await
        .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "startup accepted a queued/mismatched published artifact"
        );

        reset(&control).await;
        let blank_pool = PgPoolOptions::new().connect(&url).await.unwrap();
        let blank =
            PostgresArtifactScheduler::from_pool(blank_pool, Default::default(), Arc::new(Accept))
                .await
                .unwrap();
        blank
            .schedule(&key("unknown-provenance", ArtifactKind::Head))
            .await
            .unwrap();
        sqlx::query("UPDATE scheduler_state SET config_fingerprint=''")
            .execute(blank.pool())
            .await
            .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "startup adopted a verifier fingerprint over existing state"
        );

        reset(&control).await;
        let capped_limits = SchedulerLimits {
            total_backlog: 8,
            workspace_backlog: 8,
            head_reserved: 2,
            head_backlog: 8,
            full_history_backlog: 8,
            files_backlog: 8,
            total_running: 4,
            head_running: 2,
            full_history_running: 1,
            files_running: 1,
            workspace_running: 4,
            ..Default::default()
        };
        let capped_a = PostgresArtifactScheduler::from_pool(
            PgPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .unwrap(),
            capped_limits.clone(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        let capped_b = PostgresArtifactScheduler::from_pool(
            PgPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .unwrap(),
            capped_limits,
            Arc::new(Accept),
        )
        .await
        .unwrap();
        for n in 0..6 {
            capped_a
                .schedule(&key(&format!("head-{n}"), ArtifactKind::Head))
                .await
                .unwrap();
        }
        let claims = futures::future::join_all((0..6).map(|n| {
            let scheduler = if n % 2 == 0 {
                capped_a.clone()
            } else {
                capped_b.clone()
            };
            async move { scheduler.claim(&format!("cap-{n}"), 5).await.unwrap() }
        }))
        .await;
        assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 2);

        // Same-repo exclusion is per artifact kind: Files and FullHistory are
        // independent products and must not recreate a phase sequence.
        reset(&control).await;
        let independent = PostgresArtifactScheduler::from_pool(
            PgPoolOptions::new().connect(&url).await.unwrap(),
            SchedulerLimits {
                total_running: 5,
                head_running: 1,
                full_history_running: 2,
                files_running: 2,
                workspace_running: 5,
                ..Default::default()
            },
            Arc::new(Accept),
        )
        .await
        .unwrap();
        independent
            .schedule(&key("full-a", ArtifactKind::FullHistory))
            .await
            .unwrap();
        independent
            .schedule(&key("files-a", ArtifactKind::Files))
            .await
            .unwrap();
        let first = independent.claim("first", 5).await.unwrap().unwrap();
        independent
            .schedule(&key("same-kind-newer", first.record.key.kind))
            .await
            .unwrap();
        let sibling = independent.claim("sibling", 5).await.unwrap().unwrap();
        assert_ne!(first.record.key.kind, sibling.record.key.kind);
        assert!(independent.claim("blocked", 5).await.unwrap().is_none());

        // Aggregate expensive additions, not each kind independently, must
        // preserve the reserved HEAD backlog.
        reset(&control).await;
        let reserve_limits = SchedulerLimits {
            total_backlog: 4,
            workspace_backlog: 4,
            head_reserved: 2,
            head_backlog: 4,
            full_history_backlog: 4,
            files_backlog: 4,
            ..Default::default()
        };
        let reserve = PostgresArtifactScheduler::from_pool(
            PgPoolOptions::new().connect(&url).await.unwrap(),
            reserve_limits,
            Arc::new(Accept),
        )
        .await
        .unwrap();
        reserve
            .schedule(&key("existing-expensive", ArtifactKind::FullHistory))
            .await
            .unwrap();
        assert!(
            reserve
                .observe(
                    "ws",
                    "owner/repo",
                    "reserve",
                    "expensive-batch",
                    &[ArtifactKind::FullHistory, ArtifactKind::Files],
                    1,
                    None
                )
                .await
                .is_err()
        );
        assert_eq!(
            reserve.counts().await.unwrap(),
            vec![(ArtifactKind::FullHistory, ArtifactState::Queued, 1)]
        );

        reset(&control).await;
        let shape_pool = PgPoolOptions::new().connect(&url).await.unwrap();
        PostgresArtifactScheduler::from_pool(
            shape_pool.clone(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query("ALTER TABLE artifact_jobs ALTER COLUMN format_version DROP NOT NULL")
            .execute(&shape_pool)
            .await
            .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "schema with nullable format_version was accepted"
        );

        reset(&control).await;
        let default_pool = PgPoolOptions::new().connect(&url).await.unwrap();
        PostgresArtifactScheduler::from_pool(
            default_pool.clone(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query("ALTER TABLE artifact_jobs ALTER COLUMN lease_generation DROP DEFAULT")
            .execute(&default_pool)
            .await
            .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "schema missing a required lease_generation default was accepted"
        );

        reset(&control).await;
        let check_pool = PgPoolOptions::new().connect(&url).await.unwrap();
        PostgresArtifactScheduler::from_pool(
            check_pool.clone(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::raw_sql(
            "ALTER TABLE artifact_jobs DROP CONSTRAINT artifact_jobs_format;
             ALTER TABLE artifact_jobs ADD CONSTRAINT artifact_jobs_format
               CHECK(true OR (format_version BETWEEN 1 AND 4294967295));",
        )
        .execute(&check_pool)
        .await
        .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "schema with a named but ineffective format check was accepted"
        );

        reset(&control).await;
        let index_pool = PgPoolOptions::new().connect(&url).await.unwrap();
        PostgresArtifactScheduler::from_pool(
            index_pool.clone(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::raw_sql(
            "DROP INDEX artifact_jobs_claim;
             CREATE INDEX artifact_jobs_claim
               ON artifact_jobs(kind,state,created_at,id) WHERE state='queued';",
        )
        .execute(&index_pool)
        .await
        .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "schema with reordered/predicate claim index was accepted"
        );

        reset(&control).await;
        sqlx::raw_sql(
            "CREATE TABLE artifact_scheduler_schema(
                id SMALLINT PRIMARY KEY CHECK(id=1),version BIGINT NOT NULL);
             INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,1);",
        )
        .execute(&control)
        .await
        .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "same-version schema without required named constraints was accepted"
        );

        reset(&control).await;
        sqlx::raw_sql(
            "CREATE TABLE artifact_scheduler_schema(
                id SMALLINT PRIMARY KEY CHECK(id=1),version BIGINT NOT NULL);
             INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,999);",
        )
        .execute(&control)
        .await
        .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err()
        );
        reset(&control).await;
        sqlx::raw_sql(SCHEMA).execute(&control).await.unwrap();
        sqlx::query("DROP TABLE artifact_base_retention")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "postgres v3 missing base-retention table was repaired"
        );

        reset(&control).await;
        sqlx::raw_sql(SCHEMA).execute(&control).await.unwrap();
        sqlx::query("DROP INDEX artifact_base_retention_scope")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "postgres v3 missing base-retention index was repaired"
        );

        reset(&control).await;
        sqlx::raw_sql(SCHEMA).execute(&control).await.unwrap();
        sqlx::query(
            "ALTER TABLE artifact_base_retention DROP CONSTRAINT artifact_base_retention_ranks",
        )
        .execute(&control)
        .await
        .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "postgres v3 missing base-retention constraint was repaired"
        );

        reset(&control).await;
        sqlx::raw_sql(SCHEMA).execute(&control).await.unwrap();
        sqlx::raw_sql("DROP TABLE artifact_base_retention; DROP TABLE artifact_gc_sweep")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,2)")
            .execute(&control)
            .await
            .unwrap();
        let migrated_v2 = PostgresArtifactScheduler::from_pool(
            PgPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(migrated_v2.pool())
            .await
            .unwrap(),
            6
        );

        reset(&control).await;
        sqlx::raw_sql(SCHEMA).execute(&control).await.unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,4)")
            .execute(&control)
            .await
            .unwrap();
        let migrated_v4 = PostgresArtifactScheduler::from_pool(
            PgPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(migrated_v4.pool())
            .await
            .unwrap(),
            6
        );

        reset(&control).await;
        sqlx::raw_sql(SCHEMA).execute(&control).await.unwrap();
        sqlx::query("DROP TABLE artifact_gc_sweep")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,4)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            PostgresArtifactScheduler::from_pool(
                PgPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "partial postgres v4 without GC table was accepted"
        );
        reset(&control).await;
        sqlx::query("SELECT pg_advisory_unlock(731904218)")
            .execute(&mut *advisory)
            .await
            .unwrap();
    }
}
