//! MySQL persistence for the normalized artifact scheduler.
//!
//! Admission and claim transactions lock the singleton scheduler control row.
//! The lock is held only while touching normalized rows; heartbeats and fenced
//! settlement are O(1) conditional updates and do not take the control lock.

use crate::artifact_scheduler::{
    ActivationFenceProvenance, ArtifactKey, ArtifactKind, ArtifactRecord, ArtifactState,
    ClaimedArtifact, CompletionSealAuthority, CompletionVerifier, FailureClass, ObservationOutcome,
    ObservationSnapshot, QuarantineOutcome, ReadyPublicationFence, RetryOutcome, ScheduleOutcome,
    SchedulerLimits, UnknownActivationFencePage, VerifiedCompletionEvidence, scheduler_fingerprint,
    scheduler_limits_fingerprint, validate_lease, validate_limits, validate_resolved_commit,
};
#[cfg(test)]
use crate::artifact_scheduler::{CompletionEvidence, validate_evidence};
use crate::artifact_scheduler_backend::{
    ArtifactSchedulerPersistence, GcDeleteFence, SchedulerGcRoot, TRANSPORT_ROOT_PAGE_MAX,
    TransportRootLease, validate_public_consumer_id, validate_transport_lease_identity,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sqlx::mysql::MySqlPool;
use sqlx::{Acquire, MySql, MySqlConnection, Row, Transaction};
use std::collections::HashMap;
use std::sync::Arc;

const SCHEMA_VERSION: i64 = 7;
const V1_TO_V4_TRANSITION: i64 = 401;
const V2_TO_V4_TRANSITION: i64 = 402;
const V5_TO_V6_DDL: i64 = 501;
const V5_TO_V6_VALIDATED: i64 = 502;
const V6_TO_V7_DDL: i64 = 601;
const V6_TO_V7_VALIDATED: i64 = 602;
const _: () = assert!(V5_TO_V6_DDL > 5 && V5_TO_V6_VALIDATED > 5);
const _: () = assert!(V6_TO_V7_DDL > 6 && V6_TO_V7_VALIDATED > 6);
const SCHEMA: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS artifact_scheduler_schema(
 id SMALLINT NOT NULL PRIMARY KEY,
 version BIGINT NOT NULL,
 CONSTRAINT artifact_scheduler_schema_singleton CHECK(id=1)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS artifact_jobs(
 id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
 workspace VARCHAR(128) NOT NULL, repo VARCHAR(320) NOT NULL,
 commit_oid VARCHAR(64) NOT NULL, kind VARCHAR(16) NOT NULL,
 format_version BIGINT NOT NULL,
 state VARCHAR(16) NOT NULL, owner VARCHAR(255),
 heartbeat_at BIGINT, lease_expires_at BIGINT,
 lease_generation BIGINT NOT NULL DEFAULT 0,
 claim_attempts BIGINT NOT NULL DEFAULT 0,
 retry_count BIGINT NOT NULL DEFAULT 0,
 manifest LONGTEXT, error LONGTEXT, failure_class VARCHAR(16),
 created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
 CONSTRAINT artifact_jobs_identity UNIQUE(workspace,repo,commit_oid,kind,format_version),
 CONSTRAINT artifact_jobs_format CHECK(format_version BETWEEN 1 AND 4294967295),
 CONSTRAINT artifact_jobs_state CHECK(state IN('queued','running','ready','failed')),
 CONSTRAINT artifact_jobs_kind CHECK(kind IN('head','full_history','files')),
 CONSTRAINT artifact_jobs_lease_generation CHECK(lease_generation BETWEEN 0 AND 9223372036854775807),
 CONSTRAINT artifact_jobs_claim_attempts CHECK(claim_attempts BETWEEN 0 AND 4294967295),
 CONSTRAINT artifact_jobs_retry_count CHECK(retry_count BETWEEN 0 AND 4294967295),
 CONSTRAINT artifact_jobs_failure_class CHECK(failure_class IS NULL OR failure_class IN('retryable','permanent','dead_letter')),
 INDEX artifact_jobs_claim(state,kind,created_at,id),
 INDEX artifact_jobs_lease(state,lease_expires_at)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS branch_observations(
 workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,branch VARCHAR(191) NOT NULL,
 generation BIGINT NOT NULL, desired_commit VARCHAR(64) NOT NULL,updated_at BIGINT NOT NULL,
 PRIMARY KEY(workspace,repo,branch),
 CONSTRAINT branch_observations_generation CHECK(generation>=1)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS artifact_observations(
 workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,branch VARCHAR(191) NOT NULL,kind VARCHAR(16) NOT NULL,
 desired_commit VARCHAR(64) NOT NULL,desired_artifact_id BIGINT NOT NULL,
 desired_generation BIGINT NOT NULL,published_artifact_id BIGINT,
 format_version BIGINT NOT NULL,observed_at BIGINT NOT NULL,
 PRIMARY KEY(workspace,repo,branch,kind),
 CONSTRAINT artifact_observations_generation CHECK(desired_generation>=1),
 CONSTRAINT artifact_observations_format CHECK(format_version BETWEEN 1 AND 4294967295),
 INDEX artifact_observations_desired(desired_artifact_id),
 INDEX artifact_observations_published(published_artifact_id)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS artifact_consumers(
 artifact_id BIGINT NOT NULL,consumer_id VARCHAR(255) NOT NULL,expires_at BIGINT NOT NULL,
 PRIMARY KEY(artifact_id,consumer_id), INDEX artifact_consumers_expiry(expires_at)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS artifact_transport_leases(
 root_hash VARCHAR(64) NOT NULL,session_id VARCHAR(64) NOT NULL,
 workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,expires_at BIGINT NOT NULL,
 PRIMARY KEY(root_hash,session_id), INDEX artifact_transport_leases_expiry(expires_at)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS artifact_base_retention(
 artifact_id BIGINT NOT NULL PRIMARY KEY,workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,
 format_version BIGINT NOT NULL,head_rank SMALLINT,pair_rank SMALLINT,
 CONSTRAINT artifact_base_retention_artifact FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE,
 CONSTRAINT artifact_base_retention_ranks CHECK((head_rank IS NULL OR head_rank BETWEEN 1 AND 8) AND (pair_rank IS NULL OR pair_rank BETWEEN 1 AND 8) AND (head_rank IS NOT NULL OR pair_rank IS NOT NULL)),
 INDEX artifact_base_retention_scope(workspace,repo,format_version)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS artifact_gc_sweep(
 id SMALLINT NOT NULL PRIMARY KEY,owner VARCHAR(200) NOT NULL,expires_at BIGINT NOT NULL,
 CONSTRAINT artifact_gc_sweep_singleton CHECK(id=1)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS scheduler_state(
 id SMALLINT NOT NULL PRIMARY KEY, fairness_cursor BIGINT NOT NULL,
 workspace_cursor VARCHAR(128) NOT NULL DEFAULT '',config_fingerprint VARCHAR(512) NOT NULL DEFAULT '',
 CONSTRAINT scheduler_state_singleton CHECK(id=1),
 CONSTRAINT scheduler_state_fairness CHECK(fairness_cursor BETWEEN 0 AND 3)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS ready_publication_fence_sequence(id SMALLINT NOT NULL PRIMARY KEY,generation BIGINT NOT NULL,CONSTRAINT ready_fence_sequence_singleton CHECK(id=1),CONSTRAINT ready_fence_sequence_generation CHECK(generation>=0)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS ready_publication_fences(token VARCHAR(64) NOT NULL PRIMARY KEY,generation BIGINT NOT NULL UNIQUE,operation_id VARCHAR(96) NOT NULL UNIQUE,workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,branch VARCHAR(191) NOT NULL,target VARCHAR(64) NOT NULL,attempt_id VARCHAR(255) NOT NULL,expires_at BIGINT NOT NULL,state VARCHAR(24) NOT NULL,CONSTRAINT ready_fences_generation CHECK(generation>0),CONSTRAINT ready_fences_state CHECK(state IN('held','activation_unknown')),UNIQUE(token,generation),INDEX ready_publication_fences_recovery(state,generation,token)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS ready_publication_fence_members(token VARCHAR(64) NOT NULL,generation BIGINT NOT NULL,artifact_id BIGINT NOT NULL,manifest LONGTEXT NOT NULL,PRIMARY KEY(token,artifact_id),CONSTRAINT ready_fence_members_generation CHECK(generation>0),CONSTRAINT ready_fence_members_parent FOREIGN KEY(token,generation) REFERENCES ready_publication_fences(token,generation) ON DELETE CASCADE,CONSTRAINT ready_fence_members_artifact FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE CASCADE) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
];

#[derive(Clone)]
pub struct MysqlArtifactScheduler {
    pool: MySqlPool,
    limits: SchedulerLimits,
    verifier: Arc<dyn CompletionVerifier>,
    completion_sealer: Arc<CompletionSealAuthority>,
}
struct MysqlGcDeleteFence(Option<Transaction<'static, MySql>>);
fn validate_mysql_fence(
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
    validate_mysql_identity(&p.workspace, &p.repo, Some(&p.branch))?;
    Ok(())
}
fn mysql_expected(v: &[(i64, Option<String>)]) -> Vec<(i64, Option<String>)> {
    let mut v = v.to_vec();
    v.sort_by_key(|x| x.0);
    v
}
async fn exact_ready_pair_mysql(
    tx: &mut Transaction<'_, MySql>,
    e: &[(i64, Option<String>)],
    p: &ActivationFenceProvenance,
) -> Result<bool> {
    let rows:Vec<(i64,String,String,String,String,i64,Option<String>)>=sqlx::query_as("SELECT id,workspace,repo,commit_oid,kind,format_version,manifest FROM artifact_jobs WHERE id IN(?,?) AND state='ready' FOR UPDATE").bind(e[0].0).bind(e[1].0).fetch_all(&mut **tx).await?;
    if rows.len() != 2 {
        return Ok(false);
    }
    let mut kinds = Vec::new();
    let mut format = None;
    for (id, w, r, c, k, v, m) in rows {
        let want = e.iter().find(|x| x.0 == id).unwrap();
        if w != p.workspace
            || r != p.repo
            || c != p.target
            || m != want.1
            || m.as_deref().is_none_or(|x| x.trim().is_empty())
            || format.is_some_and(|x| x != v)
        {
            return Ok(false);
        }
        format = Some(v);
        kinds.push(k)
    }
    kinds.sort();
    Ok(kinds == ["full_history", "head"])
}
#[async_trait]
impl GcDeleteFence for MysqlGcDeleteFence {
    async fn release(mut self: Box<Self>) -> Result<()> {
        if let Some(tx) = self.0.take() {
            tx.commit().await?;
        }
        Ok(())
    }
}

async fn preflight_mysql_schema(connection: &mut MySqlConnection, version: i64) -> Result<()> {
    let fence_namespace:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND LEFT(table_name,23)='ready_publication_fence'").fetch_one(&mut *connection).await?;
    if version < 6 && fence_namespace != 0 {
        bail!("mysql pre-v6 marker contains Ready fence DDL")
    }
    if version < 2 {
        return Ok(());
    }
    let fence_tables:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')").fetch_one(&mut *connection).await?;
    if version == 6 || fence_tables == 3 {
        let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND ((table_name='ready_publication_fence_sequence' AND column_name IN('id','generation')) OR (table_name='ready_publication_fences' AND column_name IN('token','generation','operation_id','workspace','repo','branch','target','attempt_id','expires_at','state')) OR (table_name='ready_publication_fence_members' AND column_name IN('token','generation','artifact_id','manifest')))").fetch_one(&mut *connection).await?;
        let recovery:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT index_name,GROUP_CONCAT(column_name ORDER BY seq_in_index) columns_csv FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='ready_publication_fences' GROUP BY index_name) i WHERE index_name='ready_publication_fences_recovery' AND columns_csv='state,generation,token'").fetch_one(&mut *connection).await?;
        if fence_tables != 3 || columns != 16 || recovery != 1 {
            bail!("mysql v6 Ready fence schema differs from its marker")
        }
    } else if version == 5 && fence_tables != 0 {
        bail!("mysql v5 marker contains a partial v6 Ready fence schema")
    }
    let tables:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN('artifact_scheduler_schema','artifact_jobs','branch_observations','artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')").fetch_one(&mut *connection).await?;
    if tables != if version == 2 { 7 } else { 9 } {
        bail!("mysql artifact scheduler table inventory does not match schema marker")
    }
    let additions:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN('artifact_base_retention','artifact_gc_sweep')").fetch_one(&mut *connection).await?;
    if version == 2 {
        if additions != 0 {
            bail!("mysql v2 scheduler contains unversioned v3 additions")
        }
        return Ok(());
    }
    let base_columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='artifact_base_retention'").fetch_one(&mut *connection).await?;
    let base_indexes:i64=sqlx::query_scalar("SELECT count(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='artifact_base_retention'").fetch_one(&mut *connection).await?;
    let base_constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='artifact_base_retention' AND constraint_name IN('PRIMARY','artifact_base_retention_artifact','artifact_base_retention_ranks')").fetch_one(&mut *connection).await?;
    let base_scope:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT index_name,GROUP_CONCAT(column_name ORDER BY seq_in_index) columns_csv FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='artifact_base_retention' GROUP BY index_name) indexes WHERE index_name='artifact_base_retention_scope' AND columns_csv='workspace,repo,format_version'").fetch_one(&mut *connection).await?;
    let gc_columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='artifact_gc_sweep'").fetch_one(&mut *connection).await?;
    let gc_constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='artifact_gc_sweep' AND constraint_name IN('PRIMARY','artifact_gc_sweep_singleton')").fetch_one(&mut *connection).await?;
    if additions != 2
        || base_columns != 6
        || base_indexes != 2
        || base_constraints != 3
        || base_scope != 1
        || gc_columns != 3
        || gc_constraints != 2
    {
        bail!("mysql retention/GC schema differs from its v3/v4 marker")
    }
    Ok(())
}

async fn preflight_mysql_transition(connection: &mut MySqlConnection, version: i64) -> Result<()> {
    let fences:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND LEFT(table_name,23)='ready_publication_fence'").fetch_one(&mut *connection).await?;
    if fences != 0 {
        bail!("mysql historical transition contains premature Ready fence DDL")
    }
    let core:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN('artifact_scheduler_schema','artifact_jobs','branch_observations','artifact_observations','artifact_consumers','scheduler_state')").fetch_one(&mut *connection).await?;
    if core != 6 {
        bail!("mysql transitional migration is missing core v1 tables")
    }
    let transport_exists:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='artifact_transport_leases'").fetch_one(&mut *connection).await?;
    if version == V2_TO_V4_TRANSITION && transport_exists != 1 {
        bail!("mysql v2 transition is missing transport leases")
    }
    if transport_exists == 1 {
        let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='artifact_transport_leases'").fetch_one(&mut *connection).await?;
        let indexes:i64=sqlx::query_scalar("SELECT count(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='artifact_transport_leases'").fetch_one(&mut *connection).await?;
        let constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='artifact_transport_leases' AND constraint_type='PRIMARY KEY'").fetch_one(&mut *connection).await?;
        let expiry:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='artifact_transport_leases' AND index_name='artifact_transport_leases_expiry' AND column_name='expires_at' AND seq_in_index=1").fetch_one(&mut *connection).await?;
        if columns != 5 || indexes != 2 || constraints != 1 || expiry != 1 {
            bail!("mysql transitional transport table is malformed")
        }
    }
    let base_exists:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='artifact_base_retention'").fetch_one(&mut *connection).await?;
    if base_exists == 1 {
        let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='artifact_base_retention'").fetch_one(&mut *connection).await?;
        let indexes:i64=sqlx::query_scalar("SELECT count(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='artifact_base_retention'").fetch_one(&mut *connection).await?;
        let constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='artifact_base_retention' AND constraint_name IN('PRIMARY','artifact_base_retention_artifact','artifact_base_retention_ranks')").fetch_one(&mut *connection).await?;
        let scope:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT index_name,GROUP_CONCAT(column_name ORDER BY seq_in_index) columns_csv FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='artifact_base_retention' GROUP BY index_name) indexes WHERE index_name='artifact_base_retention_scope' AND columns_csv='workspace,repo,format_version'").fetch_one(&mut *connection).await?;
        if columns != 6 || indexes != 2 || constraints != 3 || scope != 1 {
            bail!("mysql transitional base-retention table is malformed")
        }
    }
    let gc_exists:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='artifact_gc_sweep'").fetch_one(&mut *connection).await?;
    if gc_exists == 1 {
        let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='artifact_gc_sweep'").fetch_one(&mut *connection).await?;
        let indexes:i64=sqlx::query_scalar("SELECT count(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='artifact_gc_sweep'").fetch_one(&mut *connection).await?;
        let constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='artifact_gc_sweep' AND constraint_name IN('PRIMARY','artifact_gc_sweep_singleton')").fetch_one(&mut *connection).await?;
        if columns != 3 || indexes != 1 || constraints != 2 {
            bail!("mysql transitional GC table is malformed")
        }
    }
    Ok(())
}

async fn preflight_mysql_v6_transition(c: &mut MySqlConnection, marker: i64) -> Result<()> {
    // The transition marker is provenance, not permission to repair a corrupt
    // core. Prove the exact transport-v5 schema before touching fence DDL.
    {
        let mut tx = c.begin().await?;
        MysqlArtifactScheduler::validate_schema(&mut tx).await?;
        tx.rollback().await?;
    }
    let core:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN('artifact_scheduler_schema','artifact_jobs','branch_observations','artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')").fetch_one(&mut *c).await?;
    if core != 9 {
        bail!("mysql v6 transition is missing the exact v5 core")
    }
    let fences:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')").fetch_one(&mut *c).await?;
    let fence_namespace:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND LEFT(table_name,23)='ready_publication_fence'").fetch_one(&mut *c).await?;
    if fence_namespace != fences {
        bail!("mysql v6 transition contains an unexpected Ready fence table")
    }
    if marker == V5_TO_V6_VALIDATED && fences != 3 {
        bail!("mysql validated v6 transition is incomplete")
    }
    for (table, columns, expected_names) in [
        (
            "ready_publication_fence_sequence",
            2_i64,
            "'id','generation'",
        ),
        (
            "ready_publication_fences",
            10,
            "'token','generation','operation_id','workspace','repo','branch','target','attempt_id','expires_at','state'",
        ),
        (
            "ready_publication_fence_members",
            4,
            "'token','generation','artifact_id','manifest'",
        ),
    ] {
        let exists:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?").bind(table).fetch_one(&mut *c).await?;
        if exists == 1 {
            let actual:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=?").bind(table).fetch_one(&mut *c).await?;
            let recognized_sql = format!(
                "SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name IN({expected_names})"
            );
            let recognized: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(recognized_sql))
                .bind(table)
                .fetch_one(&mut *c)
                .await?;
            if actual != columns || recognized != columns {
                bail!("mysql v6 transition contains malformed {table}")
            }
        }
    }
    validate_mysql_v6_prefix(c, fences).await?;
    if fences >= 1 {
        let rows: Vec<(i64, i64)> =
            sqlx::query_as("SELECT id,generation FROM ready_publication_fence_sequence")
                .fetch_all(&mut *c)
                .await?;
        if rows.len() > 1
            || rows
                .first()
                .is_some_and(|(id, generation)| *id != 1 || *generation < 0)
            || (marker == V5_TO_V6_VALIDATED && rows.len() != 1)
        {
            bail!("mysql v6 transition contains invalid fence sequence state")
        }
    }
    if fences >= 2 {
        let live: i64 = sqlx::query_scalar("SELECT count(*) FROM ready_publication_fences")
            .fetch_one(&mut *c)
            .await?;
        if live != 0 {
            bail!("mysql v6 transition contains unprovenanced live fences")
        }
    }
    if fences >= 3 {
        let members: i64 =
            sqlx::query_scalar("SELECT count(*) FROM ready_publication_fence_members")
                .fetch_one(&mut *c)
                .await?;
        if members != 0 {
            bail!("mysql v6 transition contains unprovenanced fence members")
        }
    }
    Ok(())
}

async fn validate_mysql_v6_prefix(c: &mut MySqlConnection, count: i64) -> Result<()> {
    let names = [
        "ready_publication_fence_sequence",
        "ready_publication_fences",
        "ready_publication_fence_members",
    ];
    for (position, table) in names.iter().enumerate() {
        let exists:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=? AND engine='InnoDB' AND table_collation='utf8mb4_bin'").bind(table).fetch_one(&mut *c).await?;
        if (position as i64) < count {
            if exists != 1 {
                bail!("mysql v6 transition fence prefix is not canonical")
            }
        } else if exists != 0 {
            bail!("mysql v6 transition fence DDL is not a prefix")
        }
    }
    let expected: &[(&str, &str)] = &[
        (
            "ready_publication_fence_sequence",
            "id:smallint:NO:|generation:bigint:NO:",
        ),
        (
            "ready_publication_fences",
            "token:varchar(64):NO:utf8mb4_bin|generation:bigint:NO:|operation_id:varchar(96):NO:utf8mb4_bin|workspace:varchar(128):NO:utf8mb4_bin|repo:varchar(320):NO:utf8mb4_bin|branch:varchar(191):NO:utf8mb4_bin|target:varchar(64):NO:utf8mb4_bin|attempt_id:varchar(255):NO:utf8mb4_bin|expires_at:bigint:NO:|state:varchar(24):NO:utf8mb4_bin",
        ),
        (
            "ready_publication_fence_members",
            "token:varchar(64):NO:utf8mb4_bin|generation:bigint:NO:|artifact_id:bigint:NO:|manifest:longtext:NO:utf8mb4_bin",
        ),
    ];
    for (position, (table, want)) in expected.iter().enumerate().take(count as usize) {
        let rows:Vec<(String,String,String,Option<String>,String,Option<String>)>=sqlx::query_as("SELECT column_name,lower(column_type),is_nullable,column_default,extra,collation_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? ORDER BY ordinal_position").bind(table).fetch_all(&mut *c).await?;
        if rows.iter().any(|r| r.3.is_some() || !r.4.is_empty()) {
            bail!("mysql v6 transition column default/extra differs")
        }
        let actual = rows
            .iter()
            .map(|r| format!("{}:{}:{}:{}", r.0, r.1, r.2, r.5.as_deref().unwrap_or("")))
            .collect::<Vec<_>>()
            .join("|");
        if actual != *want {
            bail!("mysql v6 transition column shape differs: {table}")
        }
        let constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name=?").bind(table).fetch_one(&mut *c).await?;
        let indexes:i64=sqlx::query_scalar("SELECT count(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=?").bind(table).fetch_one(&mut *c).await?;
        let (want_constraints, want_indexes) = match position {
            0 => (3, 1),
            1 => (6, 5),
            _ => (4, 3),
        };
        if constraints != want_constraints || indexes != want_indexes {
            bail!("mysql v6 transition constraint/index inventory differs: {table}")
        }
        let actual_indexes: Vec<(i64, String)> = sqlx::query_as(
            "SELECT non_unique,GROUP_CONCAT(column_name ORDER BY seq_in_index)
             FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=?
             GROUP BY index_name,non_unique ORDER BY non_unique,GROUP_CONCAT(column_name ORDER BY seq_in_index)",
        )
        .bind(table)
        .fetch_all(&mut *c)
        .await?;
        let wanted_indexes: Vec<(i64, String)> = match position {
            0 => vec![(0, "id".into())],
            1 => vec![
                (0, "generation".into()),
                (0, "operation_id".into()),
                (0, "token".into()),
                (0, "token,generation".into()),
                (1, "state,generation,token".into()),
            ],
            _ => vec![
                (0, "token,artifact_id".into()),
                (1, "artifact_id".into()),
                (1, "token,generation".into()),
            ],
        };
        if actual_indexes != wanted_indexes {
            bail!("mysql v6 transition index definitions differ: {table}")
        }
    }
    if count >= 1 {
        for (name, clause) in [
            ("ready_fence_sequence_singleton", "`id` = 1"),
            ("ready_fence_sequence_generation", "`generation` >= 0"),
        ] {
            let actual:Option<String>=sqlx::query_scalar("SELECT lower(check_clause) FROM information_schema.check_constraints WHERE constraint_schema=DATABASE() AND constraint_name=?").bind(name).fetch_optional(&mut *c).await?;
            if actual.as_deref().map(normalize_check).as_deref()
                != Some(normalize_check(clause).as_str())
            {
                bail!("mysql v6 transition check differs: {name}")
            }
        }
    }
    if count >= 2 {
        for (name, clause) in [
            ("ready_fences_generation", "`generation` > 0"),
            (
                "ready_fences_state",
                "`state` in ('held','activation_unknown')",
            ),
        ] {
            let actual:Option<String>=sqlx::query_scalar("SELECT lower(check_clause) FROM information_schema.check_constraints WHERE constraint_schema=DATABASE() AND constraint_name=?").bind(name).fetch_optional(&mut *c).await?;
            if actual.as_deref().map(normalize_check).as_deref()
                != Some(normalize_check(clause).as_str())
            {
                bail!("mysql v6 transition check differs: {name}")
            }
        }
    }
    if count >= 3 {
        let check:Option<String>=sqlx::query_scalar("SELECT lower(check_clause) FROM information_schema.check_constraints WHERE constraint_schema=DATABASE() AND constraint_name='ready_fence_members_generation'").fetch_optional(&mut *c).await?;
        if check.as_deref().map(normalize_check).as_deref()
            != Some(normalize_check("`generation` > 0").as_str())
        {
            bail!("mysql v6 transition member check differs")
        }
        let parent:i64=sqlx::query_scalar("SELECT count(DISTINCT r.constraint_name) FROM information_schema.referential_constraints r JOIN information_schema.key_column_usage k ON k.constraint_schema=r.constraint_schema AND k.table_name=r.table_name AND k.constraint_name=r.constraint_name WHERE r.constraint_schema=DATABASE() AND r.table_name='ready_publication_fence_members' AND r.constraint_name='ready_fence_members_parent' AND r.referenced_table_name='ready_publication_fences' AND r.delete_rule='CASCADE' AND ((k.ordinal_position=1 AND k.column_name='token' AND k.referenced_column_name='token') OR (k.ordinal_position=2 AND k.column_name='generation' AND k.referenced_column_name='generation')) GROUP BY r.constraint_name HAVING count(*)=2").fetch_optional(&mut *c).await?.unwrap_or(0);
        let artifact:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.referential_constraints r JOIN information_schema.key_column_usage k ON k.constraint_schema=r.constraint_schema AND k.table_name=r.table_name AND k.constraint_name=r.constraint_name WHERE r.constraint_schema=DATABASE() AND r.table_name='ready_publication_fence_members' AND r.constraint_name='ready_fence_members_artifact' AND r.referenced_table_name='artifact_jobs' AND r.delete_rule='CASCADE' AND k.column_name='artifact_id' AND k.referenced_column_name='id'").fetch_one(&mut *c).await?;
        if parent != 1 || artifact != 1 {
            bail!("mysql v6 transition foreign keys differ")
        }
    }
    Ok(())
}

async fn validate_mysql_v6_fences(tx: &mut Transaction<'_, MySql>) -> Result<()> {
    let tables: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members') AND engine='InnoDB' AND table_collation='utf8mb4_bin'").fetch_one(&mut **tx).await?;
    let namespace: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND LEFT(table_name,23)='ready_publication_fence'").fetch_one(&mut **tx).await?;
    if tables != 3 || namespace != 3 {
        bail!("mysql v6 Ready fence table inventory is not exact")
    }

    const COLUMNS: &[(&str, &str, &str, &str, Option<&str>)] = &[
        (
            "ready_publication_fence_sequence",
            "id",
            "smallint",
            "NO",
            None,
        ),
        (
            "ready_publication_fence_sequence",
            "generation",
            "bigint",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "token",
            "varchar(64)",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "generation",
            "bigint",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "operation_id",
            "varchar(96)",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "workspace",
            "varchar(128)",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "repo",
            "varchar(320)",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "branch",
            "varchar(191)",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "target",
            "varchar(64)",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "attempt_id",
            "varchar(255)",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "expires_at",
            "bigint",
            "NO",
            None,
        ),
        (
            "ready_publication_fences",
            "state",
            "varchar(24)",
            "NO",
            None,
        ),
        (
            "ready_publication_fence_members",
            "token",
            "varchar(64)",
            "NO",
            None,
        ),
        (
            "ready_publication_fence_members",
            "generation",
            "bigint",
            "NO",
            None,
        ),
        (
            "ready_publication_fence_members",
            "artifact_id",
            "bigint",
            "NO",
            None,
        ),
        (
            "ready_publication_fence_members",
            "manifest",
            "longtext",
            "NO",
            None,
        ),
    ];
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')").fetch_one(&mut **tx).await?;
    if count != COLUMNS.len() as i64 {
        bail!("mysql v6 Ready fence columns are not exact")
    }
    for (table, column, kind, nullable, default) in COLUMNS {
        let row:Option<(String,String,Option<String>,String,Option<String>)>=sqlx::query_as("SELECT lower(column_type),is_nullable,column_default,extra,collation_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?").bind(table).bind(column).fetch_optional(&mut **tx).await?;
        let Some((actual_kind, actual_nullable, actual_default, extra, collation)) = row else {
            bail!("mysql v6 Ready fence column is missing")
        };
        if actual_kind != *kind
            || actual_nullable != *nullable
            || actual_default.as_deref() != *default
            || !extra.is_empty()
            || ((kind.contains("char") || kind.contains("text"))
                && collation.as_deref() != Some("utf8mb4_bin"))
        {
            bail!("mysql v6 Ready fence column definition differs: {table}.{column}")
        }
    }
    let indexes:Vec<(String,String,i64,String)>=sqlx::query_as("SELECT table_name,index_name,non_unique,GROUP_CONCAT(column_name ORDER BY seq_in_index) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members') GROUP BY table_name,index_name,non_unique ORDER BY table_name,index_name").fetch_all(&mut **tx).await?;
    if indexes.len() != 9
        || !indexes.iter().any(|x| {
            x.0 == "ready_publication_fence_sequence" && x.1 == "PRIMARY" && x.2 == 0 && x.3 == "id"
        })
        || !indexes.iter().any(|x| {
            x.0 == "ready_publication_fences"
                && x.1 == "ready_publication_fences_recovery"
                && x.2 == 1
                && x.3 == "state,generation,token"
        })
        || indexes
            .iter()
            .filter(|x| {
                x.0 == "ready_publication_fences"
                    && x.2 == 0
                    && matches!(
                        x.3.as_str(),
                        "token" | "generation" | "operation_id" | "token,generation"
                    )
            })
            .count()
            != 4
        || indexes
            .iter()
            .filter(|x| {
                x.0 == "ready_publication_fence_members"
                    && ((x.2 == 0 && x.3 == "token,artifact_id")
                        || (x.2 == 1 && matches!(x.3.as_str(), "token,generation" | "artifact_id")))
            })
            .count()
            != 3
    {
        bail!("mysql v6 Ready fence index inventory differs")
    }
    let constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')").fetch_one(&mut **tx).await?;
    if constraints != 13 {
        bail!("mysql v6 Ready fence constraint inventory differs")
    }
    let parent_fk:i64=sqlx::query_scalar("SELECT count(DISTINCT r.constraint_name) FROM information_schema.referential_constraints r JOIN information_schema.key_column_usage k ON k.constraint_schema=r.constraint_schema AND k.table_name=r.table_name AND k.constraint_name=r.constraint_name WHERE r.constraint_schema=DATABASE() AND r.table_name='ready_publication_fence_members' AND r.constraint_name='ready_fence_members_parent' AND r.referenced_table_name='ready_publication_fences' AND r.delete_rule='CASCADE' AND ((k.ordinal_position=1 AND k.column_name='token' AND k.referenced_column_name='token') OR (k.ordinal_position=2 AND k.column_name='generation' AND k.referenced_column_name='generation')) GROUP BY r.constraint_name HAVING count(*)=2").fetch_optional(&mut **tx).await?.unwrap_or(0);
    let artifact_fk:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.referential_constraints r JOIN information_schema.key_column_usage k ON k.constraint_schema=r.constraint_schema AND k.table_name=r.table_name AND k.constraint_name=r.constraint_name WHERE r.constraint_schema=DATABASE() AND r.table_name='ready_publication_fence_members' AND r.constraint_name='ready_fence_members_artifact' AND r.referenced_table_name='artifact_jobs' AND r.delete_rule='CASCADE' AND k.column_name='artifact_id' AND k.referenced_column_name='id'").fetch_one(&mut **tx).await?;
    if parent_fk != 1 || artifact_fk != 1 {
        bail!("mysql v6 Ready fence foreign keys differ")
    }
    for (name, clause) in [
        ("ready_fence_sequence_singleton", "`id` = 1"),
        ("ready_fence_sequence_generation", "`generation` >= 0"),
        ("ready_fences_generation", "`generation` > 0"),
        (
            "ready_fences_state",
            "`state` in ('held','activation_unknown')",
        ),
        ("ready_fence_members_generation", "`generation` > 0"),
    ] {
        let actual:Option<String>=sqlx::query_scalar("SELECT lower(check_clause) FROM information_schema.check_constraints WHERE constraint_schema=DATABASE() AND constraint_name=?").bind(name).fetch_optional(&mut **tx).await?;
        if actual.as_deref().map(normalize_check).as_deref()
            != Some(normalize_check(clause).as_str())
        {
            bail!("mysql v6 Ready fence check differs: {name}")
        }
    }
    let sequence: Vec<(i64, i64)> =
        sqlx::query_as("SELECT id,generation FROM ready_publication_fence_sequence")
            .fetch_all(&mut **tx)
            .await?;
    if sequence.len() != 1 || sequence[0].0 != 1 || sequence[0].1 < 0 {
        bail!("mysql v6 Ready fence sequence is not the singleton")
    }
    let max_generation: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(generation),0) FROM ready_publication_fences")
            .fetch_one(&mut **tx)
            .await?;
    if sequence[0].1 < max_generation {
        bail!("mysql v6 Ready fence sequence is behind persisted generations")
    }
    let fences:Vec<(String,i64,String,String,String,String,String,String,i64,String)>=sqlx::query_as("SELECT token,generation,operation_id,workspace,repo,branch,target,attempt_id,expires_at,state FROM ready_publication_fences").fetch_all(&mut **tx).await?;
    let members:Vec<(String,i64,i64,String,String,String,String,String,i64,String,Option<String>)>=sqlx::query_as("SELECT m.token,m.generation,m.artifact_id,m.manifest,j.workspace,j.repo,j.commit_oid,j.kind,j.format_version,j.state,j.manifest FROM ready_publication_fence_members m LEFT JOIN artifact_jobs j ON j.id=m.artifact_id ORDER BY m.token,m.artifact_id").fetch_all(&mut **tx).await?;
    let mut by_token: HashMap<
        &str,
        Vec<&(
            String,
            i64,
            i64,
            String,
            String,
            String,
            String,
            String,
            i64,
            String,
            Option<String>,
        )>,
    > = HashMap::new();
    for member in &members {
        by_token.entry(&member.0).or_default().push(member);
    }
    for (token, generation, operation, workspace, repo, branch, target, attempt, expires, state) in
        fences
    {
        let provenance = ActivationFenceProvenance {
            workspace: workspace.clone(),
            repo: repo.clone(),
            branch: branch.clone(),
            target: target.clone(),
            attempt_id: attempt.clone(),
        };
        let rows = by_token.remove(token.as_str()).unwrap_or_default();
        let mut kinds = rows.iter().map(|row| row.7.as_str()).collect::<Vec<_>>();
        kinds.sort();
        if token.len() != 64
            || !token
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || generation <= 0
            || operation != provenance.operation_id()
            || expires <= 0
            || !matches!(state.as_str(), "held" | "activation_unknown")
            || rows.len() != 2
            || rows.iter().any(|row| {
                row.1 != generation
                    || row.4 != workspace
                    || row.5 != repo
                    || row.6 != target
                    || row.9 != "ready"
                    || row.10.as_deref() != Some(row.3.as_str())
                    || row.3.trim().is_empty()
            })
            || rows[0].8 != rows[1].8
            || kinds != ["full_history", "head"]
        {
            bail!("mysql v6 Ready fence persisted state is invalid")
        }
    }
    if !by_token.is_empty() {
        bail!("mysql v6 Ready fence contains orphan members")
    }
    Ok(())
}

async fn validate_mysql_v7_limits_capability(
    c: &mut MySqlConnection,
    required: bool,
) -> Result<()> {
    let row:Option<(String,String,Option<String>,Option<String>,String)>=sqlx::query_as("SELECT lower(column_type),is_nullable,column_default,collation_name,extra FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='scheduler_state' AND column_name='limits_fingerprint'").fetch_optional(&mut *c).await?;
    if row.is_some()
        && row
            != Some((
                "varchar(64)".into(),
                "NO".into(),
                Some("".into()),
                Some("utf8mb4_bin".into()),
                "".into(),
            ))
    {
        bail!("mysql v7 scheduler limits capability differs")
    }
    if required && row.is_none() {
        bail!("mysql v7 scheduler limits capability is absent")
    }
    Ok(())
}

impl MysqlArtifactScheduler {
    pub async fn from_pool(
        pool: MySqlPool,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        validate_limits(&limits)?;
        let verifier_id = verifier.identity().trim();
        if verifier_id.is_empty() {
            bail!("completion verifier identity is empty")
        }

        // A named migration lock must live on one physical connection. Detach
        // it from the pool so cancellation/drop closes the socket and cannot
        // leak a session-scoped GET_LOCK into an unrelated future checkout.
        let mut connection = pool.acquire().await?.detach();
        let locked: i64 =
            sqlx::query_scalar("SELECT GET_LOCK('ripclone_artifact_scheduler_v1',30)")
                .fetch_one(&mut connection)
                .await?;
        if locked != 1 {
            bail!("timed out acquiring mysql artifact scheduler migration lock")
        }
        let initialized: Result<()> = async {
            let marker_exists:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='artifact_scheduler_schema'").fetch_one(&mut connection).await?;
            let mut version: i64 = if marker_exists == 0 {
                let partial:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND (table_name IN('artifact_jobs','branch_observations','artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state','branch_source_generations','branch_source_current','artifact_intents') OR table_name LIKE 'git\\_source\\_%' ESCAPE '\\\\')").fetch_one(&mut connection).await?;
                if partial != 0 { bail!("unmarked partial mysql artifact scheduler schema") }
                // Fresh databases first publish the exact transport-only v5
                // shape. Fence DDL is forbidden until marker 501 is durable.
                for statement in &SCHEMA[..9] { sqlx::raw_sql(*statement).execute(&mut connection).await?; }
                sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)").execute(&mut connection).await?;
                sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,5)").execute(&mut connection).await?;
                5
            } else {
                let version:i64=sqlx::query_scalar("SELECT version FROM artifact_scheduler_schema WHERE id=1").fetch_one(&mut connection).await?;
                if version > SCHEMA_VERSION && ![V1_TO_V4_TRANSITION,V2_TO_V4_TRANSITION,V5_TO_V6_DDL,V5_TO_V6_VALIDATED,V6_TO_V7_DDL,V6_TO_V7_VALIDATED].contains(&version) { bail!("artifact scheduler database is newer than this binary") }
                if ![1,2,3,4,5,6,SCHEMA_VERSION,V1_TO_V4_TRANSITION,V2_TO_V4_TRANSITION,V5_TO_V6_DDL,V5_TO_V6_VALIDATED,V6_TO_V7_DDL,V6_TO_V7_VALIDATED].contains(&version) { bail!("unsupported mysql artifact scheduler schema {version}") }
                if [V1_TO_V4_TRANSITION,V2_TO_V4_TRANSITION].contains(&version) {
                    preflight_mysql_transition(&mut connection, version).await?;
                } else if [V5_TO_V6_DDL,V5_TO_V6_VALIDATED].contains(&version) {
                    preflight_mysql_v6_transition(&mut connection,version).await?;
                } else if [V6_TO_V7_DDL,V6_TO_V7_VALIDATED].contains(&version) {
                    let mut exact=connection.begin().await?; Self::validate_schema(&mut exact).await?; validate_mysql_v6_fences(&mut exact).await?; exact.rollback().await?;
                    validate_mysql_v7_limits_capability(&mut connection,version==V6_TO_V7_VALIDATED).await?;
                    crate::git_source_registry::validate_mysql_v7_prefix(&mut connection,version==V6_TO_V7_VALIDATED).await?;
                } else {
                    preflight_mysql_schema(&mut connection,version).await?;
                }
                if version == 3 || version == 4 || version == 5 {
                    for statement in &SCHEMA[..9] { sqlx::raw_sql(*statement).execute(&mut connection).await?; }
                    sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0) ON DUPLICATE KEY UPDATE id=VALUES(id)").execute(&mut connection).await?;
                }
                version
            };

            // Released v1-v4 lineages, including the durable 401/402 crash
            // markers, converge to the exact released v5 shape first. No fence
            // table is mentioned by this phase.
            if version == 1 || version == 2 {
                let transition = if version == 1 { V1_TO_V4_TRANSITION } else { V2_TO_V4_TRANSITION };
                let mut marker = connection.begin().await?;
                sqlx::query(
                    "UPDATE artifact_scheduler_schema SET version=? WHERE id=1 AND version=?",
                )
                .bind(transition)
                .bind(version)
                .execute(&mut *marker)
                .await?;
                marker.commit().await?;
                version = transition;
            }
            if version == V1_TO_V4_TRANSITION || version == V2_TO_V4_TRANSITION {
                for statement in &SCHEMA[..9] { sqlx::raw_sql(*statement).execute(&mut connection).await?; }
                sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0) ON DUPLICATE KEY UPDATE id=VALUES(id)").execute(&mut connection).await?;
                let mut migration = connection.begin().await?;
                Self::validate_schema(&mut migration).await?;
                sqlx::query("DELETE FROM artifact_base_retention").execute(&mut *migration).await?;
                let scopes: Vec<(String,String,i64)> = sqlx::query_as("SELECT DISTINCT workspace,repo,format_version FROM artifact_jobs WHERE state='ready' AND kind IN('head','full_history')").fetch_all(&mut *migration).await?;
                for (w,r,v) in scopes { refresh_base_retention(&mut migration,&w,&r,v).await?; }
                let changed=sqlx::query("UPDATE artifact_scheduler_schema SET version=5 WHERE id=1 AND version=?").bind(version).execute(&mut *migration).await?.rows_affected();
                if changed != 1 { bail!("mysql transition marker changed during resumed v5 publication") }
                migration.commit().await?;
                version = 5;
            } else if version == 3 || version == 4 {
                let mut migration = connection.begin().await?;
                Self::validate_schema(&mut migration).await?;
                let changed=sqlx::query("UPDATE artifact_scheduler_schema SET version=5 WHERE id=1 AND version=?").bind(version).execute(&mut *migration).await?.rows_affected();
                if changed != 1 { bail!("mysql v3/v4 marker changed during v5 publication") }
                migration.commit().await?;
                version = 5;
            }

            // A crash here leaves exact v5, which old v5 understands. Persist
            // 501 before the first fence DDL, then resume 501 -> 502 -> 6.
            if version == 5 {
                let mut marker=connection.begin().await?;
                if sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1 AND version=5").bind(V5_TO_V6_DDL).execute(&mut *marker).await?.rows_affected()!=1{bail!("mysql v5 to v6 transition marker CAS failed")}
                marker.commit().await?; version=V5_TO_V6_DDL;
            }
            if version == V5_TO_V6_DDL {
                for statement in &SCHEMA[9..] { sqlx::raw_sql(*statement).execute(&mut connection).await?; }
                sqlx::query("INSERT INTO ready_publication_fence_sequence(id,generation) VALUES(1,0) ON DUPLICATE KEY UPDATE id=VALUES(id)").execute(&mut connection).await?;
                preflight_mysql_v6_transition(&mut connection,V5_TO_V6_VALIDATED).await?;
                let mut exact=connection.begin().await?; validate_mysql_v6_fences(&mut exact).await?; exact.rollback().await?;
                let mut marker=connection.begin().await?;
                if sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1 AND version=?").bind(V5_TO_V6_VALIDATED).bind(V5_TO_V6_DDL).execute(&mut *marker).await?.rows_affected()!=1{bail!("mysql v6 validated marker CAS failed")}
                marker.commit().await?; version=V5_TO_V6_VALIDATED;
            }
            if version == V5_TO_V6_VALIDATED {
                preflight_mysql_v6_transition(&mut connection,V5_TO_V6_VALIDATED).await?;
                let mut exact=connection.begin().await?; validate_mysql_v6_fences(&mut exact).await?; exact.rollback().await?;
                let mut marker=connection.begin().await?;
                if sqlx::query("UPDATE artifact_scheduler_schema SET version=6 WHERE id=1 AND version=?").bind(V5_TO_V6_VALIDATED).execute(&mut *marker).await?.rows_affected()!=1{bail!("mysql v6 publication marker CAS failed")}
                marker.commit().await?; version=6;
            }

            if version==6 {
                let mut exact=connection.begin().await?;Self::validate_schema(&mut exact).await?;validate_mysql_v6_fences(&mut exact).await?;exact.rollback().await?;
                validate_mysql_v7_limits_capability(&mut connection,false).await?;
                let premature:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='scheduler_state' AND column_name='limits_fingerprint'").fetch_one(&mut connection).await?;if premature!=0{bail!("mysql v6 contains premature v7 limits capability")}
                crate::git_source_registry::validate_mysql_v7_prefix(&mut connection,false).await?;
                let mut marker=connection.begin().await?;
                if sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1 AND version=6").bind(V6_TO_V7_DDL).execute(&mut *marker).await?.rows_affected()!=1{bail!("mysql v7 transition marker CAS failed")}
                marker.commit().await?;version=V6_TO_V7_DDL;
            }
            if version==V6_TO_V7_DDL {
                validate_mysql_v7_limits_capability(&mut connection,false).await?;
                let limits_column:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='scheduler_state' AND column_name='limits_fingerprint'").fetch_one(&mut connection).await?;
                if limits_column==0{sqlx::raw_sql("ALTER TABLE scheduler_state ADD COLUMN limits_fingerprint VARCHAR(64) NOT NULL DEFAULT ''").execute(&mut connection).await?;}
                validate_mysql_v7_limits_capability(&mut connection,true).await?;
                let durable_limits=scheduler_limits_fingerprint(&limits);
                let existing:String=sqlx::query_scalar("SELECT limits_fingerprint FROM scheduler_state WHERE id=1").fetch_one(&mut connection).await?;
                if existing.is_empty(){sqlx::query("UPDATE scheduler_state SET limits_fingerprint=? WHERE id=1 AND limits_fingerprint=''").bind(&durable_limits).execute(&mut connection).await?;}else if existing!=durable_limits{bail!("mysql scheduler limits capability differs during v7 migration")}
                for ddl in crate::git_source_registry::MYSQL_V7_TABLES {crate::git_source_registry::validate_mysql_v7_prefix(&mut connection,false).await?;sqlx::raw_sql(*ddl).execute(&mut connection).await?;}
                crate::git_source_registry::validate_mysql_v7_prefix(&mut connection,true).await?;
                sqlx::query("INSERT INTO git_source_acquisition_sequence(id,generation) VALUES(1,0) ON DUPLICATE KEY UPDATE id=VALUES(id)").execute(&mut connection).await?;
                sqlx::query("INSERT INTO git_source_maintenance(id) VALUES(1) ON DUPLICATE KEY UPDATE id=VALUES(id)").execute(&mut connection).await?;
                crate::git_source_registry::validate_mysql_v7_state(&mut connection).await?;
                let mut marker=connection.begin().await?;
                if sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1 AND version=?").bind(V6_TO_V7_VALIDATED).bind(V6_TO_V7_DDL).execute(&mut *marker).await?.rows_affected()!=1{bail!("mysql v7 validated marker CAS failed")}
                marker.commit().await?;version=V6_TO_V7_VALIDATED;
            }
            if version==V6_TO_V7_VALIDATED {
                validate_mysql_v7_limits_capability(&mut connection,true).await?;
                let stored_limits:String=sqlx::query_scalar("SELECT limits_fingerprint FROM scheduler_state WHERE id=1").fetch_one(&mut connection).await?;if stored_limits!=scheduler_limits_fingerprint(&limits){bail!("mysql scheduler limits capability differs before v7 publication")}
                crate::git_source_registry::validate_mysql_v7_prefix(&mut connection,true).await?;crate::git_source_registry::validate_mysql_v7_state(&mut connection).await?;
                let mut marker=connection.begin().await?;
                if sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1 AND version=?").bind(SCHEMA_VERSION).bind(V6_TO_V7_VALIDATED).execute(&mut *marker).await?.rows_affected()!=1{bail!("mysql v7 publication marker CAS failed")}
                marker.commit().await?;version=SCHEMA_VERSION;
            }

            let mut migration = connection.begin().await?;
            let locked_version: i64 = sqlx::query_scalar("SELECT version FROM artifact_scheduler_schema WHERE id=1 FOR UPDATE").fetch_one(&mut *migration).await?;
            if locked_version != version || version != SCHEMA_VERSION { bail!("mysql artifact scheduler version changed under migration lock") }
            Self::validate_schema(&mut migration).await?;
            validate_mysql_v6_fences(&mut migration).await?;
            migration.rollback().await?;
            crate::git_source_registry::validate_mysql_v7_prefix(&mut connection,true).await?;
            crate::git_source_registry::validate_mysql_v7_state(&mut connection).await?;
            validate_mysql_v7_limits_capability(&mut connection,true).await?;
            let stored_limits:String=sqlx::query_scalar("SELECT limits_fingerprint FROM scheduler_state WHERE id=1").fetch_one(&mut connection).await?;if stored_limits!=scheduler_limits_fingerprint(&limits){bail!("mysql scheduler limits capability differs from fleet")}
            let mut migration=connection.begin().await?;

            let fingerprint = scheduler_fingerprint(&limits, verifier_id);
            if fingerprint.chars().count() > 512 {
                bail!("scheduler verifier/config fingerprint exceeds mysql storage limit")
            }
            let stored: String = sqlx::query_scalar(
                "SELECT config_fingerprint FROM scheduler_state WHERE id=1 FOR UPDATE",
            )
            .fetch_one(&mut *migration)
            .await?;
            if stored.is_empty() {
                let existing_state: i64 = sqlx::query_scalar(
                    "SELECT (SELECT count(*) FROM artifact_jobs)
                          + (SELECT count(*) FROM branch_observations)
                          + (SELECT count(*) FROM artifact_observations)
                          + (SELECT count(*) FROM artifact_consumers)
                          + (SELECT count(*) FROM artifact_transport_leases)",
                )
                .fetch_one(&mut *migration)
                .await?;
                if existing_state != 0 {
                    bail!(
                        "cannot establish scheduler verifier/config fingerprint over existing state"
                    )
                }
                let adopted = sqlx::query(
                    "UPDATE scheduler_state SET config_fingerprint=?
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
            Ok(())
        }
        .await;
        // Best-effort explicit release; dropping the detached physical
        // connection below is the fail-safe if this query itself fails.
        let release = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT RELEASE_LOCK('ripclone_artifact_scheduler_v1')",
        )
        .fetch_one(&mut connection)
        .await;
        initialized?;
        if release?.unwrap_or(0) != 1 {
            bail!("mysql artifact scheduler migration lock ownership was lost")
        }
        let completion_sealer = Arc::new(CompletionSealAuthority::new(verifier.identity())?);
        Ok(Self {
            pool,
            limits,
            verifier,
            completion_sealer,
        })
    }

    async fn validate_schema(tx: &mut Transaction<'_, MySql>) -> Result<()> {
        // Validate every column's storage type, nullability and default. This
        // rejects tables that merely share our names but silently truncate or
        // reinterpret scheduler identities and counters.
        const COLUMNS: &[(&str, &str, &str, &str, Option<&str>)] = &[
            ("artifact_scheduler_schema", "id", "smallint", "NO", None),
            ("artifact_scheduler_schema", "version", "bigint", "NO", None),
            ("artifact_jobs", "id", "bigint", "NO", None),
            ("artifact_jobs", "workspace", "varchar(128)", "NO", None),
            ("artifact_jobs", "repo", "varchar(320)", "NO", None),
            ("artifact_jobs", "commit_oid", "varchar(64)", "NO", None),
            ("artifact_jobs", "kind", "varchar(16)", "NO", None),
            ("artifact_jobs", "format_version", "bigint", "NO", None),
            ("artifact_jobs", "state", "varchar(16)", "NO", None),
            ("artifact_jobs", "owner", "varchar(255)", "YES", None),
            ("artifact_jobs", "heartbeat_at", "bigint", "YES", None),
            ("artifact_jobs", "lease_expires_at", "bigint", "YES", None),
            (
                "artifact_jobs",
                "lease_generation",
                "bigint",
                "NO",
                Some("0"),
            ),
            ("artifact_jobs", "claim_attempts", "bigint", "NO", Some("0")),
            ("artifact_jobs", "retry_count", "bigint", "NO", Some("0")),
            ("artifact_jobs", "manifest", "longtext", "YES", None),
            ("artifact_jobs", "error", "longtext", "YES", None),
            ("artifact_jobs", "failure_class", "varchar(16)", "YES", None),
            ("artifact_jobs", "created_at", "bigint", "NO", None),
            ("artifact_jobs", "updated_at", "bigint", "NO", None),
            (
                "branch_observations",
                "workspace",
                "varchar(128)",
                "NO",
                None,
            ),
            ("branch_observations", "repo", "varchar(320)", "NO", None),
            ("branch_observations", "branch", "varchar(191)", "NO", None),
            ("branch_observations", "generation", "bigint", "NO", None),
            (
                "branch_observations",
                "desired_commit",
                "varchar(64)",
                "NO",
                None,
            ),
            ("branch_observations", "updated_at", "bigint", "NO", None),
            (
                "artifact_observations",
                "workspace",
                "varchar(128)",
                "NO",
                None,
            ),
            ("artifact_observations", "repo", "varchar(320)", "NO", None),
            (
                "artifact_observations",
                "branch",
                "varchar(191)",
                "NO",
                None,
            ),
            ("artifact_observations", "kind", "varchar(16)", "NO", None),
            (
                "artifact_observations",
                "desired_commit",
                "varchar(64)",
                "NO",
                None,
            ),
            (
                "artifact_observations",
                "desired_artifact_id",
                "bigint",
                "NO",
                None,
            ),
            (
                "artifact_observations",
                "desired_generation",
                "bigint",
                "NO",
                None,
            ),
            (
                "artifact_observations",
                "published_artifact_id",
                "bigint",
                "YES",
                None,
            ),
            (
                "artifact_observations",
                "format_version",
                "bigint",
                "NO",
                None,
            ),
            ("artifact_observations", "observed_at", "bigint", "NO", None),
            ("artifact_consumers", "artifact_id", "bigint", "NO", None),
            (
                "artifact_consumers",
                "consumer_id",
                "varchar(255)",
                "NO",
                None,
            ),
            ("artifact_consumers", "expires_at", "bigint", "NO", None),
            (
                "artifact_transport_leases",
                "root_hash",
                "varchar(64)",
                "NO",
                None,
            ),
            (
                "artifact_transport_leases",
                "session_id",
                "varchar(64)",
                "NO",
                None,
            ),
            (
                "artifact_transport_leases",
                "workspace",
                "varchar(128)",
                "NO",
                None,
            ),
            (
                "artifact_transport_leases",
                "repo",
                "varchar(320)",
                "NO",
                None,
            ),
            (
                "artifact_transport_leases",
                "expires_at",
                "bigint",
                "NO",
                None,
            ),
            (
                "artifact_base_retention",
                "artifact_id",
                "bigint",
                "NO",
                None,
            ),
            (
                "artifact_base_retention",
                "workspace",
                "varchar(128)",
                "NO",
                None,
            ),
            (
                "artifact_base_retention",
                "repo",
                "varchar(320)",
                "NO",
                None,
            ),
            (
                "artifact_base_retention",
                "format_version",
                "bigint",
                "NO",
                None,
            ),
            (
                "artifact_base_retention",
                "head_rank",
                "smallint",
                "YES",
                None,
            ),
            (
                "artifact_base_retention",
                "pair_rank",
                "smallint",
                "YES",
                None,
            ),
            ("artifact_gc_sweep", "id", "smallint", "NO", None),
            ("artifact_gc_sweep", "owner", "varchar(200)", "NO", None),
            ("artifact_gc_sweep", "expires_at", "bigint", "NO", None),
            ("scheduler_state", "id", "smallint", "NO", None),
            ("scheduler_state", "fairness_cursor", "bigint", "NO", None),
            (
                "scheduler_state",
                "workspace_cursor",
                "varchar(128)",
                "NO",
                Some(""),
            ),
            (
                "scheduler_state",
                "config_fingerprint",
                "varchar(512)",
                "NO",
                Some(""),
            ),
        ];
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema=DATABASE() AND table_name IN
             ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')",
        )
        .fetch_one(&mut **tx)
        .await?;
        let limits_column:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='scheduler_state' AND column_name='limits_fingerprint'").fetch_one(&mut **tx).await?;
        if count != COLUMNS.len() as i64 + limits_column {
            bail!("mysql artifact scheduler schema has unexpected or missing columns")
        }
        if limits_column == 1 {
            let shape:Option<(String,String,Option<String>,Option<String>)>=sqlx::query_as("SELECT lower(column_type),is_nullable,column_default,collation_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='scheduler_state' AND column_name='limits_fingerprint'").fetch_optional(&mut **tx).await?;
            if shape
                != Some((
                    "varchar(64)".into(),
                    "NO".into(),
                    Some("".into()),
                    Some("utf8mb4_bin".into()),
                ))
            {
                bail!("mysql scheduler limits capability column differs")
            }
        }
        let invalid_tables: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.tables
             WHERE table_schema=DATABASE() AND table_name IN
             ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')
               AND (engine IS NULL OR engine<>'InnoDB' OR table_collation IS NULL
                    OR table_collation<>'utf8mb4_bin')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_tables != 0 {
            bail!("mysql artifact scheduler requires InnoDB and utf8mb4_bin tables")
        }
        let invalid_collations: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema=DATABASE() AND table_name IN
             ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')
               AND data_type IN('varchar','text','longtext')
               AND (collation_name IS NULL OR collation_name<>'utf8mb4_bin')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_collations != 0 {
            bail!("mysql artifact scheduler text columns require binary collation")
        }
        let id_extra: String = sqlx::query_scalar(
            "SELECT extra FROM information_schema.columns WHERE table_schema=DATABASE()
               AND table_name='artifact_jobs' AND column_name='id'",
        )
        .fetch_one(&mut **tx)
        .await?;
        if id_extra != "auto_increment" {
            bail!("mysql artifact scheduler id is not auto_increment")
        }
        let invalid_extra: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema=DATABASE() AND table_name IN
             ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')
               AND NOT(table_name='artifact_jobs' AND column_name='id')
               AND (extra IS NULL OR extra<>'')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_extra != 0 {
            bail!("mysql artifact scheduler has unexpected generated column behavior")
        }
        for (table, column, column_type, nullable, default) in COLUMNS {
            let found: Option<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT lower(column_type),is_nullable,column_default
                 FROM information_schema.columns WHERE table_schema=DATABASE()
                   AND table_name=? AND column_name=?",
            )
            .bind(table)
            .bind(column)
            .fetch_optional(&mut **tx)
            .await?;
            let Some((actual_type, actual_nullable, actual_default)) = found else {
                bail!("mysql artifact scheduler missing {table}.{column}")
            };
            if actual_type != *column_type
                || actual_nullable != *nullable
                || actual_default.as_deref() != *default
            {
                bail!("mysql artifact scheduler column definition mismatch for {table}.{column}")
            }
        }
        const INDEXES: &[(&str, &str, &str, bool)] = &[
            ("artifact_scheduler_schema", "PRIMARY", "id", true),
            ("artifact_jobs", "PRIMARY", "id", true),
            (
                "artifact_jobs",
                "artifact_jobs_identity",
                "workspace,repo,commit_oid,kind,format_version",
                true,
            ),
            (
                "artifact_jobs",
                "artifact_jobs_claim",
                "state,kind,created_at,id",
                false,
            ),
            (
                "artifact_jobs",
                "artifact_jobs_lease",
                "state,lease_expires_at",
                false,
            ),
            (
                "branch_observations",
                "PRIMARY",
                "workspace,repo,branch",
                true,
            ),
            (
                "artifact_observations",
                "PRIMARY",
                "workspace,repo,branch,kind",
                true,
            ),
            (
                "artifact_observations",
                "artifact_observations_desired",
                "desired_artifact_id",
                false,
            ),
            (
                "artifact_observations",
                "artifact_observations_published",
                "published_artifact_id",
                false,
            ),
            (
                "artifact_consumers",
                "PRIMARY",
                "artifact_id,consumer_id",
                true,
            ),
            (
                "artifact_consumers",
                "artifact_consumers_expiry",
                "expires_at",
                false,
            ),
            (
                "artifact_transport_leases",
                "PRIMARY",
                "root_hash,session_id",
                true,
            ),
            (
                "artifact_transport_leases",
                "artifact_transport_leases_expiry",
                "expires_at",
                false,
            ),
            ("artifact_base_retention", "PRIMARY", "artifact_id", true),
            (
                "artifact_base_retention",
                "artifact_base_retention_scope",
                "workspace,repo,format_version",
                false,
            ),
            ("artifact_gc_sweep", "PRIMARY", "id", true),
            ("scheduler_state", "PRIMARY", "id", true),
        ];
        let index_count: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT table_name,index_name) FROM information_schema.statistics
             WHERE table_schema=DATABASE() AND table_name IN
              ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if index_count != INDEXES.len() as i64 {
            bail!("mysql artifact scheduler schema has unexpected or missing indexes")
        }
        for (table, name, columns, unique) in INDEXES {
            let found: Vec<(
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i64>,
                String,
                i64,
                String,
            )> = sqlx::query_as(
                "SELECT column_name,collation,expression,sub_part,index_type,non_unique,is_visible
                 FROM information_schema.statistics WHERE table_schema=DATABASE()
                   AND table_name=? AND index_name=? ORDER BY seq_in_index",
            )
            .bind(table)
            .bind(name)
            .fetch_all(&mut **tx)
            .await?;
            let expected_columns: Vec<&str> = columns.split(',').collect();
            if found.len() != expected_columns.len() {
                bail!("mysql artifact scheduler index arity mismatch for {table}.{name}")
            }
            for (part, expected_column) in found.iter().zip(expected_columns) {
                let (column, order, expression, prefix, index_type, non_unique, visible) = part;
                if column.as_deref() != Some(expected_column)
                    || order.as_deref() != Some("A")
                    || expression.is_some()
                    || prefix.is_some()
                    || index_type != "BTREE"
                    || (*non_unique == 0) != *unique
                    || visible != "YES"
                {
                    bail!("mysql artifact scheduler index definition mismatch for {table}.{name}")
                }
            }
        }
        const CHECKS: &[(&str, &str)] = &[
            ("artifact_scheduler_schema_singleton", "`id` = 1"),
            (
                "artifact_jobs_format",
                "`format_version` between 1 and 4294967295",
            ),
            (
                "artifact_jobs_state",
                "`state` in ('queued','running','ready','failed')",
            ),
            (
                "artifact_jobs_kind",
                "`kind` in ('head','full_history','files')",
            ),
            (
                "artifact_jobs_lease_generation",
                "`lease_generation` between 0 and 9223372036854775807",
            ),
            (
                "artifact_jobs_claim_attempts",
                "`claim_attempts` between 0 and 4294967295",
            ),
            (
                "artifact_jobs_retry_count",
                "`retry_count` between 0 and 4294967295",
            ),
            (
                "artifact_jobs_failure_class",
                "`failure_class` is null or `failure_class` in ('retryable','permanent','dead_letter')",
            ),
            ("branch_observations_generation", "`generation` >= 1"),
            (
                "artifact_observations_generation",
                "`desired_generation` >= 1",
            ),
            (
                "artifact_observations_format",
                "`format_version` between 1 and 4294967295",
            ),
            ("scheduler_state_singleton", "`id` = 1"),
            (
                "scheduler_state_fairness",
                "`fairness_cursor` between 0 and 3",
            ),
            (
                "artifact_base_retention_ranks",
                "(`head_rank` is null or `head_rank` between 1 and 8) and (`pair_rank` is null or `pair_rank` between 1 and 8) and (`head_rank` is not null or `pair_rank` is not null)",
            ),
            ("artifact_gc_sweep_singleton", "`id` = 1"),
        ];
        const CONSTRAINTS: &[(&str, &str, &str)] = &[
            ("artifact_scheduler_schema", "PRIMARY", "PRIMARY KEY"),
            (
                "artifact_scheduler_schema",
                "artifact_scheduler_schema_singleton",
                "CHECK",
            ),
            ("artifact_jobs", "PRIMARY", "PRIMARY KEY"),
            ("artifact_jobs", "artifact_jobs_identity", "UNIQUE"),
            ("artifact_jobs", "artifact_jobs_format", "CHECK"),
            ("artifact_jobs", "artifact_jobs_state", "CHECK"),
            ("artifact_jobs", "artifact_jobs_kind", "CHECK"),
            ("artifact_jobs", "artifact_jobs_lease_generation", "CHECK"),
            ("artifact_jobs", "artifact_jobs_claim_attempts", "CHECK"),
            ("artifact_jobs", "artifact_jobs_retry_count", "CHECK"),
            ("artifact_jobs", "artifact_jobs_failure_class", "CHECK"),
            ("branch_observations", "PRIMARY", "PRIMARY KEY"),
            (
                "branch_observations",
                "branch_observations_generation",
                "CHECK",
            ),
            ("artifact_observations", "PRIMARY", "PRIMARY KEY"),
            (
                "artifact_observations",
                "artifact_observations_generation",
                "CHECK",
            ),
            (
                "artifact_observations",
                "artifact_observations_format",
                "CHECK",
            ),
            ("artifact_consumers", "PRIMARY", "PRIMARY KEY"),
            ("artifact_transport_leases", "PRIMARY", "PRIMARY KEY"),
            ("artifact_base_retention", "PRIMARY", "PRIMARY KEY"),
            (
                "artifact_base_retention",
                "artifact_base_retention_artifact",
                "FOREIGN KEY",
            ),
            (
                "artifact_base_retention",
                "artifact_base_retention_ranks",
                "CHECK",
            ),
            ("artifact_gc_sweep", "PRIMARY", "PRIMARY KEY"),
            ("artifact_gc_sweep", "artifact_gc_sweep_singleton", "CHECK"),
            ("scheduler_state", "PRIMARY", "PRIMARY KEY"),
            ("scheduler_state", "scheduler_state_singleton", "CHECK"),
            ("scheduler_state", "scheduler_state_fairness", "CHECK"),
        ];
        let mut actual_constraints: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT table_name,constraint_name,constraint_type,enforced
             FROM information_schema.table_constraints WHERE constraint_schema=DATABASE()
               AND table_name IN ('artifact_scheduler_schema','artifact_jobs','branch_observations',
                 'artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')",
        )
        .fetch_all(&mut **tx)
        .await?;
        actual_constraints.sort();
        let mut expected_constraints: Vec<(String, String, String, String)> = CONSTRAINTS
            .iter()
            .map(|(table, name, kind)| {
                (
                    (*table).into(),
                    (*name).into(),
                    (*kind).into(),
                    "YES".into(),
                )
            })
            .collect();
        expected_constraints.sort();
        if actual_constraints != expected_constraints {
            bail!("mysql artifact scheduler constraint inventory differs from schema version")
        }
        // An FK owned by any other table can still target scheduler rows and
        // turn ordinary pruning/supersession deletes into externally-controlled
        // failures. The constraint is recorded on the child, so validating only
        // the owned-table inventory above cannot see it. This reverse scan is
        // deliberately database-wide and fail-closed: scheduler tables do not
        // define or permit incoming referential dependencies.
        let incoming_foreign_keys: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.key_column_usage k
             JOIN information_schema.referential_constraints r
               ON r.constraint_schema=k.constraint_schema
              AND r.table_name=k.table_name AND r.constraint_name=k.constraint_name
             WHERE k.referenced_table_schema=DATABASE()
               AND k.referenced_table_name IN
                  ('artifact_scheduler_schema','artifact_jobs','branch_observations',
                  'artifact_observations','artifact_consumers','artifact_transport_leases','artifact_base_retention','artifact_gc_sweep','scheduler_state')
               AND NOT((k.table_name='artifact_base_retention' AND k.constraint_name='artifact_base_retention_artifact')
                    OR (k.table_name='ready_publication_fence_members' AND k.constraint_name='ready_fence_members_artifact')
                    OR (k.table_name='artifact_intents' AND k.constraint_name='artifact_intents_artifact'))",
        )
        .fetch_one(&mut **tx)
        .await?;
        if incoming_foreign_keys != 0 {
            bail!("mysql artifact scheduler tables have external foreign-key dependents")
        }
        let retention_fk: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.referential_constraints r JOIN information_schema.key_column_usage k ON k.constraint_schema=r.constraint_schema AND k.table_name=r.table_name AND k.constraint_name=r.constraint_name WHERE r.constraint_schema=DATABASE() AND r.table_name='artifact_base_retention' AND r.constraint_name='artifact_base_retention_artifact' AND r.referenced_table_name='artifact_jobs' AND r.delete_rule='CASCADE' AND k.column_name='artifact_id' AND k.referenced_column_name='id'").fetch_one(&mut **tx).await?;
        if retention_fk != 1 {
            bail!("mysql artifact base retention foreign key differs from schema version")
        }
        for (name, clause) in CHECKS {
            let actual: Option<String> = sqlx::query_scalar(
                "SELECT lower(check_clause)
                 FROM information_schema.check_constraints
                 WHERE constraint_schema=DATABASE() AND constraint_name=?",
            )
            .bind(name)
            .fetch_optional(&mut **tx)
            .await?;
            // MySQL varies harmless outer parentheses, but nothing else is
            // ignored: the exact allowed states and numeric bounds are provenance.
            if actual.as_deref().map(normalize_check).as_deref()
                != Some(normalize_check(clause).as_str())
            {
                bail!(
                    "mysql artifact scheduler check definition mismatch for {name}: actual={:?} expected={:?}",
                    actual.as_deref().map(normalize_check),
                    normalize_check(clause)
                )
            }
        }
        let invalid_jobs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs WHERE
               state IS NULL OR state NOT IN('queued','running','ready','failed')
               OR kind IS NULL OR kind NOT IN('head','full_history','files')
               OR format_version IS NULL OR format_version NOT BETWEEN 1 AND 4294967295
               OR lease_generation IS NULL OR lease_generation<0
               OR claim_attempts IS NULL OR claim_attempts NOT BETWEEN 0 AND 4294967295
               OR retry_count IS NULL OR retry_count NOT BETWEEN 0 AND 4294967295
               OR (failure_class IS NOT NULL AND failure_class NOT IN('retryable','permanent','dead_letter'))
               OR (state='running' AND (owner IS NULL OR length(trim(owner))=0 OR lease_expires_at IS NULL))
               OR (state='ready' AND (manifest IS NULL OR length(trim(manifest))=0))",
        ).fetch_one(&mut **tx).await?;
        if invalid_jobs != 0 {
            bail!("mysql artifact scheduler contains invalid artifact jobs")
        }
        let invalid_observations: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_observations a
             LEFT JOIN artifact_jobs d ON d.id=a.desired_artifact_id
               AND d.workspace=a.workspace AND d.repo=a.repo AND d.kind=a.kind
               AND d.commit_oid=a.desired_commit AND d.format_version=a.format_version
             LEFT JOIN artifact_jobs p ON p.id=a.published_artifact_id
               AND p.workspace=a.workspace AND p.repo=a.repo AND p.kind=a.kind
               AND p.format_version=a.format_version AND p.state='ready'
               AND p.manifest IS NOT NULL AND length(trim(p.manifest))>0
             WHERE a.desired_generation IS NULL OR a.desired_generation<1
                OR a.format_version IS NULL OR a.format_version NOT BETWEEN 1 AND 4294967295
                OR d.id IS NULL OR (a.published_artifact_id IS NOT NULL AND p.id IS NULL)",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_observations != 0 {
            bail!("mysql artifact scheduler contains invalid artifact observations")
        }
        let invalid_branches: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM branch_observations WHERE generation IS NULL OR generation<1",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_branches != 0 {
            bail!("mysql artifact scheduler contains invalid branch observations")
        }
        let invalid_retention: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact_base_retention r LEFT JOIN artifact_jobs j ON j.id=r.artifact_id WHERE j.id IS NULL OR j.workspace<>r.workspace OR j.repo<>r.repo OR j.format_version<>r.format_version OR (r.head_rank IS NULL AND r.pair_rank IS NULL) OR (r.head_rank IS NOT NULL AND r.head_rank NOT BETWEEN 1 AND 8) OR (r.pair_rank IS NOT NULL AND r.pair_rank NOT BETWEEN 1 AND 8)").fetch_one(&mut **tx).await?;
        if invalid_retention != 0 {
            bail!("mysql artifact scheduler contains invalid base retention")
        }
        let invalid_transport: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_transport_leases
             WHERE root_hash NOT REGEXP '^[0-9a-f]{64}$'
                OR session_id NOT REGEXP '^[0-9a-f]{64}$'
                OR length(trim(workspace))=0 OR length(trim(repo))=0",
        )
        .fetch_one(&mut **tx)
        .await?;
        let conflicting_sessions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT session_id FROM artifact_transport_leases
             GROUP BY session_id HAVING count(DISTINCT concat(workspace,char(0),repo))>1) conflicts",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_transport != 0 || conflicting_sessions != 0 {
            bail!("mysql artifact scheduler contains invalid transport leases")
        }
        let invalid_control: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM scheduler_state WHERE id IS NULL OR id<>1
               OR fairness_cursor IS NULL OR fairness_cursor NOT BETWEEN 0 AND 3",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_control != 0 {
            bail!("mysql artifact scheduler contains invalid control state")
        }
        Ok(())
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    async fn controlled(&self) -> Result<(Transaction<'_, MySql>, i64)> {
        let mut tx = self.pool.begin().await?;
        // All cap/fairness/admission decisions serialize on this tiny row. The
        // jobs themselves remain normalized and heartbeat/settlement bypass it.
        sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        Ok((tx, now))
    }

    async fn get_tx(tx: &mut Transaction<'_, MySql>, id: i64) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,
                    lease_expires_at,lease_generation,claim_attempts,retry_count,
                    manifest,error,failure_class FROM artifact_jobs WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;
        row.map(row_record).transpose()
    }

    async fn get_key_tx(
        tx: &mut Transaction<'_, MySql>,
        key: &ArtifactKey,
    ) -> Result<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,
                    lease_expires_at,lease_generation,claim_attempts,retry_count,
                    manifest,error,failure_class FROM artifact_jobs
             WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?",
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
        tx: &mut Transaction<'_, MySql>,
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
             WHERE state IN('queued','running') AND workspace=?",
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
        tx: &mut Transaction<'_, MySql>,
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
             WHERE state IN('queued','running') AND workspace=?",
        )
        .bind(workspace)
        .fetch_one(&mut **tx)
        .await?;
        let per_kind: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE state IN('queued','running') AND kind=?",
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
        tx: &mut Transaction<'_, MySql>,
        key: &ArtifactKey,
        now: i64,
    ) -> Result<ScheduleOutcome> {
        if let Some(record) = Self::get_key_tx(tx, key).await? {
            return Ok(existing_outcome(record));
        }
        let result = sqlx::query(
            "INSERT INTO artifact_jobs(
                workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at)
             VALUES(?,?,?,?,?,'queued',?,?)",
        )
        .bind(&key.workspace)
        .bind(&key.repo)
        .bind(&key.commit)
        .bind(key.kind.as_str())
        .bind(key.format_version as i64)
        .bind(now)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        let id = i64::try_from(result.last_insert_id()).context("mysql artifact id overflow")?;
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
    tx: &mut Transaction<'_, MySql>,
    w: &str,
    r: &str,
    v: i64,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM artifact_base_retention WHERE workspace=? AND repo=? AND format_version=?",
    )
    .bind(w)
    .bind(r)
    .bind(v)
    .execute(&mut **tx)
    .await?;
    sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank) SELECT id,workspace,repo,format_version,rank_value FROM (SELECT id,workspace,repo,format_version,row_number() OVER(ORDER BY updated_at DESC,id DESC) rank_value FROM artifact_jobs WHERE workspace=? AND repo=? AND format_version=? AND kind='head' AND state='ready' AND manifest IS NOT NULL AND length(trim(manifest))>0) ranked WHERE rank_value<=8").bind(w).bind(r).bind(v).execute(&mut **tx).await?;
    for history in [false, true] {
        let id = if history { "history_id" } else { "head_id" };
        let sql = format!(
            "INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,pair_rank) SELECT {id},workspace,repo,format_version,rank_value FROM (SELECT h.id head_id,f.id history_id,h.workspace,h.repo,h.format_version,row_number() OVER(ORDER BY GREATEST(h.updated_at,f.updated_at) DESC,GREATEST(h.id,f.id) DESC) rank_value FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' AND f.state='ready' AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 WHERE h.workspace=? AND h.repo=? AND h.format_version=? AND h.kind='head' AND h.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0) ranked WHERE rank_value<=8 ON DUPLICATE KEY UPDATE pair_rank=VALUES(pair_rank)"
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

fn normalize_check(value: &str) -> String {
    // MySQL decorates stored string literals with their charset introducer and
    // adds redundant grouping parentheses. Our tiny check grammar contains no
    // functions or precedence-sensitive mixed AND/OR expressions, so removing
    // only those decorations still compares every identifier/operator/bound
    // and every literal exactly.
    value
        .replace("_utf8mb4", "")
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '(' && *c != ')' && *c != '\\')
        .collect()
}

fn check_mysql_len(field: &str, value: &str, max_chars: usize) -> Result<()> {
    if value.trim().is_empty() {
        bail!("artifact {field} is empty")
    }
    if value.chars().count() > max_chars {
        bail!("artifact {field} exceeds mysql scheduler limit of {max_chars} characters")
    }
    Ok(())
}

fn validate_mysql_identity(workspace: &str, repo: &str, branch: Option<&str>) -> Result<()> {
    check_mysql_len("workspace", workspace, 128)?;
    check_mysql_len("repo", repo, 320)?;
    if let Some(branch) = branch {
        check_mysql_len("branch", branch, 191)?;
    }
    Ok(())
}

fn validate_mysql_key(key: &ArtifactKey) -> Result<()> {
    validate_mysql_identity(&key.workspace, &key.repo, None)?;
    check_mysql_len("commit", &key.commit, 64)?;
    if key.format_version == 0 {
        bail!("artifact format version must be positive")
    }
    Ok(())
}

fn validate_gc_sweep_args(owner: &str, ttl: i64) -> Result<()> {
    if owner.trim().is_empty() || owner.len() > 200 || !(1..=600).contains(&ttl) {
        bail!("GC sweep owner or TTL is invalid")
    }
    Ok(())
}

async fn ensure_gc_unfenced_mysql(tx: &mut Transaction<'_, MySql>, now: i64) -> Result<()> {
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
impl ArtifactSchedulerPersistence for MysqlArtifactScheduler {
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
        e: &[(i64, Option<String>)],
        p: &ActivationFenceProvenance,
        ttl: i64,
    ) -> Result<Option<ReadyPublicationFence>> {
        validate_mysql_fence(e, p, ttl)?;
        let op = p.operation_id();
        let token = hex::encode(rand::random::<[u8; 32]>());
        let (mut tx, now) = self.controlled().await?;
        let existing:Option<(String,i64,i64,String,String,String,String,String,String)>=sqlx::query_as("SELECT token,generation,expires_at,state,workspace,repo,branch,target,attempt_id FROM ready_publication_fences WHERE operation_id=? FOR UPDATE").bind(&op).fetch_optional(&mut *tx).await?;
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
            if w != &p.workspace
                || r != &p.repo
                || b != &p.branch
                || target != &p.target
                || attempt != &p.attempt_id
            {
                bail!("activation operation provenance mismatch")
            }
            if state == "held" && expires > now {
                tx.rollback().await?;
                return Ok(None);
            }
            if state == "activation_unknown" {
                let members:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=? AND generation=? ORDER BY artifact_id").bind(old).bind(generation).fetch_all(&mut *tx).await?;
                if members != mysql_expected(e) {
                    bail!("activation recovery fence membership does not match operation")
                }
                tx.commit().await?;
                return Ok(Some(ReadyPublicationFence::new(
                    old.clone(),
                    generation as u64,
                    op,
                    p.clone(),
                    e.to_vec(),
                )));
            }
            sqlx::query("DELETE FROM ready_publication_fences WHERE token=? AND generation=?")
                .bind(old)
                .bind(generation)
                .execute(&mut *tx)
                .await?;
        }
        if !exact_ready_pair_mysql(&mut tx, e, p).await? {
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
        sqlx::query(
            "UPDATE ready_publication_fence_sequence SET generation=? WHERE id=1 AND generation=?",
        )
        .bind(generation)
        .bind(prior)
        .execute(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO ready_publication_fences(token,generation,operation_id,workspace,repo,branch,target,attempt_id,expires_at,state) VALUES(?,?,?,?,?,?,?,?,?,'held')").bind(&token).bind(generation).bind(&op).bind(&p.workspace).bind(&p.repo).bind(&p.branch).bind(&p.target).bind(&p.attempt_id).bind(now.saturating_add(ttl)).execute(&mut *tx).await?;
        for (id, m) in e {
            sqlx::query("INSERT INTO ready_publication_fence_members(token,generation,artifact_id,manifest) VALUES(?,?,?,?)").bind(&token).bind(generation).bind(id).bind(m).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(Some(ReadyPublicationFence::new(
            token,
            generation as u64,
            op,
            p.clone(),
            e.to_vec(),
        )))
    }
    async fn release_ready_publication_fence(&self, f: ReadyPublicationFence) -> Result<()> {
        let (token, generation, op, e) = f.parts();
        let mut tx = self.pool.begin().await?;
        let m:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=? AND generation=? ORDER BY artifact_id FOR UPDATE").bind(token).bind(generation as i64).fetch_all(&mut *tx).await?;
        if m == mysql_expected(e) {
            sqlx::query("DELETE FROM ready_publication_fences WHERE token=? AND generation=? AND operation_id=?").bind(token).bind(generation as i64).bind(op).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn mark_activation_unknown(&self, f: &ReadyPublicationFence, ttl: i64) -> Result<bool> {
        if !(1..=3600).contains(&ttl) {
            bail!("activation fence TTL is invalid")
        }
        let (token, generation, op, e) = f.parts();
        let (mut tx, now) = self.controlled().await?;
        let m:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=? AND generation=? ORDER BY artifact_id").bind(token).bind(generation as i64).fetch_all(&mut *tx).await?;
        let changed=m==mysql_expected(e)&&sqlx::query("UPDATE ready_publication_fences SET state='activation_unknown',expires_at=? WHERE token=? AND generation=? AND operation_id=?").bind(now.saturating_add(ttl)).bind(token).bind(generation as i64).bind(op).execute(&mut *tx).await?.rows_affected()==1;
        tx.commit().await?;
        Ok(changed)
    }
    async fn recover_activation_fence(
        &self,
        p: &ActivationFenceProvenance,
    ) -> Result<Option<ReadyPublicationFence>> {
        let op = p.operation_id();
        let row:Option<(String,i64)>=sqlx::query_as("SELECT token,generation FROM ready_publication_fences WHERE operation_id=? AND workspace=? AND repo=? AND branch=? AND target=? AND attempt_id=? AND state='activation_unknown'").bind(&op).bind(&p.workspace).bind(&p.repo).bind(&p.branch).bind(&p.target).bind(&p.attempt_id).fetch_optional(&self.pool).await?;
        let Some((token, generation)) = row else {
            return Ok(None);
        };
        let e:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=? AND generation=? ORDER BY artifact_id").bind(&token).bind(generation).fetch_all(&self.pool).await?;
        if e.len() != 2 {
            bail!("activation recovery fence is not an exact pair")
        }
        Ok(Some(ReadyPublicationFence::new(
            token,
            generation as u64,
            op,
            p.clone(),
            e,
        )))
    }
    async fn unknown_activation_fences_page(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<UnknownActivationFencePage> {
        if !(1..=128).contains(&limit) || after.unwrap_or(0) > i64::MAX as u64 {
            bail!("unknown activation fence page is invalid")
        }
        let rows:Vec<(String,i64,String,String,String,String,String,String)>=sqlx::query_as("SELECT token,generation,operation_id,workspace,repo,branch,target,attempt_id FROM ready_publication_fences WHERE state='activation_unknown' AND generation>? ORDER BY generation LIMIT ?").bind(after.unwrap_or(0)as i64).bind(limit as i64).fetch_all(&self.pool).await?;
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
            }
            let e:Vec<(i64,Option<String>)>=sqlx::query_as("SELECT artifact_id,manifest FROM ready_publication_fence_members WHERE token=? AND generation=? ORDER BY artifact_id").bind(&token).bind(generation).fetch_all(&self.pool).await?;
            if e.len() != 2 {
                bail!("unknown activation fence is not an exact pair")
            }
            fences.push(ReadyPublicationFence::new(
                token,
                generation as u64,
                op,
                p,
                e,
            ))
        }
        let next = (fences.len() == limit).then(|| fences.last().unwrap().parts().1);
        Ok(UnknownActivationFencePage {
            fences,
            next_generation: next,
        })
    }

    async fn acquire_gc_sweep(&self, owner: &str, ttl: i64) -> Result<bool> {
        validate_gc_sweep_args(owner, ttl)?;
        let (mut tx, now) = self.controlled().await?;
        sqlx::query("INSERT INTO artifact_gc_sweep(id,owner,expires_at) VALUES(1,?,?) ON DUPLICATE KEY UPDATE owner=IF(expires_at<=? OR owner=VALUES(owner),VALUES(owner),owner),expires_at=IF(expires_at<=? OR owner=VALUES(owner),VALUES(expires_at),expires_at)")
            .bind(owner).bind(now + ttl).bind(now).bind(now).execute(&mut *tx).await?;
        let held: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_gc_sweep WHERE id=1 AND owner=? AND expires_at>?",
        )
        .bind(owner)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(held == 1)
    }
    async fn renew_gc_sweep(&self, owner: &str, ttl: i64) -> Result<bool> {
        validate_gc_sweep_args(owner, ttl)?;
        let (mut tx, now) = self.controlled().await?;
        let won = sqlx::query(
            "UPDATE artifact_gc_sweep SET expires_at=? WHERE id=1 AND owner=? AND expires_at>?",
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
        sqlx::query("DELETE FROM artifact_gc_sweep WHERE id=1 AND owner=?")
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
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
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
        Ok(Box::new(MysqlGcDeleteFence(Some(tx))))
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
        ensure_gc_unfenced_mysql(&mut tx, now).await?;
        let foreign: i64 = sqlx::query_scalar("SELECT count(*) FROM artifact_transport_leases WHERE session_id=? AND (workspace<>? OR repo<>?)")
            .bind(session).bind(workspace).bind(repo).fetch_one(&mut *tx).await?;
        if foreign != 0 {
            bail!("transport session is already bound to another repository")
        }
        let changed = sqlx::query("INSERT INTO artifact_transport_leases(root_hash,session_id,workspace,repo,expires_at) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE expires_at=IF(workspace=VALUES(workspace) AND repo=VALUES(repo),VALUES(expires_at),expires_at)")
            .bind(root).bind(session).bind(workspace).bind(repo).bind(now + ttl).execute(&mut *tx).await?.rows_affected();
        // MySQL reports 1 for insert, 2 for a changed update, and 0 for a no-op.
        // Re-read identity because a no-op may be either an identical expiry or a conflict.
        let identity: Option<(String, String)> = sqlx::query_as("SELECT workspace,repo FROM artifact_transport_leases WHERE root_hash=? AND session_id=?")
            .bind(root).bind(session).fetch_optional(&mut *tx).await?;
        if identity.as_ref().map(|(w, r)| (w.as_str(), r.as_str())) != Some((workspace, repo)) {
            bail!("transport root identity conflict")
        }
        let _ = changed;
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
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        let won = sqlx::query("UPDATE artifact_transport_leases SET expires_at=? WHERE root_hash=? AND session_id=? AND workspace=? AND repo=? AND expires_at>?")
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
        Ok(sqlx::query("DELETE FROM artifact_transport_leases WHERE root_hash=? AND session_id=? AND workspace=? AND repo=?")
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
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        let rows = if let Some((root, session)) = after {
            validate_transport_lease_identity(root, session, "cursor", "cursor", 1)?;
            sqlx::query("SELECT root_hash,session_id,workspace,repo,expires_at FROM artifact_transport_leases WHERE expires_at>? AND (root_hash>? OR (root_hash=? AND session_id>?)) ORDER BY root_hash,session_id LIMIT ?")
                .bind(now).bind(root).bind(root).bind(session).bind(limit as i64).fetch_all(&mut *tx).await?
        } else {
            sqlx::query("SELECT root_hash,session_id,workspace,repo,expires_at FROM artifact_transport_leases WHERE expires_at>? ORDER BY root_hash,session_id LIMIT ?")
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
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        let rows = sqlx::query(
            "WITH candidates(id) AS ((SELECT published_artifact_id FROM artifact_observations WHERE published_artifact_id>? ORDER BY published_artifact_id LIMIT ?) UNION ALL (SELECT artifact_id FROM artifact_consumers WHERE artifact_id>? AND expires_at>? ORDER BY artifact_id LIMIT ?) UNION ALL (SELECT artifact_id FROM artifact_base_retention WHERE artifact_id>? ORDER BY artifact_id LIMIT ?) UNION ALL (SELECT m.artifact_id FROM ready_publication_fence_members m JOIN ready_publication_fences f ON f.token=m.token AND f.generation=m.generation WHERE m.artifact_id>? AND (f.state='activation_unknown' OR f.expires_at>?) ORDER BY m.artifact_id LIMIT ?)), page_ids(id) AS (SELECT DISTINCT id FROM candidates ORDER BY id LIMIT ?) SELECT j.id,j.workspace,j.repo,j.commit_oid,j.kind,j.format_version,j.manifest FROM page_ids p JOIN artifact_jobs j ON j.id=p.id WHERE j.state='ready' AND j.manifest IS NOT NULL AND length(trim(j.manifest))>0 ORDER BY j.id",
        )
        .bind(after_artifact_id.unwrap_or(0)).bind(limit as i64).bind(after_artifact_id.unwrap_or(0)).bind(now).bind(limit as i64).bind(after_artifact_id.unwrap_or(0)).bind(limit as i64).bind(after_artifact_id.unwrap_or(0)).bind(now).bind(limit as i64).bind(limit as i64)
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

    async fn live_source_objects_page(
        &self,
        after: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<crate::git_source_registry::SourceGcObject>> {
        if limit == 0 || limit > crate::git_source_registry::SOURCE_ROOT_PAGE_MAX {
            bail!("source GC page limit is invalid")
        }
        let (hash, owner) = after.unwrap_or(("", ""));
        let rows=sqlx::query("WITH objects(hash,len,owner) AS (SELECT root_hash,root_len,CONCAT('r:',root_hash) FROM git_source_roots UNION ALL SELECT child_hash,child_len,CONCAT('r:',root_hash,':',LPAD(ordinal,20,'0')) FROM git_source_members UNION ALL SELECT root_hash,root_len,CONCAT('a:',token) FROM git_source_acquisitions WHERE state='activation_unknown' OR (state='graph_published' AND expires_at>UNIX_TIMESTAMP()) UNION ALL SELECT m.child_hash,m.child_len,CONCAT('a:',m.token,':',LPAD(m.ordinal,20,'0')) FROM git_source_acquisition_members m JOIN git_source_acquisitions a ON a.token=m.token WHERE a.state='activation_unknown' OR (a.state='graph_published' AND a.expires_at>UNIX_TIMESTAMP())) SELECT hash,len,owner FROM objects WHERE hash>? OR (hash=? AND owner>?) ORDER BY hash,owner LIMIT ?")
            .bind(hash).bind(hash).bind(owner).bind(limit as i64).fetch_all(&self.pool).await?;
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

    async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome> {
        validate_mysql_key(key)?;
        let (mut tx, now) = self.controlled().await?;
        ensure_gc_unfenced_mysql(&mut tx, now).await?;
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
        validate_mysql_key(key)?;
        validate_public_consumer_id(consumer_id)?;
        check_mysql_len("consumer id", consumer_id, 255)?;
        if !(2..=86400).contains(&ttl_secs) {
            bail!("consumer subscription TTL is invalid")
        }
        let (mut tx, now) = self.controlled().await?;
        ensure_gc_unfenced_mysql(&mut tx, now).await?;
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
             VALUES(?,?,?)
             ON DUPLICATE KEY UPDATE expires_at=VALUES(expires_at)",
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
        validate_public_consumer_id(consumer_id)?;
        let (mut tx, _) = self.controlled().await?;
        sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?")
            .bind(artifact_id)
            .bind(consumer_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "DELETE FROM artifact_jobs WHERE id=? AND state='queued'
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
        validate_mysql_identity(workspace, repo, Some(branch))?;
        validate_resolved_commit(commit)?;
        check_mysql_len("commit", commit, 64)?;
        if kinds.is_empty() {
            bail!("observation requests no artifact kinds")
        }
        if format_version == 0 {
            bail!("artifact format version must be positive")
        }
        let mut unique = Vec::new();
        for &kind in kinds {
            if !unique.contains(&kind) {
                unique.push(kind);
            }
        }
        let (mut tx, now) = self.controlled().await?;
        ensure_gc_unfenced_mysql(&mut tx, now).await?;
        let current: Option<(i64, String)> = sqlx::query_as(
            "SELECT generation,desired_commit FROM branch_observations
             WHERE workspace=? AND repo=? AND branch=?",
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
                    "SELECT count(*) FROM artifact_observations WHERE workspace=? AND repo=? AND branch=? AND kind=? AND desired_commit=? AND format_version=?",
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
                           WHERE workspace=? AND repo=? AND branch=? AND kind=?)
                 AND id NOT IN(SELECT desired_artifact_id FROM artifact_observations
                               WHERE NOT(workspace=? AND repo=? AND branch=? AND kind=?))
                 AND id NOT IN(SELECT artifact_id FROM artifact_consumers)",
            )
            .bind(workspace)
            .bind(repo)
            .bind(branch)
            .bind(kind.as_str())
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
                 VALUES(?,?,?,?,?,?,?,
                    CASE WHEN (SELECT state FROM artifact_jobs WHERE id=?)='ready'
                         THEN ? ELSE NULL END,?,?)
                 ON DUPLICATE KEY UPDATE
                    desired_commit=VALUES(desired_commit),
                    desired_artifact_id=VALUES(desired_artifact_id),
                    desired_generation=VALUES(desired_generation),
                    published_artifact_id=CASE
                      WHEN (SELECT state FROM artifact_jobs WHERE id=VALUES(desired_artifact_id))='ready'
                        THEN VALUES(desired_artifact_id)
                      WHEN artifact_observations.format_version=VALUES(format_version)
                        THEN artifact_observations.published_artifact_id
                      ELSE NULL END,
                    format_version=VALUES(format_version),observed_at=VALUES(observed_at)",
            )
            .bind(workspace)
            .bind(repo)
            .bind(branch)
            .bind(kind.as_str())
            .bind(commit)
            .bind(id)
            .bind(generation as i64)
            .bind(id)
            .bind(id)
            .bind(format_version as i64)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            artifacts.push((kind, outcome));
        }
        sqlx::query(
            "INSERT INTO branch_observations(
                workspace,repo,branch,generation,desired_commit,updated_at)
             VALUES(?,?,?,?,?,?)
             ON DUPLICATE KEY UPDATE
                generation=VALUES(generation),desired_commit=VALUES(desired_commit),
                updated_at=VALUES(updated_at)",
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
            "DELETE FROM artifact_jobs WHERE workspace=? AND repo=? AND state='queued'
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
        validate_mysql_key(key)?;
        let (mut tx, now) = self.controlled().await?;
        let row: Option<(i64, String, Option<String>, i64)> = sqlx::query_as(
            "SELECT id,state,failure_class,retry_count FROM artifact_jobs
             WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?
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
                        failure_class=NULL,updated_at=? WHERE id=? AND state='failed'",
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
        validate_mysql_identity(workspace, repo, Some(branch))?;
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

    async fn claim(&self, owner: &str, lease_secs: i64) -> Result<Option<ClaimedArtifact>> {
        validate_lease(owner, lease_secs)?;
        check_mysql_len("lease owner", owner, 255)?;
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
                "SELECT count(*) FROM artifact_jobs WHERE state='running' AND kind=?",
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
                     WHERE q.state='queued' AND q.kind=?
                       AND (SELECT count(*) FROM artifact_jobs wr
                            WHERE wr.state='running' AND wr.workspace=q.workspace) < ?
                       AND NOT EXISTS(SELECT 1 FROM artifact_jobs r
                           WHERE r.state='running' AND r.workspace=q.workspace AND r.repo=q.repo
                             AND r.kind=q.kind)
                     ORDER BY CASE WHEN q.workspace>? THEN 0 ELSE 1 END,
                              q.workspace,q.created_at,q.id
                     LIMIT 1 FOR UPDATE SKIP LOCKED",
                )
                .bind(kind.as_str())
                .bind(self.limits.workspace_running as i64)
                .bind(&workspace_cursor)
                .fetch_optional(&mut *tx)
                .await?
            } else {
                sqlx::query_scalar(
                    "SELECT q.id FROM artifact_jobs q
                     WHERE q.state='queued' AND q.kind=?
                       AND (SELECT count(*) FROM artifact_jobs wr
                            WHERE wr.state='running' AND wr.workspace=q.workspace) < ?
                     ORDER BY CASE WHEN q.workspace>? THEN 0 ELSE 1 END,
                              q.workspace,q.created_at,q.id
                     LIMIT 1 FOR UPDATE SKIP LOCKED",
                )
                .bind(kind.as_str())
                .bind(self.limits.workspace_running as i64)
                .bind(&workspace_cursor)
                .fetch_optional(&mut *tx)
                .await?
            };
            let Some(id) = id else { continue };
            let won = sqlx::query(
                "UPDATE artifact_jobs SET state='running',owner=?,heartbeat_at=?,
                    lease_expires_at=?,lease_generation=lease_generation+1,
                    claim_attempts=claim_attempts+1,updated_at=?
                 WHERE id=? AND state='queued'",
            )
            .bind(owner)
            .bind(now)
            .bind(now + lease_secs)
            .bind(now)
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if won == 1 {
                let record = Self::get_tx(&mut tx, id)
                    .await?
                    .context("claimed artifact disappeared")?;
                sqlx::query(
                    "UPDATE scheduler_state SET fairness_cursor=?,workspace_cursor=? WHERE id=1",
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
        check_mysql_len("lease owner", owner, 255)?;
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        let affected = sqlx::query(
            "UPDATE artifact_jobs SET heartbeat_at=?,lease_expires_at=?,updated_at=?
             WHERE id=? AND state='running' AND owner=? AND lease_generation=?
               AND lease_expires_at>=?",
        )
        .bind(now)
        .bind(now + lease_secs)
        .bind(now)
        .bind(claim.record.id)
        .bind(owner)
        .bind(claim.record.lease_generation as i64)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let won = affected == 1
            || sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM artifact_jobs WHERE id=? AND state='running'
               AND owner=? AND lease_generation=? AND lease_expires_at>=?",
            )
            .bind(claim.record.id)
            .bind(owner)
            .bind(claim.record.lease_generation as i64)
            .bind(now)
            .fetch_one(&mut *tx)
            .await?
                == 1;
        tx.commit().await?;
        Ok(won)
    }

    async fn owns(&self, claim: &ClaimedArtifact, owner: &str) -> Result<bool> {
        check_mysql_len("lease owner", owner, 255)?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_jobs
             WHERE id=? AND state='running' AND owner=? AND lease_generation=?
               AND lease_expires_at>=UNIX_TIMESTAMP()",
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
        check_mysql_len("lease owner", owner, 255)?;
        let evidence = self.completion_sealer.verify(claim, verified)?;
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        ensure_gc_unfenced_mysql(&mut tx, now).await?;
        let won = sqlx::query(
            "UPDATE artifact_jobs SET state='ready',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,manifest=?,error=NULL,failure_class=NULL,updated_at=?
             WHERE id=? AND state='running' AND owner=? AND lease_generation=?
               AND lease_expires_at>=?",
        )
        .bind(evidence.manifest())
        .bind(now)
        .bind(claim.record.id)
        .bind(owner)
        .bind(claim.record.lease_generation as i64)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        if won {
            // Exact desired identity and format were established atomically by
            // observe. The id predicate prevents an older completion from ever
            // repointing a branch that has advanced.
            sqlx::query(
                "UPDATE artifact_observations SET published_artifact_id=?
                 WHERE desired_artifact_id=?",
            )
            .bind(claim.record.id)
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
        check_mysql_len("lease owner", owner, 255)?;
        if error.trim().is_empty() {
            bail!("artifact failure reason is empty")
        }
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        let won = sqlx::query(
            "UPDATE artifact_jobs SET state='failed',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,error=?,failure_class=?,updated_at=?
             WHERE id=? AND state='running' AND owner=? AND lease_generation=?
               AND lease_expires_at>=?",
        )
        .bind(error)
        .bind(class.as_str())
        .bind(now)
        .bind(claim.record.id)
        .bind(owner)
        .bind(claim.record.lease_generation as i64)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(won)
    }

    async fn reconcile_expired(&self) -> Result<(u64, u64)> {
        let (mut tx, now) = self.controlled().await?;
        sqlx::query("DELETE FROM artifact_consumers WHERE expires_at<=?")
            .bind(now)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "DELETE FROM artifact_transport_leases WHERE expires_at<=?
             ORDER BY expires_at,root_hash,session_id LIMIT 512",
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
                failure_class='dead_letter',updated_at=?
             WHERE state='running' AND lease_expires_at<=? AND claim_attempts>=?",
        )
        .bind(now)
        .bind(now)
        .bind(self.limits.max_claim_attempts as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let queued = sqlx::query(
            "UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,error='lease expired; reclaimed',updated_at=?
             WHERE state='running' AND lease_expires_at<=? AND claim_attempts<?",
        )
        .bind(now)
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
                    manifest,error,failure_class FROM artifact_jobs WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_record).transpose()
    }

    async fn get_by_key(&self, key: &ArtifactKey) -> Result<Option<ArtifactRecord>> {
        validate_mysql_key(key)?;
        let row = sqlx::query(
            "SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,
                    lease_expires_at,lease_generation,claim_attempts,retry_count,
                    manifest,error,failure_class FROM artifact_jobs
             WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?",
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
        sqlx::query("SELECT id,workspace,repo,commit_oid,kind,format_version,state,owner,lease_expires_at,lease_generation,claim_attempts,retry_count,manifest,error,failure_class FROM artifact_jobs WHERE state='ready' AND manifest IS NOT NULL AND id>? ORDER BY id LIMIT ?")
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
        let retries:Option<i64>=sqlx::query_scalar("SELECT retry_count FROM artifact_jobs WHERE id=? AND state='ready' AND manifest=? FOR UPDATE").bind(id).bind(manifest).fetch_optional(&mut *tx).await?;
        let Some(retries) = retries else {
            tx.rollback().await?;
            return Ok(QuarantineOutcome::LostRace);
        };
        let fenced:i64=sqlx::query_scalar("SELECT count(*) FROM ready_publication_fence_members m JOIN ready_publication_fences f ON f.token=m.token AND f.generation=m.generation WHERE m.artifact_id=? AND (f.state='activation_unknown' OR f.expires_at>?)").bind(id).bind(now).fetch_one(&mut *tx).await?;
        if fenced != 0 {
            tx.rollback().await?;
            return Ok(QuarantineOutcome::LostRace);
        }
        let exhausted = retries as u32 >= self.limits.max_manual_retries;
        let changed=sqlx::query(if exhausted{"UPDATE artifact_jobs SET state='failed',manifest=NULL,owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,error=?,failure_class='permanent',updated_at=? WHERE id=? AND state='ready' AND manifest=?"}else{"UPDATE artifact_jobs SET state='queued',manifest=NULL,owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,retry_count=retry_count+1,error=?,failure_class=NULL,updated_at=? WHERE id=? AND state='ready' AND manifest=?"}).bind(reason.chars().take(4096).collect::<String>()).bind(now).bind(id).bind(manifest).execute(&mut *tx).await?.rows_affected()==1;
        if changed {
            sqlx::query("UPDATE artifact_observations SET published_artifact_id=NULL WHERE published_artifact_id=?")
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
        validate_mysql_identity(workspace, repo, None)?;
        if format_version == 0 || !(1..=32).contains(&limit) {
            bail!("ready candidate format or limit is invalid")
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

    async fn published(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
        kind: ArtifactKind,
        format_version: u32,
    ) -> Result<Option<ArtifactRecord>> {
        validate_mysql_identity(workspace, repo, Some(branch))?;
        if format_version == 0 {
            bail!("artifact format version must be positive")
        }
        let row = sqlx::query(
            "SELECT j.id,j.workspace,j.repo,j.commit_oid,j.kind,j.format_version,j.state,j.owner,
                    j.lease_expires_at,j.lease_generation,j.claim_attempts,j.retry_count,
                    j.manifest,j.error,j.failure_class
             FROM artifact_observations a JOIN artifact_jobs j
               ON j.id=a.published_artifact_id AND j.workspace=a.workspace AND j.repo=a.repo
              AND j.kind=a.kind AND j.format_version=a.format_version
             WHERE a.workspace=? AND a.repo=? AND a.branch=? AND a.kind=?
               AND a.format_version=? AND j.state='ready' AND j.manifest IS NOT NULL
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
        validate_mysql_identity(workspace, repo, None)?;
        if format_version == 0 || !(1..=32).contains(&limit) {
            bail!("full base candidate format or limit is invalid")
        }
        sqlx::query_scalar(
            "SELECT h.commit_oid FROM artifact_jobs h JOIN artifact_jobs f ON f.workspace=h.workspace AND f.repo=h.repo AND f.commit_oid=h.commit_oid AND f.format_version=h.format_version AND f.kind='full_history' WHERE h.workspace=? AND h.repo=? AND h.format_version=? AND h.kind='head' AND h.state='ready' AND f.state='ready' AND h.manifest IS NOT NULL AND length(trim(h.manifest))>0 AND f.manifest IS NOT NULL AND length(trim(f.manifest))>0 ORDER BY GREATEST(h.updated_at,f.updated_at) DESC,GREATEST(h.id,f.id) DESC LIMIT ?",
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
        validate_mysql_key(key)?;
        crate::cas::Cas::validate_artifact_id(expected_manifest)?;
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE artifact_jobs SET state='failed',manifest=NULL,error=?,failure_class='retryable',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,updated_at=UNIX_TIMESTAMP() WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=? AND state='ready' AND manifest=?",
        )
        .bind(reason)
        .bind(&key.workspace)
        .bind(&key.repo)
        .bind(&key.commit)
        .bind(key.kind.as_str())
        .bind(key.format_version as i64)
        .bind(expected_manifest)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() > 0 {
            sqlx::query("UPDATE artifact_observations SET published_artifact_id=NULL WHERE workspace=? AND repo=? AND published_artifact_id=(SELECT id FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?)")
                .bind(&key.workspace)
                .bind(&key.repo)
                .bind(&key.workspace)
                .bind(&key.repo)
                .bind(&key.commit)
                .bind(key.kind.as_str())
                .bind(key.format_version as i64)
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
        Ok(result.rows_affected() > 0)
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
                    bail!("mysql returned a negative artifact count")
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

fn row_record(row: sqlx::mysql::MySqlRow) -> Result<ArtifactRecord> {
    let format_version = row.try_get::<i64, _>("format_version")?;
    let lease_generation = row.try_get::<i64, _>("lease_generation")?;
    let claim_attempts = row.try_get::<i64, _>("claim_attempts")?;
    let retry_count = row.try_get::<i64, _>("retry_count")?;
    if !(1..=u32::MAX as i64).contains(&format_version)
        || lease_generation < 0
        || !(0..=u32::MAX as i64).contains(&claim_attempts)
        || !(0..=u32::MAX as i64).contains(&retry_count)
    {
        bail!("mysql artifact scheduler row contains an invalid unsigned value")
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
    use sqlx::mysql::MySqlPoolOptions;

    fn mysql_test_url(test: &str) -> Option<String> {
        match std::env::var("RIPCLONE_TEST_MYSQL_URL") {
            Ok(url) => Some(url),
            Err(_) if std::env::var_os("RIPCLONE_REQUIRE_MYSQL_TESTS").is_some() => {
                panic!("{test} requires RIPCLONE_TEST_MYSQL_URL")
            }
            Err(_) => {
                eprintln!("SKIP {test}: RIPCLONE_TEST_MYSQL_URL unset");
                None
            }
        }
    }

    struct Accept;
    impl CompletionVerifier for Accept {
        fn identity(&self) -> &'static str {
            "mysql-live-conformance-v1"
        }
        fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
            validate_evidence(claim, evidence)
        }
    }
    struct Reject;
    impl CompletionVerifier for Reject {
        fn identity(&self) -> &'static str {
            "mysql-live-conformance-v1"
        }
        fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
            bail!("rejected by test verifier")
        }
    }
    struct Other;
    impl CompletionVerifier for Other {
        fn identity(&self) -> &'static str {
            "mysql-other-verifier-v1"
        }
        fn verify(&self, _: &ClaimedArtifact, _: &CompletionEvidence) -> Result<()> {
            Ok(())
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

    async fn reset(pool: &MySqlPool) {
        for statement in [
            "DROP TABLE IF EXISTS external_source_child",
            "DROP TABLE IF EXISTS external_scheduler_child",
            "DROP TABLE IF EXISTS artifact_intents",
            "DROP TABLE IF EXISTS git_source_consumers",
            "DROP TABLE IF EXISTS branch_source_current",
            "DROP TABLE IF EXISTS branch_source_generations",
            "DROP TABLE IF EXISTS git_source_desires",
            "DROP TABLE IF EXISTS git_source_acquisition_members",
            "DROP TABLE IF EXISTS git_source_acquisitions",
            "DROP TABLE IF EXISTS git_source_acquisition_sequence",
            "DROP TABLE IF EXISTS git_source_maintenance",
            "DROP TABLE IF EXISTS git_source_members",
            "DROP TABLE IF EXISTS git_source_roots",
            "DROP TABLE IF EXISTS ready_publication_fence_rogue",
            "DROP TABLE IF EXISTS ready_publication_fence_members",
            "DROP TABLE IF EXISTS ready_publication_fences",
            "DROP TABLE IF EXISTS ready_publication_fence_sequence",
            "DROP TABLE IF EXISTS artifact_base_retention",
            "DROP TABLE IF EXISTS artifact_gc_sweep",
            "DROP TABLE IF EXISTS artifact_transport_leases",
            "DROP TABLE IF EXISTS artifact_consumers",
            "DROP TABLE IF EXISTS artifact_observations",
            "DROP TABLE IF EXISTS branch_observations",
            "DROP TABLE IF EXISTS artifact_jobs",
            "DROP TABLE IF EXISTS scheduler_state",
            "DROP TABLE IF EXISTS artifact_scheduler_schema",
        ] {
            sqlx::query(statement).execute(pool).await.unwrap();
        }
    }

    async fn seed_v6_transition(pool: &MySqlPool) {
        reset(pool).await;
        for ddl in &SCHEMA[..9] {
            sqlx::raw_sql(*ddl).execute(pool).await.unwrap();
        }
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,?)")
            .bind(V5_TO_V6_DDL)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(pool)
            .await
            .unwrap();
    }

    async fn assert_v6_transition_rejected_unchanged(
        pool: &MySqlPool,
        url: &str,
        expected_fence_tables: i64,
    ) {
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(pool)
            .await
            .unwrap(),
            V5_TO_V6_DDL
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE()
                 AND table_name IN('ready_publication_fence_sequence','ready_publication_fences','ready_publication_fence_members')"
            )
            .fetch_one(pool)
            .await
            .unwrap(),
            expected_fence_tables
        );
    }

    /// This test is intentionally discoverable in ordinary `cargo test` runs.
    /// It reports a visible skip only when no live test server was configured;
    /// CI must set RIPCLONE_TEST_MYSQL_URL and run this exact test name.
    #[tokio::test]
    async fn mysql_artifact_scheduler_live_conformance() {
        let Some(url) = mysql_test_url("mysql_artifact_scheduler_live_conformance") else {
            return;
        };
        let control = MySqlPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .unwrap();
        let mut test_lock = control.acquire().await.unwrap().detach();
        let lock: i64 = sqlx::query_scalar("SELECT GET_LOCK('ripclone_mysql_scheduler_test',30)")
            .fetch_one(&mut test_lock)
            .await
            .unwrap();
        assert_eq!(lock, 1, "test database lock unavailable");
        reset(&control).await;
        for statement in &SCHEMA[..9] {
            sqlx::raw_sql(*statement).execute(&control).await.unwrap();
        }
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        let a_pool = MySqlPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .unwrap();
        let b_pool = MySqlPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .unwrap();
        let (a, b) = tokio::join!(
            MysqlArtifactScheduler::from_pool(a_pool, Default::default(), Arc::new(Accept)),
            MysqlArtifactScheduler::from_pool(b_pool, Default::default(), Arc::new(Accept))
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(&control)
            .await
            .unwrap(),
            6
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='artifact_gc_sweep'").fetch_one(&control).await.unwrap(), 1);

        let target = "a".repeat(40);
        let head=sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('admission-ws','admission/repo',?,'head',1,'ready','head-manifest',0,0)").bind(&target).execute(&control).await.unwrap().last_insert_id() as i64;
        let full=sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('admission-ws','admission/repo',?,'full_history',1,'ready','full-manifest',0,0)").bind(&target).execute(&control).await.unwrap().last_insert_id() as i64;
        let provenance = ActivationFenceProvenance {
            workspace: "admission-ws".into(),
            repo: "admission/repo".into(),
            branch: "main".into(),
            target,
            attempt_id: "live-admission".into(),
        };
        let expected = vec![
            (head, Some("head-manifest".into())),
            (full, Some("full-manifest".into())),
        ];
        let fence = a
            .fence_ready_publications(&expected, &provenance, 60)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            a.quarantine_ready(head, Some("head-manifest"), "race")
                .await
                .unwrap(),
            QuarantineOutcome::LostRace
        );
        assert!(a.mark_activation_unknown(&fence, 1).await.unwrap());
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(
            a.recover_activation_fence(&provenance)
                .await
                .unwrap()
                .is_some()
        );
        MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .expect("exact persisted Ready fence was rejected at startup");
        sqlx::query(
            "UPDATE ready_publication_fences SET operation_id='admission-operation-forged'",
        )
        .execute(&control)
        .await
        .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "forged Ready fence operation provenance was accepted"
        );
        sqlx::query("UPDATE ready_publication_fences SET operation_id=?")
            .bind(provenance.operation_id())
            .execute(&control)
            .await
            .unwrap();
        assert!(
            a.live_scheduler_roots_page(None, 512)
                .await
                .unwrap()
                .iter()
                .any(|r| r.artifact_id == head)
        );
        a.release_ready_publication_fence(fence).await.unwrap();
        assert!(
            matches!(a.quarantine_ready(head,Some("head-manifest"),"verified corrupt").await.unwrap(),QuarantineOutcome::Requeued(id) if id==head)
        );
        sqlx::query("DELETE FROM artifact_jobs WHERE id IN(?,?)")
            .bind(head)
            .bind(full)
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
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !blocked.is_finished(),
            "mysql publication bypassed delete transaction lock"
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

        let mut gc_ids = Vec::new();
        for (commit, manifest) in [
            ("gc-consumer", "gc-a"),
            ("gc-published", "gc-b"),
            ("gc-superseded", "gc-c"),
            ("gc-expired", "gc-d"),
        ] {
            let result = sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,manifest,created_at,updated_at) VALUES('gc-ws','gc/repo',?,'files',1,'ready',?,UNIX_TIMESTAMP(),UNIX_TIMESTAMP())").bind(commit).bind(manifest).execute(a.pool()).await.unwrap();
            gc_ids.push(result.last_insert_id() as i64);
        }
        sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,'admission',UNIX_TIMESTAMP()+60),(?,'expired',UNIX_TIMESTAMP()-1)").bind(gc_ids[0]).bind(gc_ids[3]).execute(a.pool()).await.unwrap();
        sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES('gc-ws','gc/repo','main','files','gc-published',?,1,?,1,UNIX_TIMESTAMP())").bind(gc_ids[1]).bind(gc_ids[1]).execute(a.pool()).await.unwrap();
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
        sqlx::query("DELETE FROM artifact_consumers WHERE artifact_id IN (?,?,?,?)")
            .bind(gc_ids[0])
            .bind(gc_ids[1])
            .bind(gc_ids[2])
            .bind(gc_ids[3])
            .execute(a.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM artifact_jobs WHERE workspace='gc-ws'")
            .execute(a.pool())
            .await
            .unwrap();

        // Exact identity and atomic dedup across independent pools.
        let duplicate = key("dedup", ArtifactKind::Head);
        let (one, two) = tokio::join!(a.schedule(&duplicate), b.schedule(&duplicate));
        assert_eq!(
            [one.unwrap(), two.unwrap()]
                .into_iter()
                .filter(|v| matches!(v, ScheduleOutcome::Enqueued(_)))
                .count(),
            1
        );
        assert!(
            a.schedule(&ArtifactKey {
                format_version: 0,
                ..duplicate.clone()
            })
            .await
            .is_err()
        );
        assert!(
            a.schedule(&ArtifactKey {
                workspace: "x".repeat(129),
                ..duplicate.clone()
            })
            .await
            .is_err()
        );

        // Generation compare-and-swap makes concurrent observations all-or-nothing.
        let (one, two) = tokio::join!(
            a.observe(
                "ws",
                "owner/repo",
                "main",
                "tip",
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                None
            ),
            b.observe(
                "ws",
                "owner/repo",
                "main",
                "other",
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                None
            )
        );
        assert_eq!(
            [&one, &two]
                .into_iter()
                .filter(|v| matches!(v, Ok(ObservationOutcome::Accepted { .. })))
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
            a.counts()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, _, n)| n)
                .sum::<u64>(),
            2
        );

        // Claims are unique; stale generations cannot heartbeat, fail, or publish.
        let first = a.claim("worker-a", 5).await.unwrap().unwrap();
        let second = b.claim("worker-b", 5).await.unwrap().unwrap();
        assert_ne!(first.record.id, second.record.id);
        sqlx::query("UPDATE artifact_jobs SET lease_expires_at=0 WHERE id=?")
            .bind(first.record.id)
            .execute(a.pool())
            .await
            .unwrap();
        assert_eq!(a.reconcile_expired().await.unwrap().0, 1);
        let replacement = a.claim("worker-c", 5).await.unwrap().unwrap();
        assert_eq!(replacement.record.id, first.record.id);
        assert!(replacement.record.lease_generation > first.record.lease_generation);
        assert!(!a.heartbeat(&first, "worker-a", 5).await.unwrap());
        assert!(
            !a.complete(
                &first,
                "worker-a",
                &CompletionEvidence::new(first.record.key.clone(), "stale").unwrap()
            )
            .await
            .unwrap()
        );
        assert!(a.heartbeat(&replacement, "worker-c", 5).await.unwrap());

        let dead = key("dead-letter", ArtifactKind::Head);
        a.schedule(&dead).await.unwrap();
        let dead_claim = a.claim("dead-worker", 5).await.unwrap().unwrap();
        sqlx::query("UPDATE artifact_jobs SET claim_attempts=?,lease_expires_at=0 WHERE id=?")
            .bind(a.limits.max_claim_attempts as i64)
            .bind(dead_claim.record.id)
            .execute(a.pool())
            .await
            .unwrap();
        assert_eq!(a.reconcile_expired().await.unwrap().1, 1);
        assert_eq!(
            a.retry_failed(&dead).await.unwrap(),
            RetryOutcome::NotRetryable(FailureClass::DeadLetter)
        );

        // Current completion publishes only the exact desired artifact.
        assert!(
            a.complete(
                &replacement,
                "worker-c",
                &CompletionEvidence::new(replacement.record.key.clone(), "manifest").unwrap()
            )
            .await
            .unwrap()
        );
        let branch: String = sqlx::query_scalar(
            "SELECT branch FROM artifact_observations WHERE published_artifact_id=? LIMIT 1",
        )
        .bind(replacement.record.id)
        .fetch_one(a.pool())
        .await
        .unwrap();
        let published = a
            .published("ws", "owner/repo", &branch, replacement.record.key.kind, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(published.id, replacement.record.id);

        // A consumer protects queued work from superseding observations until release.
        let held = key("held", ArtifactKind::Head);
        let held_id = outcome_id(&a.subscribe_consumer(&held, "clone-1", 60).await.unwrap());
        a.observe(
            "ws",
            "owner/repo",
            "held",
            "new",
            &[ArtifactKind::Files],
            1,
            None,
        )
        .await
        .unwrap();
        assert!(a.get(held_id).await.unwrap().is_some());
        a.release_consumer(held_id, "clone-1").await.unwrap();
        assert!(a.get(held_id).await.unwrap().is_none());

        // Retry classes and attempt cap are durable and fenced.
        let permanent = key("permanent", ArtifactKind::Head);
        a.schedule(&permanent).await.unwrap();
        let claim = a.claim("permanent-worker", 5).await.unwrap().unwrap();
        assert!(
            a.fail(
                &claim,
                "permanent-worker",
                FailureClass::Permanent,
                "bad input"
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
        let claim = a.claim("retry-worker", 5).await.unwrap().unwrap();
        assert!(
            a.fail(&claim, "retry-worker", FailureClass::Retryable, "transient")
                .await
                .unwrap()
        );
        assert!(matches!(
            a.retry_failed(&retryable).await.unwrap(),
            RetryOutcome::Requeued(_)
        ));

        // A branch advance never lets an older in-flight completion repoint
        // the publication alias.
        reset(&control).await;
        let a = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        a.observe(
            "ws",
            "owner/repo",
            "race",
            "old",
            &[ArtifactKind::Head],
            1,
            None,
        )
        .await
        .unwrap();
        let old = a.claim("old-worker", 5).await.unwrap().unwrap();
        a.observe(
            "ws",
            "owner/repo",
            "race",
            "new",
            &[ArtifactKind::Head],
            1,
            Some(1),
        )
        .await
        .unwrap();
        assert!(
            a.complete(
                &old,
                "old-worker",
                &CompletionEvidence::new(old.record.key.clone(), "old-manifest").unwrap()
            )
            .await
            .unwrap()
        );
        assert!(
            a.published("ws", "owner/repo", "race", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .is_none()
        );
        let new = a.claim("new-worker", 5).await.unwrap().unwrap();
        assert!(
            a.complete(
                &new,
                "new-worker",
                &CompletionEvidence::new(new.record.key.clone(), "new-manifest").unwrap()
            )
            .await
            .unwrap()
        );
        assert_eq!(
            a.published("ws", "owner/repo", "race", ArtifactKind::Head, 1)
                .await
                .unwrap()
                .unwrap()
                .key
                .commit,
            "new"
        );

        let owned_key = key("run-owned", ArtifactKind::Head);
        a.schedule(&owned_key).await.unwrap();
        let owned = a.claim("owned-worker", 5).await.unwrap().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let outcome = ArtifactSchedulerPersistence::run_owned(
            &a,
            &owned,
            "owned-worker",
            vec![crate::artifact_scheduler::ArtifactTask::cooperative(
                |context| async move {
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
        assert_eq!(outcome, crate::artifact_scheduler::ExecutionOutcome::Ready);

        // Verifier failure leaves the lease live; a worker can explicitly fail it.
        let rejecting = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Reject),
        )
        .await
        .unwrap();
        let rejected = key("rejected", ArtifactKind::Head);
        rejecting.schedule(&rejected).await.unwrap();
        let claim = rejecting.claim("reject-worker", 5).await.unwrap().unwrap();
        assert!(
            rejecting
                .complete(
                    &claim,
                    "reject-worker",
                    &CompletionEvidence::new(claim.record.key.clone(), "bad").unwrap()
                )
                .await
                .is_err()
        );
        assert!(rejecting.owns(&claim, "reject-worker").await.unwrap());
        assert!(
            rejecting
                .fail(
                    &claim,
                    "reject-worker",
                    FailureClass::Permanent,
                    "verification failed"
                )
                .await
                .unwrap()
        );

        // Fleet provenance is immutable once any state exists.
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Other)
            )
            .await
            .is_err()
        );
        sqlx::query("UPDATE scheduler_state SET config_fingerprint='' ")
            .execute(a.pool())
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err()
        );

        // Multi-kind observation capacity is atomic. Running caps and the
        // per-kind same-repo exclusion applies across independent workers,
        // while FullHistory and Files remain independent.
        reset(&control).await;
        let tiny_limits = SchedulerLimits {
            total_backlog: 1,
            workspace_backlog: 1,
            head_reserved: 0,
            head_backlog: 1,
            full_history_backlog: 1,
            files_backlog: 1,
            total_running: 3,
            head_running: 1,
            full_history_running: 1,
            files_running: 1,
            workspace_running: 3,
            ..Default::default()
        };
        let tiny = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            tiny_limits,
            Arc::new(Accept),
        )
        .await
        .unwrap();
        assert!(
            tiny.observe(
                "tiny",
                "owner/repo",
                "main",
                "tip",
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                None
            )
            .await
            .is_err()
        );
        assert!(tiny.counts().await.unwrap().is_empty());

        reset(&control).await;
        let cap_limits = SchedulerLimits {
            total_backlog: 8,
            workspace_backlog: 8,
            head_reserved: 1,
            head_backlog: 4,
            full_history_backlog: 2,
            files_backlog: 2,
            total_running: 4,
            head_running: 2,
            full_history_running: 1,
            files_running: 1,
            workspace_running: 3,
            ..Default::default()
        };
        let capped = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            cap_limits,
            Arc::new(Accept),
        )
        .await
        .unwrap();
        capped
            .schedule(&key("expensive", ArtifactKind::FullHistory))
            .await
            .unwrap();
        capped
            .schedule(&key("expensive", ArtifactKind::Files))
            .await
            .unwrap();
        let expensive = capped.claim("expensive-worker", 5).await.unwrap().unwrap();
        assert!(expensive.record.key.kind.expensive());
        let sibling = capped
            .claim("independent-same-repo", 5)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(expensive.record.key.kind, sibling.record.key.kind);
        capped
            .schedule(&key("same-kind-newer", expensive.record.key.kind))
            .await
            .unwrap();
        assert!(
            capped
                .claim("blocked-same-kind", 5)
                .await
                .unwrap()
                .is_none()
        );
        let mut other_head = key("other-head", ArtifactKind::Head);
        other_head.workspace = "other-workspace".into();
        capped.schedule(&other_head).await.unwrap();
        assert_eq!(
            capped
                .claim("head-worker", 5)
                .await
                .unwrap()
                .unwrap()
                .record
                .key
                .kind,
            ArtifactKind::Head
        );

        // Exact schema validation rejects missing defaults, ineffective named
        // constraints, and lookalike column shapes.
        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query("ALTER TABLE artifact_jobs ALTER COLUMN lease_generation DROP DEFAULT")
            .execute(clean.pool())
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err()
        );

        reset(&control).await;
        for statement in SCHEMA {
            sqlx::raw_sql(*statement).execute(&control).await.unwrap();
        }
        sqlx::query("DROP TABLE artifact_base_retention")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "mysql v3 missing base-retention table was repaired"
        );

        reset(&control).await;
        for statement in SCHEMA {
            sqlx::raw_sql(*statement).execute(&control).await.unwrap();
        }
        sqlx::query("DROP INDEX artifact_base_retention_scope ON artifact_base_retention")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "mysql v3 missing base-retention index was repaired"
        );

        reset(&control).await;
        for statement in SCHEMA {
            sqlx::raw_sql(*statement).execute(&control).await.unwrap();
        }
        sqlx::query("ALTER TABLE artifact_base_retention DROP CHECK artifact_base_retention_ranks")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,3)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "mysql v3 missing base-retention constraint was repaired"
        );

        reset(&control).await;
        for statement in SCHEMA {
            sqlx::raw_sql(*statement).execute(&control).await.unwrap();
        }
        sqlx::query("DROP TABLE artifact_gc_sweep")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,4)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "partial mysql v4 without GC table was accepted"
        );

        reset(&control).await;
        for (index, statement) in SCHEMA.iter().enumerate() {
            if index < 6 || index == 8 {
                sqlx::raw_sql(*statement).execute(&control).await.unwrap();
            }
        }
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,2)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        let migrated_v2 = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
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
        for statement in &SCHEMA[..9] {
            sqlx::raw_sql(*statement).execute(&control).await.unwrap();
        }
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,4)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        let migrated_v4 = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
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

        for stage in ["marker_only", "base_only", "both_empty", "partial", "full"] {
            reset(&control).await;
            for (index, statement) in SCHEMA.iter().enumerate() {
                if index < 6 || index == 8 {
                    sqlx::raw_sql(*statement).execute(&control).await.unwrap();
                }
            }
            sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,?)")
                .bind(V2_TO_V4_TRANSITION)
                .execute(&control)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO scheduler_state(id,fairness_cursor,config_fingerprint) VALUES(1,0,?)",
            )
            .bind(scheduler_fingerprint(
                &Default::default(),
                "mysql-live-conformance-v1",
            ))
            .execute(&control)
            .await
            .unwrap();
            for (kind, manifest) in [("head", "a".repeat(64)), ("full_history", "b".repeat(64))] {
                sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,lease_generation,claim_attempts,retry_count,manifest,created_at,updated_at) VALUES('ws','owner/repo','crash-base',?,1,'ready',0,0,0,?,1,1)")
                    .bind(kind).bind(manifest).execute(&control).await.unwrap();
            }
            if stage != "marker_only" {
                sqlx::raw_sql(SCHEMA[6]).execute(&control).await.unwrap();
            }
            if !["marker_only", "base_only"].contains(&stage) {
                sqlx::raw_sql(SCHEMA[7]).execute(&control).await.unwrap();
            }
            if stage == "partial" || stage == "full" {
                let head_id: i64 =
                    sqlx::query_scalar("SELECT id FROM artifact_jobs WHERE kind='head'")
                        .fetch_one(&control)
                        .await
                        .unwrap();
                sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank) VALUES(?,'ws','owner/repo',1,8,NULL)")
                    .bind(head_id).execute(&control).await.unwrap();
                if stage == "full" {
                    let history_id: i64 = sqlx::query_scalar(
                        "SELECT id FROM artifact_jobs WHERE kind='full_history'",
                    )
                    .fetch_one(&control)
                    .await
                    .unwrap();
                    sqlx::query("UPDATE artifact_base_retention SET head_rank=1,pair_rank=1 WHERE artifact_id=?").bind(head_id).execute(&control).await.unwrap();
                    sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank) VALUES(?,'ws','owner/repo',1,NULL,1)").bind(history_id).execute(&control).await.unwrap();
                }
            }
            let first_pool = MySqlPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .unwrap();
            let second_pool = MySqlPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .unwrap();
            let (first, second) = tokio::join!(
                MysqlArtifactScheduler::from_pool(first_pool, Default::default(), Arc::new(Accept)),
                MysqlArtifactScheduler::from_pool(
                    second_pool,
                    Default::default(),
                    Arc::new(Accept)
                )
            );
            assert!(
                first.is_ok() && second.is_ok(),
                "concurrent transition resume failed at {stage}: {:?} / {:?}",
                first.err(),
                second.err()
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT version FROM artifact_scheduler_schema WHERE id=1"
                )
                .fetch_one(&control)
                .await
                .unwrap(),
                6
            );
            let rows:Vec<(String,Option<i16>,Option<i16>)>=sqlx::query_as("SELECT j.kind,r.head_rank,r.pair_rank FROM artifact_base_retention r JOIN artifact_jobs j ON j.id=r.artifact_id ORDER BY j.kind").fetch_all(&control).await.unwrap();
            assert_eq!(
                rows,
                vec![
                    ("full_history".into(), None, Some(1)),
                    ("head".into(), Some(1), Some(1))
                ],
                "full backfill mismatch after {stage}"
            );
        }

        for stage in [
            "marker_only",
            "transport_only",
            "base_only",
            "both_empty",
            "partial",
            "full",
        ] {
            reset(&control).await;
            for (index, statement) in SCHEMA.iter().enumerate() {
                if index < 5 || index == 8 {
                    sqlx::raw_sql(*statement).execute(&control).await.unwrap();
                }
            }
            sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,?)")
                .bind(V1_TO_V4_TRANSITION)
                .execute(&control)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO scheduler_state(id,fairness_cursor,config_fingerprint) VALUES(1,0,?)",
            )
            .bind(scheduler_fingerprint(
                &Default::default(),
                "mysql-live-conformance-v1",
            ))
            .execute(&control)
            .await
            .unwrap();
            for (kind, manifest) in [("head", "c".repeat(64)), ("full_history", "d".repeat(64))] {
                sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,lease_generation,claim_attempts,retry_count,manifest,created_at,updated_at) VALUES('ws','owner/repo','v1-crash-base',?,1,'ready',0,0,0,?,1,1)").bind(kind).bind(manifest).execute(&control).await.unwrap();
            }
            if stage != "marker_only" {
                sqlx::raw_sql(SCHEMA[5]).execute(&control).await.unwrap();
            }
            if !["marker_only", "transport_only"].contains(&stage) {
                sqlx::raw_sql(SCHEMA[6]).execute(&control).await.unwrap();
            }
            if !["marker_only", "transport_only", "base_only"].contains(&stage) {
                sqlx::raw_sql(SCHEMA[7]).execute(&control).await.unwrap();
            }
            if stage == "partial" || stage == "full" {
                let head_id: i64 =
                    sqlx::query_scalar("SELECT id FROM artifact_jobs WHERE kind='head'")
                        .fetch_one(&control)
                        .await
                        .unwrap();
                sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank) VALUES(?,'ws','owner/repo',1,8,NULL)").bind(head_id).execute(&control).await.unwrap();
                if stage == "full" {
                    let history_id: i64 = sqlx::query_scalar(
                        "SELECT id FROM artifact_jobs WHERE kind='full_history'",
                    )
                    .fetch_one(&control)
                    .await
                    .unwrap();
                    sqlx::query("UPDATE artifact_base_retention SET head_rank=1,pair_rank=1 WHERE artifact_id=?").bind(head_id).execute(&control).await.unwrap();
                    sqlx::query("INSERT INTO artifact_base_retention(artifact_id,workspace,repo,format_version,head_rank,pair_rank) VALUES(?,'ws','owner/repo',1,NULL,1)").bind(history_id).execute(&control).await.unwrap();
                }
            }
            let first_pool = MySqlPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .unwrap();
            let second_pool = MySqlPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .unwrap();
            let (first, second) = tokio::join!(
                MysqlArtifactScheduler::from_pool(first_pool, Default::default(), Arc::new(Accept)),
                MysqlArtifactScheduler::from_pool(
                    second_pool,
                    Default::default(),
                    Arc::new(Accept)
                )
            );
            assert!(
                first.is_ok() && second.is_ok(),
                "concurrent v1 transition resume failed at {stage}: {:?} / {:?}",
                first.err(),
                second.err()
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT version FROM artifact_scheduler_schema WHERE id=1"
                )
                .fetch_one(&control)
                .await
                .unwrap(),
                6
            );
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='artifact_transport_leases'").fetch_one(&control).await.unwrap(),1);
            let rows:Vec<(String,Option<i16>,Option<i16>)>=sqlx::query_as("SELECT j.kind,r.head_rank,r.pair_rank FROM artifact_base_retention r JOIN artifact_jobs j ON j.id=r.artifact_id ORDER BY j.kind").fetch_all(&control).await.unwrap();
            assert_eq!(
                rows,
                vec![
                    ("full_history".into(), None, Some(1)),
                    ("head".into(), Some(1), Some(1))
                ],
                "v1 full backfill mismatch after {stage}"
            );
        }

        reset(&control).await;
        for statement in SCHEMA {
            sqlx::raw_sql(*statement).execute(&control).await.unwrap();
        }
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,99)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "future mysql schema was accepted"
        );

        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query("ALTER TABLE artifact_jobs ALTER CHECK artifact_jobs_state NOT ENFORCED")
            .execute(clean.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO artifact_jobs(
               workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at)
             VALUES('ws','owner/repo','invalid','head',1,'invalid-state',1,1)",
        )
        .execute(clean.pool())
        .await
        .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "a disabled CHECK admitted invalid state and was accepted on reopen"
        );

        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query("ALTER TABLE artifact_jobs ALTER INDEX artifact_jobs_claim INVISIBLE")
            .execute(clean.pool())
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "an invisible required claim index was accepted"
        );

        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query(
            "ALTER TABLE artifact_jobs DROP INDEX artifact_jobs_claim,
             ADD INDEX artifact_jobs_claim(state DESC,kind,created_at,id)",
        )
        .execute(clean.pool())
        .await
        .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "a DESC replacement for the required ASC claim index was accepted"
        );

        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        let indexes_before: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT index_name) FROM information_schema.statistics
             WHERE table_schema=DATABASE() AND table_name='artifact_consumers'",
        )
        .fetch_one(clean.pool())
        .await
        .unwrap();
        sqlx::query(
            "ALTER TABLE artifact_consumers ADD CONSTRAINT planted_fk
             FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id)",
        )
        .execute(clean.pool())
        .await
        .unwrap();
        let indexes_after: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT index_name) FROM information_schema.statistics
             WHERE table_schema=DATABASE() AND table_name='artifact_consumers'",
        )
        .fetch_one(clean.pool())
        .await
        .unwrap();
        assert_eq!(
            indexes_after, indexes_before,
            "the FK did not reuse the expected PK index"
        );
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "an unexpected foreign-key constraint was accepted"
        );

        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        let blocked_key = key("externally-blocked", ArtifactKind::Head);
        let blocked_id = outcome_id(&clean.schedule(&blocked_key).await.unwrap());
        sqlx::query(
            "CREATE TABLE external_scheduler_child(
               artifact_id BIGINT NOT NULL PRIMARY KEY,
               CONSTRAINT external_scheduler_fk FOREIGN KEY(artifact_id)
                 REFERENCES artifact_jobs(id) ON DELETE RESTRICT) ENGINE=InnoDB",
        )
        .execute(clean.pool())
        .await
        .unwrap();
        sqlx::query("INSERT INTO external_scheduler_child(artifact_id) VALUES(?)")
            .bind(blocked_id)
            .execute(clean.pool())
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "an incoming FK owned by an external table was accepted"
        );
        assert!(
            clean
                .observe(
                    "ws",
                    "owner/repo",
                    "external-fk",
                    "replacement",
                    &[ArtifactKind::Head],
                    1,
                    None
                )
                .await
                .is_err(),
            "ON DELETE RESTRICT did not demonstrate the supersession hazard"
        );
        assert!(clean.get(blocked_id).await.unwrap().is_some());
        assert!(
            clean
                .get_by_key(&key("replacement", ArtifactKind::Head))
                .await
                .unwrap()
                .is_none(),
            "failed supersession did not roll back atomically"
        );

        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query(
            "ALTER TABLE artifact_jobs DROP CHECK artifact_jobs_format,
             ADD CONSTRAINT artifact_jobs_format CHECK(true OR format_version BETWEEN 1 AND 4294967295)",
        )
        .execute(clean.pool())
        .await
        .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err()
        );

        reset(&control).await;
        let clean = MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::query("ALTER TABLE artifact_jobs MODIFY workspace VARCHAR(127) NOT NULL")
            .execute(clean.pool())
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err()
        );

        reset(&control).await;
        let released: Option<i64> =
            sqlx::query_scalar("SELECT RELEASE_LOCK('ripclone_mysql_scheduler_test')")
                .fetch_one(&mut test_lock)
                .await
                .unwrap();
        assert_eq!(released, Some(1));
        eprintln!("EXECUTED mysql_artifact_scheduler_live_conformance");
    }

    #[tokio::test]
    async fn mysql_v6_transition_boundaries_live() {
        let Some(url) = mysql_test_url("mysql_v6_transition_boundaries_live") else {
            return;
        };
        let control = MySqlPoolOptions::new()
            .max_connections(12)
            .connect(&url)
            .await
            .unwrap();
        let mut lock_connection = control.acquire().await.unwrap().detach();
        let lock: i64 = sqlx::query_scalar("SELECT GET_LOCK('ripclone_mysql_scheduler_test',30)")
            .fetch_one(&mut lock_connection)
            .await
            .unwrap();
        assert_eq!(lock, 1);
        for stage in 0..=5 {
            reset(&control).await;
            for ddl in &SCHEMA[..9] {
                sqlx::raw_sql(*ddl).execute(&control).await.unwrap();
            }
            let marker = if stage == 5 {
                V5_TO_V6_VALIDATED
            } else {
                V5_TO_V6_DDL
            };
            sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,?)")
                .bind(marker)
                .execute(&control)
                .await
                .unwrap();
            sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
                .execute(&control)
                .await
                .unwrap();
            if stage >= 1 {
                sqlx::raw_sql(SCHEMA[9]).execute(&control).await.unwrap();
            }
            if stage >= 2 {
                sqlx::raw_sql(SCHEMA[10]).execute(&control).await.unwrap();
            }
            if stage >= 3 {
                sqlx::raw_sql(SCHEMA[11]).execute(&control).await.unwrap();
            }
            if stage >= 4 {
                sqlx::query(
                    "INSERT INTO ready_publication_fence_sequence(id,generation) VALUES(1,0)",
                )
                .execute(&control)
                .await
                .unwrap();
            }
            if matches!(stage, 1 | 5) {
                let p1 = MySqlPoolOptions::new().connect(&url).await.unwrap();
                let p2 = MySqlPoolOptions::new().connect(&url).await.unwrap();
                let (a, b) = tokio::join!(
                    MysqlArtifactScheduler::from_pool(p1, Default::default(), Arc::new(Accept)),
                    MysqlArtifactScheduler::from_pool(p2, Default::default(), Arc::new(Accept))
                );
                assert!(
                    a.is_ok() && b.is_ok(),
                    "concurrent resume failed at stage {stage}"
                );
            } else {
                MysqlArtifactScheduler::from_pool(
                    MySqlPoolOptions::new().connect(&url).await.unwrap(),
                    Default::default(),
                    Arc::new(Accept),
                )
                .await
                .unwrap();
            }
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT version FROM artifact_scheduler_schema WHERE id=1"
                )
                .fetch_one(&control)
                .await
                .unwrap(),
                SCHEMA_VERSION,
                "stage {stage}"
            );
        }
        reset(&control).await;
        for ddl in &SCHEMA[..9] {
            sqlx::raw_sql(*ddl).execute(&control).await.unwrap();
        }
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,?)")
            .bind(V5_TO_V6_DDL)
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE ready_publication_fence_sequence(id SMALLINT PRIMARY KEY,bogus BIGINT)",
        )
        .execute(&control)
        .await
        .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "forged partial sequence was accepted"
        );

        seed_v6_transition(&control).await;
        sqlx::raw_sql(SCHEMA[9]).execute(&control).await.unwrap();
        sqlx::raw_sql(SCHEMA[10]).execute(&control).await.unwrap();
        sqlx::raw_sql(
            "ALTER TABLE ready_publication_fences
             DROP CHECK ready_fences_generation,
             ADD CONSTRAINT ready_fences_generation CHECK(generation>=0)",
        )
        .execute(&control)
        .await
        .unwrap();
        assert_v6_transition_rejected_unchanged(&control, &url, 2).await;

        seed_v6_transition(&control).await;
        sqlx::raw_sql(SCHEMA[9]).execute(&control).await.unwrap();
        sqlx::raw_sql(SCHEMA[10]).execute(&control).await.unwrap();
        sqlx::query(
            "INSERT INTO ready_publication_fences
             (token,generation,operation_id,workspace,repo,branch,target,attempt_id,expires_at,state)
             VALUES(REPEAT('a',64),1,'forged-operation','ws','repo','main',REPEAT('b',40),'attempt',1,'held')",
        )
        .execute(&control)
        .await
        .unwrap();
        assert_v6_transition_rejected_unchanged(&control, &url, 2).await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ready_publication_fences")
                .fetch_one(&control)
                .await
                .unwrap(),
            1
        );

        seed_v6_transition(&control).await;
        sqlx::raw_sql(SCHEMA[9]).execute(&control).await.unwrap();
        sqlx::raw_sql(SCHEMA[10]).execute(&control).await.unwrap();
        sqlx::raw_sql(
            "ALTER TABLE ready_publication_fences
             DROP INDEX ready_publication_fences_recovery,
             ADD INDEX ready_publication_fences_recovery(state,token,generation)",
        )
        .execute(&control)
        .await
        .unwrap();
        assert_v6_transition_rejected_unchanged(&control, &url, 2).await;

        seed_v6_transition(&control).await;
        for ddl in &SCHEMA[9..12] {
            sqlx::raw_sql(*ddl).execute(&control).await.unwrap();
        }
        sqlx::raw_sql(
            "ALTER TABLE ready_publication_fence_members
             DROP FOREIGN KEY ready_fence_members_parent",
        )
        .execute(&control)
        .await
        .unwrap();
        sqlx::raw_sql(
            "ALTER TABLE ready_publication_fence_members
             ADD CONSTRAINT ready_fence_members_parent FOREIGN KEY(token,generation)
               REFERENCES ready_publication_fences(token,generation) ON DELETE RESTRICT",
        )
        .execute(&control)
        .await
        .unwrap();
        assert_v6_transition_rejected_unchanged(&control, &url, 3).await;

        reset(&control).await;
        for ddl in &SCHEMA[..9] {
            sqlx::raw_sql(*ddl).execute(&control).await.unwrap();
        }
        sqlx::query("INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,?)")
            .bind(V5_TO_V6_DDL)
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)")
            .execute(&control)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE ready_publication_fence_rogue(id BIGINT)")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "extra Ready fence DDL was accepted"
        );
        reset(&control).await;
        let _: Option<i64> =
            sqlx::query_scalar("SELECT RELEASE_LOCK('ripclone_mysql_scheduler_test')")
                .fetch_one(&mut lock_connection)
                .await
                .unwrap();
    }

    #[tokio::test]
    async fn mysql_v7_source_registry_transition_live() {
        let Some(url) = mysql_test_url("mysql_v7_source_registry_transition_live") else {
            return;
        };
        let control = MySqlPoolOptions::new()
            .max_connections(12)
            .connect(&url)
            .await
            .unwrap();
        let mut lock = control.acquire().await.unwrap().detach();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT GET_LOCK('ripclone_mysql_scheduler_test',30)")
                .fetch_one(&mut lock)
                .await
                .unwrap(),
            1
        );
        const TABLES: &[&str] = &[
            "git_source_roots",
            "git_source_members",
            "git_source_acquisition_sequence",
            "git_source_acquisitions",
            "git_source_acquisition_members",
            "git_source_desires",
            "branch_source_generations",
            "branch_source_current",
            "git_source_consumers",
            "artifact_intents",
            "git_source_maintenance",
        ];
        for keep in 0..=TABLES.len() {
            reset(&control).await;
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept),
            )
            .await
            .unwrap();
            sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1")
                .bind(V6_TO_V7_DDL)
                .execute(&control)
                .await
                .unwrap();
            for table in TABLES[keep..].iter().rev() {
                sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
                    .execute(&control)
                    .await
                    .unwrap();
            }
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept),
            )
            .await
            .unwrap();
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT version FROM artifact_scheduler_schema WHERE id=1"
                )
                .fetch_one(&control)
                .await
                .unwrap(),
                SCHEMA_VERSION,
                "v7 prefix {keep}"
            );
        }
        sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1")
            .bind(V6_TO_V7_VALIDATED)
            .execute(&control)
            .await
            .unwrap();
        MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(&control)
            .await
            .unwrap(),
            SCHEMA_VERSION
        );

        sqlx::query("UPDATE artifact_scheduler_schema SET version=? WHERE id=1")
            .bind(V6_TO_V7_DDL)
            .execute(&control)
            .await
            .unwrap();
        sqlx::raw_sql("ALTER TABLE git_source_roots MODIFY semantic_digest VARCHAR(63) NOT NULL")
            .execute(&control)
            .await
            .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1"
            )
            .fetch_one(&control)
            .await
            .unwrap(),
            V6_TO_V7_DDL
        );
        assert_eq!(sqlx::query_scalar::<_,String>("SELECT lower(column_type) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='git_source_roots' AND column_name='semantic_digest'").fetch_one(&control).await.unwrap(),"varchar(63)");
        reset(&control).await;
        MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::raw_sql("ALTER TABLE git_source_roots DROP CHECK git_source_roots_shape,ADD CONSTRAINT git_source_roots_shape CHECK(true)").execute(&control).await.unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "same-count weakened v7 CHECK was accepted"
        );
        reset(&control).await;
        MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::raw_sql("ALTER TABLE git_source_members DROP FOREIGN KEY git_source_members_root")
            .execute(&control)
            .await
            .unwrap();
        sqlx::raw_sql("ALTER TABLE git_source_members ADD CONSTRAINT git_source_members_root FOREIGN KEY(root_hash) REFERENCES git_source_roots(root_hash) ON DELETE CASCADE").execute(&control).await.unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "wrong v7 FK delete rule was accepted"
        );
        reset(&control).await;
        MysqlArtifactScheduler::from_pool(
            MySqlPoolOptions::new().connect(&url).await.unwrap(),
            Default::default(),
            Arc::new(Accept),
        )
        .await
        .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE external_source_child(
               root_hash VARCHAR(64) NOT NULL PRIMARY KEY,
               CONSTRAINT external_source_fk FOREIGN KEY(root_hash)
                 REFERENCES git_source_roots(root_hash) ON DELETE RESTRICT
             ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
        )
        .execute(&control)
        .await
        .unwrap();
        assert!(
            MysqlArtifactScheduler::from_pool(
                MySqlPoolOptions::new().connect(&url).await.unwrap(),
                Default::default(),
                Arc::new(Accept)
            )
            .await
            .is_err(),
            "external incoming v7 FK was accepted"
        );
        reset(&control).await;
        let _: Option<i64> =
            sqlx::query_scalar("SELECT RELEASE_LOCK('ripclone_mysql_scheduler_test')")
                .fetch_one(&mut lock)
                .await
                .unwrap();
    }
}
