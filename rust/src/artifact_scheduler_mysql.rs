//! MySQL persistence for the normalized artifact scheduler.
//!
//! Admission and claim transactions lock the singleton scheduler control row.
//! The lock is held only while touching normalized rows; heartbeats and fenced
//! settlement are O(1) conditional updates and do not take the control lock.

use crate::artifact_scheduler::{
    ArtifactKey, ArtifactKind, ArtifactRecord, ArtifactState, ClaimedArtifact, CompletionEvidence,
    CompletionVerifier, FailureClass, ObservationOutcome, RetryOutcome, ScheduleOutcome,
    SchedulerLimits, scheduler_fingerprint, validate_evidence, validate_lease, validate_limits,
};
use crate::artifact_scheduler_backend::ArtifactSchedulerPersistence;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sqlx::mysql::MySqlPool;
use sqlx::{Acquire, MySql, Row, Transaction};
use std::sync::Arc;

const SCHEMA_VERSION: i64 = 1;
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
    r#"CREATE TABLE IF NOT EXISTS scheduler_state(
 id SMALLINT NOT NULL PRIMARY KEY, fairness_cursor BIGINT NOT NULL,
 workspace_cursor VARCHAR(128) NOT NULL DEFAULT '',config_fingerprint VARCHAR(512) NOT NULL DEFAULT '',
 CONSTRAINT scheduler_state_singleton CHECK(id=1),
 CONSTRAINT scheduler_state_fairness CHECK(fairness_cursor BETWEEN 0 AND 3)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
];

#[derive(Clone)]
pub struct MysqlArtifactScheduler {
    pool: MySqlPool,
    limits: SchedulerLimits,
    verifier: Arc<dyn CompletionVerifier>,
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
            for statement in SCHEMA {
                sqlx::raw_sql(*statement).execute(&mut connection).await?;
            }
            sqlx::query(
                "INSERT INTO scheduler_state(id,fairness_cursor) VALUES(1,0)
                 ON DUPLICATE KEY UPDATE id=VALUES(id)",
            )
            .execute(&mut connection)
            .await?;
            sqlx::query(
                "INSERT INTO artifact_scheduler_schema(id,version) VALUES(1,?)
                 ON DUPLICATE KEY UPDATE id=VALUES(id)",
            )
            .bind(SCHEMA_VERSION)
            .execute(&mut connection)
            .await?;

            let mut migration = connection.begin().await?;
            let version: i64 = sqlx::query_scalar(
                "SELECT version FROM artifact_scheduler_schema WHERE id=1 FOR UPDATE",
            )
            .fetch_one(&mut *migration)
            .await?;
            if version > SCHEMA_VERSION {
                bail!("artifact scheduler database is newer than this binary")
            }
            if version != SCHEMA_VERSION {
                bail!("unsupported mysql artifact scheduler schema {version}")
            }
            Self::validate_schema(&mut migration).await?;

            let fingerprint = scheduler_fingerprint(&limits, verifier_id);
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
                          + (SELECT count(*) FROM artifact_consumers)",
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
        Ok(Self {
            pool,
            limits,
            verifier,
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
              'artifact_observations','artifact_consumers','scheduler_state')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if count != COLUMNS.len() as i64 {
            bail!("mysql artifact scheduler schema has unexpected or missing columns")
        }
        let invalid_tables: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.tables
             WHERE table_schema=DATABASE() AND table_name IN
             ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','scheduler_state')
               AND (engine<>'InnoDB' OR table_collation<>'utf8mb4_bin')",
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
              'artifact_observations','artifact_consumers','scheduler_state')
               AND data_type IN('varchar','text','longtext')
               AND collation_name<>'utf8mb4_bin'",
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
            ("scheduler_state", "PRIMARY", "id", true),
        ];
        let index_count: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT table_name,index_name) FROM information_schema.statistics
             WHERE table_schema=DATABASE() AND table_name IN
             ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','scheduler_state')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if index_count != INDEXES.len() as i64 {
            bail!("mysql artifact scheduler schema has unexpected or missing indexes")
        }
        let invalid_index_parts: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.statistics
             WHERE table_schema=DATABASE() AND table_name IN
             ('artifact_scheduler_schema','artifact_jobs','branch_observations',
              'artifact_observations','artifact_consumers','scheduler_state')
               AND (sub_part IS NOT NULL OR index_type<>'BTREE')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if invalid_index_parts != 0 {
            bail!("mysql artifact scheduler indexes must be full-column BTREE indexes")
        }
        for (table, name, columns, unique) in INDEXES {
            let found: Option<(String,i64)> = sqlx::query_as(
                "SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ','),MIN(non_unique)
                 FROM information_schema.statistics WHERE table_schema=DATABASE()
                   AND table_name=? AND index_name=? GROUP BY index_name",
            ).bind(table).bind(name).fetch_optional(&mut **tx).await?;
            if found.as_ref().map(|(c, n)| (c.as_str(), *n == 0)) != Some((*columns, *unique)) {
                bail!("mysql artifact scheduler index definition mismatch for {table}.{name}")
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
        ];
        let check_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.table_constraints
             WHERE constraint_schema=DATABASE() AND constraint_type='CHECK'
               AND table_name IN ('artifact_scheduler_schema','artifact_jobs','branch_observations',
                 'artifact_observations','scheduler_state')",
        )
        .fetch_one(&mut **tx)
        .await?;
        if check_count != CHECKS.len() as i64 {
            bail!("mysql artifact scheduler schema has unexpected or missing checks")
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

#[async_trait]
impl ArtifactSchedulerPersistence for MysqlArtifactScheduler {
    async fn schedule(&self, key: &ArtifactKey) -> Result<ScheduleOutcome> {
        validate_mysql_key(key)?;
        let (mut tx, now) = self.controlled().await?;
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
        if consumer_id.trim().is_empty() {
            bail!("artifact consumer id is empty")
        }
        check_mysql_len("consumer id", consumer_id, 255)?;
        if !(2..=86400).contains(&ttl_secs) {
            bail!("consumer subscription TTL is invalid")
        }
        let (mut tx, now) = self.controlled().await?;
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
        let current: Option<i64> = sqlx::query_scalar(
            "SELECT generation FROM branch_observations
             WHERE workspace=? AND repo=? AND branch=?",
        )
        .bind(workspace)
        .bind(repo)
        .bind(branch)
        .fetch_optional(&mut *tx)
        .await?;
        let current = current.map(|value| value as u64);
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
                             AND r.kind IN('full_history','files'))
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

    async fn complete(
        &self,
        claim: &ClaimedArtifact,
        owner: &str,
        evidence: &CompletionEvidence,
    ) -> Result<bool> {
        check_mysql_len("lease owner", owner, 255)?;
        validate_evidence(claim, evidence)?;
        self.verifier.verify(claim, evidence)?;
        let mut tx = self.pool.begin().await?;
        let now: i64 = sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
            .fetch_one(&mut *tx)
            .await?;
        let won = sqlx::query(
            "UPDATE artifact_jobs SET state='ready',owner=NULL,heartbeat_at=NULL,
                lease_expires_at=NULL,manifest=?,error=NULL,failure_class=NULL,updated_at=?
             WHERE id=? AND state='running' AND owner=? AND lease_generation=?
               AND lease_expires_at>=?",
        )
        .bind(&evidence.manifest)
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

    /// This test is intentionally discoverable in ordinary `cargo test` runs.
    /// It reports a visible skip only when no live test server was configured;
    /// CI must set RIPCLONE_TEST_MYSQL_URL and run this exact test name.
    #[tokio::test]
    async fn mysql_artifact_scheduler_live_conformance() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
            eprintln!(
                "SKIP mysql_artifact_scheduler_live_conformance: RIPCLONE_TEST_MYSQL_URL unset"
            );
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
        // same-repo expensive exclusion apply across independent workers.
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
        assert!(
            capped
                .claim("blocked-same-repo", 5)
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

        // Exact schema validation rejects a lookalike mutation, not merely missing names.
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
}
