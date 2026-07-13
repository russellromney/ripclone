use super::*;
use sqlx::MySqlConnection;
use sqlx::mysql::MySqlPool;

pub(crate) const MYSQL_V7_TABLES: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS git_source_roots(root_hash VARCHAR(64) NOT NULL PRIMARY KEY,root_len BIGINT NOT NULL,workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,commit_oid VARCHAR(64) NOT NULL,source_format_version BIGINT NOT NULL,object_format VARCHAR(8) NOT NULL,semantic_digest VARCHAR(64) NOT NULL,object_set_digest VARCHAR(64) NOT NULL,object_count BIGINT NOT NULL,total_bytes BIGINT NOT NULL,registration_operation VARCHAR(96) NOT NULL UNIQUE,registration_generation BIGINT NOT NULL UNIQUE,state VARCHAR(16) NOT NULL,created_at BIGINT NOT NULL,registered_at BIGINT NOT NULL,CONSTRAINT git_source_roots_identity UNIQUE(workspace,repo,commit_oid,source_format_version),CONSTRAINT git_source_roots_binding UNIQUE(root_hash,workspace,repo,commit_oid,source_format_version),CONSTRAINT git_source_roots_shape CHECK(root_len>0 AND source_format_version BETWEEN 1 AND 4294967295 AND object_format IN('sha1','sha256') AND CHAR_LENGTH(semantic_digest)=64 AND CHAR_LENGTH(object_set_digest)=64 AND object_count>0 AND total_bytes>0 AND registration_generation>0 AND state IN('registered','quarantined'))) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS git_source_members(root_hash VARCHAR(64) NOT NULL,ordinal BIGINT NOT NULL,child_hash VARCHAR(64) NOT NULL,child_len BIGINT NOT NULL,kind VARCHAR(8) NOT NULL,PRIMARY KEY(root_hash,ordinal),CONSTRAINT git_source_members_identity UNIQUE(root_hash,child_hash),CONSTRAINT git_source_members_shape CHECK(ordinal>=0 AND child_len>0 AND kind IN('pack','index') AND CHAR_LENGTH(child_hash)=64),CONSTRAINT git_source_members_root FOREIGN KEY(root_hash) REFERENCES git_source_roots(root_hash) ON DELETE RESTRICT,INDEX git_source_members_child(child_hash,root_hash)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS git_source_acquisition_sequence(id SMALLINT NOT NULL PRIMARY KEY,generation BIGINT NOT NULL,CONSTRAINT git_source_acquisition_sequence_shape CHECK(id=1 AND generation>=0)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS git_source_acquisitions(token VARCHAR(64) NOT NULL PRIMARY KEY,generation BIGINT NOT NULL UNIQUE,operation_id VARCHAR(96) NOT NULL UNIQUE,active_identity VARCHAR(64) UNIQUE,workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,commit_oid VARCHAR(64) NOT NULL,source_format_version BIGINT NOT NULL,owner VARCHAR(255) NOT NULL,attempt_id VARCHAR(255) NOT NULL,root_hash VARCHAR(64),root_len BIGINT,object_format VARCHAR(8),semantic_digest VARCHAR(64),object_set_digest VARCHAR(64),object_count BIGINT,total_bytes BIGINT,expires_at BIGINT NOT NULL,state VARCHAR(24) NOT NULL,failure_class VARCHAR(16),CONSTRAINT git_source_acquisitions_generation CHECK(generation>0),CONSTRAINT git_source_acquisitions_failure CHECK(failure_class IS NULL OR failure_class IN('retryable','permanent','dead_letter')),CONSTRAINT git_source_acquisitions_shape CHECK((state='held' AND active_identity IS NOT NULL AND root_hash IS NULL AND root_len IS NULL AND object_format IS NULL AND semantic_digest IS NULL AND object_set_digest IS NULL AND object_count IS NULL AND total_bytes IS NULL AND failure_class IS NULL) OR (state IN('graph_published','activation_unknown') AND active_identity IS NOT NULL AND root_hash IS NOT NULL AND root_len>0 AND object_format IN('sha1','sha256') AND semantic_digest IS NOT NULL AND object_set_digest IS NOT NULL AND object_count>0 AND total_bytes>0 AND failure_class IS NULL) OR (state='registered' AND active_identity IS NULL AND root_hash IS NOT NULL AND root_len>0 AND object_format IN('sha1','sha256') AND semantic_digest IS NOT NULL AND object_set_digest IS NOT NULL AND object_count>0 AND total_bytes>0 AND failure_class IS NULL) OR (state='failed' AND active_identity IS NULL AND failure_class IS NOT NULL)),INDEX git_source_acquisitions_recovery(state,generation,token)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS git_source_acquisition_members(token VARCHAR(64) NOT NULL,ordinal BIGINT NOT NULL,child_hash VARCHAR(64) NOT NULL,child_len BIGINT NOT NULL,kind VARCHAR(8) NOT NULL,PRIMARY KEY(token,ordinal),CONSTRAINT git_source_acquisition_members_identity UNIQUE(token,child_hash),CONSTRAINT git_source_acquisition_members_shape CHECK(ordinal>=0 AND child_len>0 AND kind IN('pack','index') AND CHAR_LENGTH(child_hash)=64),CONSTRAINT git_source_acquisition_members_parent FOREIGN KEY(token) REFERENCES git_source_acquisitions(token) ON DELETE CASCADE,INDEX git_source_acquisition_members_child(child_hash,token)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS git_source_desires(workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,commit_oid VARCHAR(64) NOT NULL,source_format_version BIGINT NOT NULL,state VARCHAR(16) NOT NULL,root_hash VARCHAR(64),failure_class VARCHAR(16),retry_count BIGINT NOT NULL DEFAULT 0,acquisition_token VARCHAR(64),updated_at BIGINT NOT NULL,PRIMARY KEY(workspace,repo,commit_oid,source_format_version),CONSTRAINT git_source_desires_failure CHECK(failure_class IS NULL OR failure_class IN('retryable','permanent','dead_letter')),CONSTRAINT git_source_desires_shape CHECK(retry_count BETWEEN 0 AND 4294967295 AND ((state='acquiring' AND acquisition_token IS NOT NULL AND root_hash IS NULL AND failure_class IS NULL) OR (state='registered' AND acquisition_token IS NULL AND root_hash IS NOT NULL AND failure_class IS NULL) OR (state='failed' AND acquisition_token IS NULL AND root_hash IS NULL AND failure_class IS NOT NULL))),CONSTRAINT git_source_desires_root FOREIGN KEY(root_hash) REFERENCES git_source_roots(root_hash) ON DELETE RESTRICT,CONSTRAINT git_source_desires_acquisition FOREIGN KEY(acquisition_token) REFERENCES git_source_acquisitions(token) ON DELETE RESTRICT,INDEX git_source_desires_root_lookup(root_hash),INDEX git_source_desires_acquisition_lookup(acquisition_token)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS branch_source_generations(workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,branch VARCHAR(191) NOT NULL,generation BIGINT NOT NULL,commit_oid VARCHAR(64) NOT NULL,source_format_version BIGINT NOT NULL,root_hash VARCHAR(64) NOT NULL,created_at BIGINT NOT NULL,PRIMARY KEY(workspace,repo,branch,generation),CONSTRAINT branch_source_generations_shape CHECK(generation>0),CONSTRAINT branch_source_generations_root FOREIGN KEY(root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT,INDEX branch_source_generations_root_lookup(root_hash,workspace,repo,commit_oid,source_format_version)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS branch_source_current(workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,branch VARCHAR(191) NOT NULL,generation BIGINT NOT NULL,PRIMARY KEY(workspace,repo,branch),CONSTRAINT branch_source_current_generation FOREIGN KEY(workspace,repo,branch,generation) REFERENCES branch_source_generations(workspace,repo,branch,generation) ON DELETE RESTRICT,INDEX branch_source_current_generation_lookup(workspace,repo,branch,generation)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS git_source_consumers(root_hash VARCHAR(64) NOT NULL,consumer_id VARCHAR(255) NOT NULL,session_id VARCHAR(64) NOT NULL UNIQUE,workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,commit_oid VARCHAR(64) NOT NULL,source_format_version BIGINT NOT NULL,purpose VARCHAR(16) NOT NULL,expires_at BIGINT NOT NULL,PRIMARY KEY(root_hash,consumer_id),CONSTRAINT git_source_consumers_shape CHECK(purpose IN('intent','builder')),CONSTRAINT git_source_consumers_root FOREIGN KEY(root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT,INDEX git_source_consumers_identity(root_hash,workspace,repo,commit_oid,source_format_version),INDEX git_source_consumers_expiry(expires_at,root_hash,consumer_id)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS artifact_intents(id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,workspace VARCHAR(128) NOT NULL,repo VARCHAR(320) NOT NULL,branch VARCHAR(191) NOT NULL,branch_generation BIGINT NOT NULL,source_root_hash VARCHAR(64) NOT NULL,source_format_version BIGINT NOT NULL,commit_oid VARCHAR(64) NOT NULL,kind VARCHAR(16) NOT NULL,format_version BIGINT NOT NULL,state VARCHAR(16) NOT NULL,artifact_id BIGINT,consumer_id VARCHAR(255) NOT NULL,created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,CONSTRAINT artifact_intents_identity UNIQUE(workspace,repo,branch,branch_generation,kind,format_version),CONSTRAINT artifact_intents_shape CHECK(format_version BETWEEN 1 AND 4294967295 AND kind IN('head','full_history','files') AND ((state='deferred' AND artifact_id IS NULL) OR (state='promoted' AND artifact_id IS NOT NULL))),CONSTRAINT artifact_intents_generation FOREIGN KEY(workspace,repo,branch,branch_generation) REFERENCES branch_source_generations(workspace,repo,branch,generation) ON DELETE RESTRICT,CONSTRAINT artifact_intents_source FOREIGN KEY(source_root_hash,workspace,repo,commit_oid,source_format_version) REFERENCES git_source_roots(root_hash,workspace,repo,commit_oid,source_format_version) ON DELETE RESTRICT,CONSTRAINT artifact_intents_artifact FOREIGN KEY(artifact_id) REFERENCES artifact_jobs(id) ON DELETE RESTRICT,INDEX artifact_intents_generation_lookup(workspace,repo,branch,branch_generation),INDEX artifact_intents_source_lookup(source_root_hash,workspace,repo,commit_oid,source_format_version),INDEX artifact_intents_artifact_lookup(artifact_id),INDEX artifact_intents_promotion(state,updated_at,id),INDEX artifact_intents_source(source_root_hash,state,id)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
    r#"CREATE TABLE IF NOT EXISTS git_source_maintenance(id SMALLINT NOT NULL PRIMARY KEY,intent_cursor BIGINT NOT NULL DEFAULT 0,intent_workspace_cursor VARCHAR(128) NOT NULL DEFAULT '',acquisition_cursor BIGINT NOT NULL DEFAULT 0,root_cursor VARCHAR(64) NOT NULL DEFAULT '',config_fingerprint VARCHAR(512) NOT NULL DEFAULT '',updated_at BIGINT NOT NULL DEFAULT 0,CONSTRAINT git_source_maintenance_shape CHECK(id=1 AND intent_cursor>=0 AND acquisition_cursor>=0)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"#,
];

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

pub(crate) async fn validate_mysql_v7_prefix(
    c: &mut MySqlConnection,
    complete: bool,
) -> Result<()> {
    let names:Vec<String>=sqlx::query_scalar("SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND (table_name LIKE 'git\\_source\\_%' ESCAPE '\\\\' OR table_name LIKE 'branch\\_source\\_%' ESCAPE '\\\\' OR table_name LIKE 'artifact\\_intents%' ESCAPE '\\\\') ORDER BY table_name").fetch_all(&mut *c).await?;
    let mut prefix = 0usize;
    for (position, table) in TABLES.iter().enumerate() {
        if names.iter().any(|name| name == table) {
            if position != prefix {
                bail!("mysql v7 source registry is not a canonical prefix")
            }
            validate_table(c, table).await?;
            prefix += 1
        }
    }
    if names.len() != prefix {
        bail!("mysql v7 source registry namespace contains foreign DDL")
    }
    if complete && prefix != TABLES.len() {
        bail!("mysql v7 source registry is incomplete")
    }
    if complete {
        let incoming:i64=sqlx::query_scalar("SELECT count(DISTINCT CONCAT(k.table_name,CHAR(0),k.constraint_name)) FROM information_schema.key_column_usage k WHERE k.referenced_table_schema=DATABASE() AND k.referenced_table_name IN('git_source_roots','git_source_acquisitions','branch_source_generations','artifact_intents','git_source_consumers','git_source_members','git_source_desires')").fetch_one(&mut *c).await?;
        if incoming != 9 {
            bail!("mysql v7 source registry has external or missing reverse foreign keys")
        }
    }
    Ok(())
}

async fn validate_table(c: &mut MySqlConnection, table: &str) -> Result<()> {
    let storage:Option<(Option<String>,Option<String>)>=sqlx::query_as("SELECT engine,table_collation FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?").bind(table).fetch_optional(&mut *c).await?;
    if storage != Some((Some("InnoDB".into()), Some("utf8mb4_bin".into()))) {
        bail!("mysql v7 source table storage differs: {table}")
    }
    let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=?").bind(table).fetch_one(&mut *c).await?;
    let expected = match table {
        "git_source_roots" => 16,
        "git_source_members" => 5,
        "git_source_acquisition_sequence" => 2,
        "git_source_acquisitions" => 20,
        "git_source_acquisition_members" => 5,
        "git_source_desires" => 10,
        "branch_source_generations" => 8,
        "branch_source_current" => 4,
        "git_source_consumers" => 9,
        "artifact_intents" => 15,
        "git_source_maintenance" => 7,
        _ => 0,
    };
    if columns != expected {
        bail!("mysql v7 source column inventory differs: {table}")
    }
    let bad_text:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND data_type IN('char','varchar','text','longtext') AND collation_name<>'utf8mb4_bin'").bind(table).fetch_one(&mut *c).await?;
    if bad_text != 0 {
        bail!("mysql v7 source text collation differs: {table}")
    }
    let extras:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND extra<>'' AND NOT(table_name='artifact_intents' AND column_name='id' AND extra='auto_increment')").bind(table).fetch_one(&mut *c).await?;
    if extras != 0 {
        bail!("mysql v7 source generated-column inventory differs: {table}")
    }
    let signature:Option<String>=sqlx::query_scalar("SELECT GROUP_CONCAT(CONCAT(column_name,':',lower(column_type),':',is_nullable,':',COALESCE(column_default,'<NULL>'),':',extra) ORDER BY ordinal_position SEPARATOR '|') FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=?").bind(table).fetch_one(&mut *c).await?;
    let expected_signature = match table {
        "git_source_roots" => {
            "root_hash:varchar(64):NO:<NULL>:|root_len:bigint:NO:<NULL>:|workspace:varchar(128):NO:<NULL>:|repo:varchar(320):NO:<NULL>:|commit_oid:varchar(64):NO:<NULL>:|source_format_version:bigint:NO:<NULL>:|object_format:varchar(8):NO:<NULL>:|semantic_digest:varchar(64):NO:<NULL>:|object_set_digest:varchar(64):NO:<NULL>:|object_count:bigint:NO:<NULL>:|total_bytes:bigint:NO:<NULL>:|registration_operation:varchar(96):NO:<NULL>:|registration_generation:bigint:NO:<NULL>:|state:varchar(16):NO:<NULL>:|created_at:bigint:NO:<NULL>:|registered_at:bigint:NO:<NULL>:"
        }
        "git_source_members" => {
            "root_hash:varchar(64):NO:<NULL>:|ordinal:bigint:NO:<NULL>:|child_hash:varchar(64):NO:<NULL>:|child_len:bigint:NO:<NULL>:|kind:varchar(8):NO:<NULL>:"
        }
        "git_source_acquisition_sequence" => "id:smallint:NO:<NULL>:|generation:bigint:NO:<NULL>:",
        "git_source_acquisitions" => {
            "token:varchar(64):NO:<NULL>:|generation:bigint:NO:<NULL>:|operation_id:varchar(96):NO:<NULL>:|active_identity:varchar(64):YES:<NULL>:|workspace:varchar(128):NO:<NULL>:|repo:varchar(320):NO:<NULL>:|commit_oid:varchar(64):NO:<NULL>:|source_format_version:bigint:NO:<NULL>:|owner:varchar(255):NO:<NULL>:|attempt_id:varchar(255):NO:<NULL>:|root_hash:varchar(64):YES:<NULL>:|root_len:bigint:YES:<NULL>:|object_format:varchar(8):YES:<NULL>:|semantic_digest:varchar(64):YES:<NULL>:|object_set_digest:varchar(64):YES:<NULL>:|object_count:bigint:YES:<NULL>:|total_bytes:bigint:YES:<NULL>:|expires_at:bigint:NO:<NULL>:|state:varchar(24):NO:<NULL>:|failure_class:varchar(16):YES:<NULL>:"
        }
        "git_source_acquisition_members" => {
            "token:varchar(64):NO:<NULL>:|ordinal:bigint:NO:<NULL>:|child_hash:varchar(64):NO:<NULL>:|child_len:bigint:NO:<NULL>:|kind:varchar(8):NO:<NULL>:"
        }
        "git_source_desires" => {
            "workspace:varchar(128):NO:<NULL>:|repo:varchar(320):NO:<NULL>:|commit_oid:varchar(64):NO:<NULL>:|source_format_version:bigint:NO:<NULL>:|state:varchar(16):NO:<NULL>:|root_hash:varchar(64):YES:<NULL>:|failure_class:varchar(16):YES:<NULL>:|retry_count:bigint:NO:0:|acquisition_token:varchar(64):YES:<NULL>:|updated_at:bigint:NO:<NULL>:"
        }
        "branch_source_generations" => {
            "workspace:varchar(128):NO:<NULL>:|repo:varchar(320):NO:<NULL>:|branch:varchar(191):NO:<NULL>:|generation:bigint:NO:<NULL>:|commit_oid:varchar(64):NO:<NULL>:|source_format_version:bigint:NO:<NULL>:|root_hash:varchar(64):NO:<NULL>:|created_at:bigint:NO:<NULL>:"
        }
        "branch_source_current" => {
            "workspace:varchar(128):NO:<NULL>:|repo:varchar(320):NO:<NULL>:|branch:varchar(191):NO:<NULL>:|generation:bigint:NO:<NULL>:"
        }
        "git_source_consumers" => {
            "root_hash:varchar(64):NO:<NULL>:|consumer_id:varchar(255):NO:<NULL>:|session_id:varchar(64):NO:<NULL>:|workspace:varchar(128):NO:<NULL>:|repo:varchar(320):NO:<NULL>:|commit_oid:varchar(64):NO:<NULL>:|source_format_version:bigint:NO:<NULL>:|purpose:varchar(16):NO:<NULL>:|expires_at:bigint:NO:<NULL>:"
        }
        "artifact_intents" => {
            "id:bigint:NO:<NULL>:auto_increment|workspace:varchar(128):NO:<NULL>:|repo:varchar(320):NO:<NULL>:|branch:varchar(191):NO:<NULL>:|branch_generation:bigint:NO:<NULL>:|source_root_hash:varchar(64):NO:<NULL>:|source_format_version:bigint:NO:<NULL>:|commit_oid:varchar(64):NO:<NULL>:|kind:varchar(16):NO:<NULL>:|format_version:bigint:NO:<NULL>:|state:varchar(16):NO:<NULL>:|artifact_id:bigint:YES:<NULL>:|consumer_id:varchar(255):NO:<NULL>:|created_at:bigint:NO:<NULL>:|updated_at:bigint:NO:<NULL>:"
        }
        "git_source_maintenance" => {
            "id:smallint:NO:<NULL>:|intent_cursor:bigint:NO:0:|intent_workspace_cursor:varchar(128):NO::|acquisition_cursor:bigint:NO:0:|root_cursor:varchar(64):NO::|config_fingerprint:varchar(512):NO::|updated_at:bigint:NO:0:"
        }
        _ => "",
    };
    if signature.as_deref() != Some(expected_signature) {
        bail!("mysql v7 source column definitions differ: {table}")
    }
    let mut indexes:Vec<(String,i64,String)>=sqlx::query_as("SELECT index_name,non_unique,GROUP_CONCAT(column_name ORDER BY seq_in_index) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? GROUP BY index_name,non_unique ORDER BY index_name").bind(table).fetch_all(&mut *c).await?;
    indexes.sort();
    if indexes != expected_indexes(table) {
        bail!("mysql v7 source index inventory differs: {table}")
    }
    let expected_constraints = match table {
        "git_source_roots" => 6,
        "git_source_members" => 4,
        "git_source_acquisition_sequence" => 2,
        "git_source_acquisitions" => 7,
        "git_source_acquisition_members" => 4,
        "git_source_desires" => 5,
        "branch_source_generations" => 3,
        "branch_source_current" => 2,
        "git_source_consumers" => 4,
        "artifact_intents" => 6,
        "git_source_maintenance" => 2,
        _ => 0,
    };
    let constraints:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name=?").bind(table).fetch_one(&mut *c).await?;
    if constraints != expected_constraints {
        bail!("mysql v7 source constraint inventory differs: {table}")
    }
    let mut actual:Vec<(String,String)>=sqlx::query_as("SELECT constraint_name,constraint_type FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name=? ORDER BY constraint_name").bind(table).fetch_all(&mut *c).await?;
    actual.sort();
    if actual != expected_constraint_inventory(table) {
        bail!("mysql v7 source constraint definitions differ: {table}")
    }
    for (name, clause) in expected_checks(table) {
        let stored:Option<String>=sqlx::query_scalar("SELECT check_clause FROM information_schema.check_constraints WHERE constraint_schema=DATABASE() AND constraint_name=?").bind(name).fetch_optional(&mut *c).await?;
        let actual = stored.as_deref().map(normalize_mysql_check);
        let expected = normalize_mysql_check(clause);
        if actual.as_deref() != Some(expected.as_str()) {
            bail!("mysql v7 source CHECK differs: {name}; actual={actual:?}; expected={expected}")
        }
    }
    let mut fks:Vec<(String,String,String,String)>=sqlx::query_as("SELECT r.constraint_name,r.referenced_table_name,r.delete_rule,GROUP_CONCAT(CONCAT(k.column_name,'=',k.referenced_column_name) ORDER BY k.ordinal_position) FROM information_schema.referential_constraints r JOIN information_schema.key_column_usage k ON k.constraint_schema=r.constraint_schema AND k.table_name=r.table_name AND k.constraint_name=r.constraint_name WHERE r.constraint_schema=DATABASE() AND r.table_name=? GROUP BY r.constraint_name,r.referenced_table_name,r.delete_rule ORDER BY r.constraint_name").bind(table).fetch_all(&mut *c).await?;
    fks.sort();
    if fks != expected_fks(table) {
        bail!("mysql v7 source foreign keys differ: {table}")
    }
    Ok(())
}

fn inventory(rows: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut v = rows
        .iter()
        .map(|x| (x.0.into(), x.1.into()))
        .collect::<Vec<_>>();
    v.sort();
    v
}
fn expected_constraint_inventory(table: &str) -> Vec<(String, String)> {
    inventory(match table {
        "git_source_roots" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("registration_operation", "UNIQUE"),
            ("registration_generation", "UNIQUE"),
            ("git_source_roots_identity", "UNIQUE"),
            ("git_source_roots_binding", "UNIQUE"),
            ("git_source_roots_shape", "CHECK"),
        ],
        "git_source_members" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("git_source_members_identity", "UNIQUE"),
            ("git_source_members_shape", "CHECK"),
            ("git_source_members_root", "FOREIGN KEY"),
        ],
        "git_source_acquisition_sequence" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("git_source_acquisition_sequence_shape", "CHECK"),
        ],
        "git_source_acquisitions" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("generation", "UNIQUE"),
            ("operation_id", "UNIQUE"),
            ("active_identity", "UNIQUE"),
            ("git_source_acquisitions_generation", "CHECK"),
            ("git_source_acquisitions_failure", "CHECK"),
            ("git_source_acquisitions_shape", "CHECK"),
        ],
        "git_source_acquisition_members" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("git_source_acquisition_members_identity", "UNIQUE"),
            ("git_source_acquisition_members_shape", "CHECK"),
            ("git_source_acquisition_members_parent", "FOREIGN KEY"),
        ],
        "git_source_desires" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("git_source_desires_failure", "CHECK"),
            ("git_source_desires_shape", "CHECK"),
            ("git_source_desires_root", "FOREIGN KEY"),
            ("git_source_desires_acquisition", "FOREIGN KEY"),
        ],
        "branch_source_generations" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("branch_source_generations_shape", "CHECK"),
            ("branch_source_generations_root", "FOREIGN KEY"),
        ],
        "branch_source_current" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("branch_source_current_generation", "FOREIGN KEY"),
        ],
        "git_source_consumers" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("session_id", "UNIQUE"),
            ("git_source_consumers_shape", "CHECK"),
            ("git_source_consumers_root", "FOREIGN KEY"),
        ],
        "artifact_intents" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("artifact_intents_identity", "UNIQUE"),
            ("artifact_intents_shape", "CHECK"),
            ("artifact_intents_generation", "FOREIGN KEY"),
            ("artifact_intents_source", "FOREIGN KEY"),
            ("artifact_intents_artifact", "FOREIGN KEY"),
        ],
        "git_source_maintenance" => &[
            ("PRIMARY", "PRIMARY KEY"),
            ("git_source_maintenance_shape", "CHECK"),
        ],
        _ => &[],
    })
}
fn expected_checks(table: &str) -> &'static [(&'static str, &'static str)] {
    match table {
        "git_source_roots" => &[(
            "git_source_roots_shape",
            "root_len>0 AND source_format_version BETWEEN 1 AND 4294967295 AND object_format IN('sha1','sha256') AND CHAR_LENGTH(semantic_digest)=64 AND CHAR_LENGTH(object_set_digest)=64 AND object_count>0 AND total_bytes>0 AND registration_generation>0 AND state IN('registered','quarantined')",
        )],
        "git_source_members" => &[(
            "git_source_members_shape",
            "ordinal>=0 AND child_len>0 AND kind IN('pack','index') AND CHAR_LENGTH(child_hash)=64",
        )],
        "git_source_acquisition_sequence" => &[(
            "git_source_acquisition_sequence_shape",
            "id=1 AND generation>=0",
        )],
        "git_source_acquisitions" => &[
            ("git_source_acquisitions_generation", "generation>0"),
            (
                "git_source_acquisitions_failure",
                "failure_class IS NULL OR failure_class IN('retryable','permanent','dead_letter')",
            ),
            (
                "git_source_acquisitions_shape",
                "(state='held' AND active_identity IS NOT NULL AND root_hash IS NULL AND root_len IS NULL AND object_format IS NULL AND semantic_digest IS NULL AND object_set_digest IS NULL AND object_count IS NULL AND total_bytes IS NULL AND failure_class IS NULL) OR (state IN('graph_published','activation_unknown') AND active_identity IS NOT NULL AND root_hash IS NOT NULL AND root_len>0 AND object_format IN('sha1','sha256') AND semantic_digest IS NOT NULL AND object_set_digest IS NOT NULL AND object_count>0 AND total_bytes>0 AND failure_class IS NULL) OR (state='registered' AND active_identity IS NULL AND root_hash IS NOT NULL AND root_len>0 AND object_format IN('sha1','sha256') AND semantic_digest IS NOT NULL AND object_set_digest IS NOT NULL AND object_count>0 AND total_bytes>0 AND failure_class IS NULL) OR (state='failed' AND active_identity IS NULL AND failure_class IS NOT NULL)",
            ),
        ],
        "git_source_acquisition_members" => &[(
            "git_source_acquisition_members_shape",
            "ordinal>=0 AND child_len>0 AND kind IN('pack','index') AND CHAR_LENGTH(child_hash)=64",
        )],
        "git_source_desires" => &[
            (
                "git_source_desires_failure",
                "failure_class IS NULL OR failure_class IN('retryable','permanent','dead_letter')",
            ),
            (
                "git_source_desires_shape",
                "retry_count BETWEEN 0 AND 4294967295 AND ((state='acquiring' AND acquisition_token IS NOT NULL AND root_hash IS NULL AND failure_class IS NULL) OR (state='registered' AND acquisition_token IS NULL AND root_hash IS NOT NULL AND failure_class IS NULL) OR (state='failed' AND acquisition_token IS NULL AND root_hash IS NULL AND failure_class IS NOT NULL))",
            ),
        ],
        "branch_source_generations" => &[("branch_source_generations_shape", "generation>0")],
        "git_source_consumers" => &[(
            "git_source_consumers_shape",
            "purpose IN('intent','builder')",
        )],
        "artifact_intents" => &[(
            "artifact_intents_shape",
            "format_version BETWEEN 1 AND 4294967295 AND kind IN('head','full_history','files') AND ((state='deferred' AND artifact_id IS NULL) OR (state='promoted' AND artifact_id IS NOT NULL))",
        )],
        "git_source_maintenance" => &[(
            "git_source_maintenance_shape",
            "id=1 AND intent_cursor>=0 AND acquisition_cursor>=0",
        )],
        _ => &[],
    }
}
fn normalize_mysql_check(v: &str) -> String {
    let mut normalized = String::with_capacity(v.len());
    let mut quoted = false;
    for c in v.chars() {
        if c == '\\' {
            continue;
        } else if c == '\'' {
            quoted = !quoted;
            normalized.push(c);
        } else if quoted {
            normalized.push(c);
        } else if c != '`' {
            normalized.push(c.to_ascii_lowercase());
        }
    }
    normalized = normalized.replace("_utf8mb4", "");
    let parenthesized = normalize_mysql_parentheses(&normalized);
    let mut compact = String::with_capacity(parenthesized.len());
    let mut quoted = false;
    for c in parenthesized.chars() {
        if c == '\'' {
            quoted = !quoted;
            compact.push(c);
        } else if quoted || !c.is_whitespace() {
            compact.push(c);
        }
    }
    compact
}

fn normalize_mysql_parentheses(value: &str) -> String {
    fn matching(chars: &[char], open: usize) -> Option<usize> {
        let mut depth = 0_i64;
        let mut quoted = false;
        for (index, c) in chars.iter().enumerate().skip(open) {
            if *c == '\'' {
                quoted = !quoted;
            } else if !quoted {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(index);
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }

    fn has_boolean(chars: &[char]) -> bool {
        let mut depth = 0_i64;
        let mut quoted = false;
        let mut word = String::new();
        let mut between = false;
        let finish_word = |word: &mut String, between: &mut bool| -> bool {
            let logical = match word.as_str() {
                "between" => {
                    *between = true;
                    false
                }
                "and" if *between => {
                    *between = false;
                    false
                }
                "and" | "or" => true,
                _ => false,
            };
            word.clear();
            logical
        };
        for c in chars {
            if *c == '\'' {
                if depth == 0 && finish_word(&mut word, &mut between) {
                    return true;
                }
                quoted = !quoted;
            } else if !quoted {
                match c {
                    '(' => {
                        if depth == 0 && finish_word(&mut word, &mut between) {
                            return true;
                        }
                        depth += 1;
                    }
                    ')' => {
                        if depth == 0 && finish_word(&mut word, &mut between) {
                            return true;
                        }
                        depth -= 1;
                    }
                    c if depth == 0 && (c.is_ascii_alphanumeric() || *c == '_') => word.push(*c),
                    _ if depth == 0 && finish_word(&mut word, &mut between) => return true,
                    _ => {}
                }
            }
        }
        finish_word(&mut word, &mut between)
    }

    fn recurse(chars: &[char]) -> String {
        if chars.first() == Some(&'(') && matching(chars, 0) == Some(chars.len() - 1) {
            return recurse(&chars[1..chars.len() - 1]);
        }
        let mut output = String::new();
        let mut index = 0;
        while index < chars.len() {
            if chars[index] != '(' {
                output.push(chars[index]);
                index += 1;
                continue;
            }
            let close =
                matching(chars, index).expect("validated MySQL CHECK has balanced parentheses");
            let content = recurse(&chars[index + 1..close]);
            let token = output
                .chars()
                .rev()
                .skip_while(|c| c.is_whitespace())
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>();
            let call_or_list = !token.is_empty() && !matches!(token.as_str(), "and" | "or" | "not");
            if call_or_list || has_boolean(&chars[index + 1..close]) {
                output.push('(');
                output.push_str(&content);
                output.push(')');
            } else {
                output.push_str(&content);
            }
            index = close + 1;
        }
        output
    }

    recurse(&value.chars().collect::<Vec<_>>())
}
fn fk(rows: &[(&str, &str, &str, &str)]) -> Vec<(String, String, String, String)> {
    let mut v = rows
        .iter()
        .map(|x| (x.0.into(), x.1.into(), x.2.into(), x.3.into()))
        .collect::<Vec<_>>();
    v.sort();
    v
}
fn expected_fks(table: &str) -> Vec<(String, String, String, String)> {
    fk(match table {
        "git_source_members" => &[(
            "git_source_members_root",
            "git_source_roots",
            "RESTRICT",
            "root_hash=root_hash",
        )],
        "git_source_acquisition_members" => &[(
            "git_source_acquisition_members_parent",
            "git_source_acquisitions",
            "CASCADE",
            "token=token",
        )],
        "git_source_desires" => &[
            (
                "git_source_desires_acquisition",
                "git_source_acquisitions",
                "RESTRICT",
                "acquisition_token=token",
            ),
            (
                "git_source_desires_root",
                "git_source_roots",
                "RESTRICT",
                "root_hash=root_hash",
            ),
        ],
        "branch_source_generations" => &[(
            "branch_source_generations_root",
            "git_source_roots",
            "RESTRICT",
            "root_hash=root_hash,workspace=workspace,repo=repo,commit_oid=commit_oid,source_format_version=source_format_version",
        )],
        "branch_source_current" => &[(
            "branch_source_current_generation",
            "branch_source_generations",
            "RESTRICT",
            "workspace=workspace,repo=repo,branch=branch,generation=generation",
        )],
        "git_source_consumers" => &[(
            "git_source_consumers_root",
            "git_source_roots",
            "RESTRICT",
            "root_hash=root_hash,workspace=workspace,repo=repo,commit_oid=commit_oid,source_format_version=source_format_version",
        )],
        "artifact_intents" => &[
            (
                "artifact_intents_artifact",
                "artifact_jobs",
                "RESTRICT",
                "artifact_id=id",
            ),
            (
                "artifact_intents_generation",
                "branch_source_generations",
                "RESTRICT",
                "workspace=workspace,repo=repo,branch=branch,branch_generation=generation",
            ),
            (
                "artifact_intents_source",
                "git_source_roots",
                "RESTRICT",
                "source_root_hash=root_hash,workspace=workspace,repo=repo,commit_oid=commit_oid,source_format_version=source_format_version",
            ),
        ],
        _ => &[],
    })
}

fn expected_indexes(table: &str) -> Vec<(String, i64, String)> {
    let rows: &[(&str, i64, &str)] = match table {
        "git_source_roots" => &[
            ("PRIMARY", 0, "root_hash"),
            (
                "git_source_roots_binding",
                0,
                "root_hash,workspace,repo,commit_oid,source_format_version",
            ),
            (
                "git_source_roots_identity",
                0,
                "workspace,repo,commit_oid,source_format_version",
            ),
            ("registration_generation", 0, "registration_generation"),
            ("registration_operation", 0, "registration_operation"),
        ],
        "git_source_members" => &[
            ("PRIMARY", 0, "root_hash,ordinal"),
            ("git_source_members_child", 1, "child_hash,root_hash"),
            ("git_source_members_identity", 0, "root_hash,child_hash"),
        ],
        "git_source_acquisition_sequence" => &[("PRIMARY", 0, "id")],
        "git_source_acquisitions" => &[
            ("PRIMARY", 0, "token"),
            ("active_identity", 0, "active_identity"),
            ("generation", 0, "generation"),
            (
                "git_source_acquisitions_recovery",
                1,
                "state,generation,token",
            ),
            ("operation_id", 0, "operation_id"),
        ],
        "git_source_acquisition_members" => &[
            ("PRIMARY", 0, "token,ordinal"),
            (
                "git_source_acquisition_members_child",
                1,
                "child_hash,token",
            ),
            (
                "git_source_acquisition_members_identity",
                0,
                "token,child_hash",
            ),
        ],
        "git_source_desires" => &[
            (
                "PRIMARY",
                0,
                "workspace,repo,commit_oid,source_format_version",
            ),
            (
                "git_source_desires_acquisition_lookup",
                1,
                "acquisition_token",
            ),
            ("git_source_desires_root_lookup", 1, "root_hash"),
        ],
        "branch_source_generations" => &[
            ("PRIMARY", 0, "workspace,repo,branch,generation"),
            (
                "branch_source_generations_root_lookup",
                1,
                "root_hash,workspace,repo,commit_oid,source_format_version",
            ),
        ],
        "branch_source_current" => &[
            ("PRIMARY", 0, "workspace,repo,branch"),
            (
                "branch_source_current_generation_lookup",
                1,
                "workspace,repo,branch,generation",
            ),
        ],
        "git_source_consumers" => &[
            ("PRIMARY", 0, "root_hash,consumer_id"),
            (
                "git_source_consumers_expiry",
                1,
                "expires_at,root_hash,consumer_id",
            ),
            (
                "git_source_consumers_identity",
                1,
                "root_hash,workspace,repo,commit_oid,source_format_version",
            ),
            ("session_id", 0, "session_id"),
        ],
        "artifact_intents" => &[
            ("PRIMARY", 0, "id"),
            ("artifact_intents_artifact_lookup", 1, "artifact_id"),
            (
                "artifact_intents_generation_lookup",
                1,
                "workspace,repo,branch,branch_generation",
            ),
            (
                "artifact_intents_identity",
                0,
                "workspace,repo,branch,branch_generation,kind,format_version",
            ),
            ("artifact_intents_promotion", 1, "state,updated_at,id"),
            ("artifact_intents_source", 1, "source_root_hash,state,id"),
            (
                "artifact_intents_source_lookup",
                1,
                "source_root_hash,workspace,repo,commit_oid,source_format_version",
            ),
        ],
        "git_source_maintenance" => &[("PRIMARY", 0, "id")],
        _ => &[],
    };
    let mut out = rows
        .iter()
        .map(|v| (v.0.into(), v.1, v.2.into()))
        .collect::<Vec<_>>();
    out.sort();
    out
}

pub(crate) async fn validate_mysql_v7_state(c: &mut MySqlConnection) -> Result<()> {
    let seq: Vec<(i64, i64)> =
        sqlx::query_as("SELECT id,generation FROM git_source_acquisition_sequence")
            .fetch_all(&mut *c)
            .await?;
    let max: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(generation),0) FROM git_source_acquisitions")
            .fetch_one(&mut *c)
            .await?;
    if seq.len() != 1 || seq[0].0 != 1 || seq[0].1 < max {
        bail!("mysql v7 source generation sequence is invalid")
    }
    let operations:Vec<(i64,String,String,String,String,String)>=sqlx::query_as("SELECT generation,workspace,repo,commit_oid,attempt_id,operation_id FROM git_source_acquisitions").fetch_all(&mut *c).await?;
    if operations
        .iter()
        .any(|(generation, workspace, repo, commit, attempt, stored)| {
            stored != &operation_id(workspace, repo, commit, attempt, *generation)
        })
    {
        bail!("mysql v7 source acquisition provenance is invalid")
    }
    let singleton:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_maintenance WHERE id=1 AND intent_cursor>=0 AND acquisition_cursor>=0").fetch_one(&mut *c).await?;
    if singleton != 1 {
        bail!("mysql v7 source maintenance singleton is invalid")
    }
    let invalid:i64=sqlx::query_scalar("SELECT
      (SELECT count(*) FROM git_source_roots r WHERE r.root_hash NOT REGEXP '^[0-9a-f]{64}$' OR r.semantic_digest NOT REGEXP '^[0-9a-f]{64}$' OR r.object_set_digest NOT REGEXP '^[0-9a-f]{64}$' OR (r.object_format='sha1' AND r.commit_oid NOT REGEXP '^[0-9a-f]{40}$') OR (r.object_format='sha256' AND r.commit_oid NOT REGEXP '^[0-9a-f]{64}$') OR NOT EXISTS(SELECT 1 FROM git_source_members m WHERE m.root_hash=r.root_hash GROUP BY m.root_hash HAVING MIN(m.ordinal)=0 AND MAX(m.ordinal)+1=count(*) AND MOD(count(*),2)=0 AND SUM(m.child_len)=r.total_bytes AND SUM(CASE WHEN (MOD(m.ordinal,2)=0 AND m.kind='pack') OR (MOD(m.ordinal,2)=1 AND m.kind='index') THEN 0 ELSE 1 END)=0))+
      (SELECT count(*) FROM git_source_members WHERE child_hash NOT REGEXP '^[0-9a-f]{64}$')+(SELECT count(*) FROM git_source_acquisition_members WHERE child_hash NOT REGEXP '^[0-9a-f]{64}$')+
      (SELECT count(*) FROM git_source_acquisitions a WHERE token NOT REGEXP '^[0-9a-f]{64}$' OR (root_hash IS NOT NULL AND root_hash NOT REGEXP '^[0-9a-f]{64}$') OR (semantic_digest IS NOT NULL AND semantic_digest NOT REGEXP '^[0-9a-f]{64}$') OR (object_set_digest IS NOT NULL AND object_set_digest NOT REGEXP '^[0-9a-f]{64}$') OR (state IN('held','graph_published','activation_unknown') AND active_identity IS NULL) OR (state IN('registered','failed') AND active_identity IS NOT NULL) OR (state='held' AND EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token)) OR (state IN('graph_published','activation_unknown','registered') AND NOT EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token)) OR EXISTS(SELECT 1 FROM git_source_acquisition_members m WHERE m.token=a.token GROUP BY m.token HAVING MIN(m.ordinal)<>0 OR MAX(m.ordinal)+1<>count(*) OR MOD(count(*),2)<>0 OR SUM(m.child_len)<>a.total_bytes OR SUM(CASE WHEN (MOD(m.ordinal,2)=0 AND m.kind='pack') OR (MOD(m.ordinal,2)=1 AND m.kind='index') THEN 0 ELSE 1 END)<>0))+
      (SELECT count(*) FROM git_source_acquisitions a LEFT JOIN git_source_roots r ON r.root_hash=a.root_hash WHERE a.state='registered' AND (r.root_hash IS NULL OR r.state<>'registered' OR r.registration_operation<>a.operation_id OR r.registration_generation<>a.generation OR r.workspace<>a.workspace OR r.repo<>a.repo OR r.commit_oid<>a.commit_oid OR r.source_format_version<>a.source_format_version OR r.root_len<>a.root_len OR r.object_format<>a.object_format OR r.semantic_digest<>a.semantic_digest OR r.object_set_digest<>a.object_set_digest OR r.object_count<>a.object_count OR r.total_bytes<>a.total_bytes))+
      (SELECT count(*) FROM git_source_desires d LEFT JOIN git_source_acquisitions a ON a.token=d.acquisition_token LEFT JOIN git_source_roots r ON r.root_hash=d.root_hash WHERE d.source_format_version<>1 OR (d.state='acquiring' AND (a.token IS NULL OR a.workspace<>d.workspace OR a.repo<>d.repo OR a.commit_oid<>d.commit_oid OR a.state NOT IN('held','graph_published','activation_unknown'))) OR (d.state='registered' AND (r.root_hash IS NULL OR r.workspace<>d.workspace OR r.repo<>d.repo OR r.commit_oid<>d.commit_oid OR r.state<>'registered')))+
      (SELECT count(*) FROM branch_source_current current JOIN branch_source_generations g ON g.workspace=current.workspace AND g.repo=current.repo AND g.branch=current.branch AND g.generation=current.generation LEFT JOIN branch_observations b ON b.workspace=current.workspace AND b.repo=current.repo AND b.branch=current.branch WHERE b.workspace IS NULL OR b.generation<>g.generation OR b.desired_commit<>g.commit_oid)+
      (SELECT count(*) FROM git_source_consumers c LEFT JOIN git_source_roots r ON r.root_hash=c.root_hash AND r.workspace=c.workspace AND r.repo=c.repo AND r.commit_oid=c.commit_oid AND r.source_format_version=c.source_format_version WHERE r.root_hash IS NULL OR c.session_id NOT REGEXP '^[0-9a-f]{64}$')+
      (SELECT count(*) FROM artifact_intents i LEFT JOIN branch_source_generations g ON g.workspace=i.workspace AND g.repo=i.repo AND g.branch=i.branch AND g.generation=i.branch_generation LEFT JOIN git_source_consumers c ON c.consumer_id=i.consumer_id AND c.root_hash=i.source_root_hash AND c.purpose='intent' LEFT JOIN artifact_jobs j ON j.id=i.artifact_id WHERE g.workspace IS NULL OR g.root_hash<>i.source_root_hash OR g.commit_oid<>i.commit_oid OR g.source_format_version<>i.source_format_version OR c.root_hash IS NULL OR i.consumer_id NOT REGEXP '^intent:[0-9a-f]{48}$' OR c.expires_at<>9223372036854775807 OR (SELECT count(*) FROM git_source_consumers sibling WHERE sibling.consumer_id=i.consumer_id)<>1 OR (SELECT count(*) FROM artifact_intents sibling WHERE sibling.consumer_id=i.consumer_id)<>1 OR (i.state='deferred' AND (i.artifact_id IS NOT NULL OR EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.consumer_id=i.consumer_id))) OR (i.state='promoted' AND (j.id IS NULL OR j.workspace<>i.workspace OR j.repo<>i.repo OR j.commit_oid<>i.commit_oid OR j.kind<>i.kind OR j.format_version<>i.format_version OR (SELECT count(*) FROM artifact_consumers sibling WHERE sibling.consumer_id=i.consumer_id)<>1 OR NOT EXISTS(SELECT 1 FROM artifact_consumers ac WHERE ac.artifact_id=i.artifact_id AND ac.consumer_id=i.consumer_id AND ac.expires_at=9223372036854775807))))").fetch_one(&mut *c).await?;
    let reverse_invalid:i64=sqlx::query_scalar("SELECT
      (SELECT count(*) FROM git_source_roots r WHERE r.state='registered' AND (NOT EXISTS(SELECT 1 FROM git_source_acquisitions a WHERE a.state='registered' AND a.root_hash=r.root_hash AND a.operation_id=r.registration_operation AND a.generation=r.registration_generation) OR NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='registered' AND d.root_hash=r.root_hash AND d.workspace=r.workspace AND d.repo=r.repo AND d.commit_oid=r.commit_oid AND d.source_format_version=r.source_format_version)))+
      (SELECT count(*) FROM git_source_acquisitions a WHERE (a.state IN('held','graph_published','activation_unknown') AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='acquiring' AND d.acquisition_token=a.token AND d.workspace=a.workspace AND d.repo=a.repo AND d.commit_oid=a.commit_oid AND d.source_format_version=a.source_format_version)) OR (a.state='registered' AND NOT EXISTS(SELECT 1 FROM git_source_desires d WHERE d.state='registered' AND d.root_hash=a.root_hash AND d.workspace=a.workspace AND d.repo=a.repo AND d.commit_oid=a.commit_oid AND d.source_format_version=a.source_format_version)) OR (a.state='registered' AND (EXISTS(SELECT 1 FROM git_source_acquisition_members am LEFT JOIN git_source_members m ON m.root_hash=a.root_hash AND m.ordinal=am.ordinal WHERE am.token=a.token AND (m.ordinal IS NULL OR m.child_hash<>am.child_hash OR m.child_len<>am.child_len OR m.kind<>am.kind)) OR EXISTS(SELECT 1 FROM git_source_members m LEFT JOIN git_source_acquisition_members am ON am.token=a.token AND am.ordinal=m.ordinal WHERE m.root_hash=a.root_hash AND am.ordinal IS NULL))))+
      (SELECT count(*) FROM (SELECT hash FROM (SELECT root_hash hash,root_len len,'root' kind FROM git_source_roots UNION ALL SELECT root_hash,root_len,'root' FROM git_source_acquisitions WHERE root_hash IS NOT NULL UNION ALL SELECT child_hash,child_len,kind FROM git_source_members UNION ALL SELECT child_hash,child_len,kind FROM git_source_acquisition_members) descriptors GROUP BY hash HAVING count(DISTINCT CONCAT(len,':',kind))<>1) conflicts)+
      (SELECT count(*) FROM (SELECT root_hash hash FROM git_source_roots UNION SELECT root_hash FROM git_source_acquisitions WHERE root_hash IS NOT NULL) roots JOIN (SELECT child_hash hash FROM git_source_members UNION SELECT child_hash FROM git_source_acquisition_members) children ON children.hash=roots.hash)+
      (SELECT count(*) FROM git_source_consumers c WHERE c.purpose='intent' AND NOT EXISTS(SELECT 1 FROM artifact_intents i WHERE i.consumer_id=c.consumer_id AND i.source_root_hash=c.root_hash))+
      (SELECT count(*) FROM artifact_consumers ac WHERE ac.consumer_id LIKE 'intent:%' AND (ac.expires_at<>9223372036854775807 OR NOT EXISTS(SELECT 1 FROM artifact_intents i WHERE i.state='promoted' AND i.consumer_id=ac.consumer_id AND i.artifact_id=ac.artifact_id)))").fetch_one(&mut *c).await?;
    if invalid + reverse_invalid != 0 {
        bail!("mysql v7 source registry persisted state is invalid")
    }
    Ok(())
}

#[derive(Clone)]
pub struct MysqlGitSourceRegistry {
    pool: MySqlPool,
    storage: StorageRef,
    scheduler_limits: SchedulerLimits,
    source_limits: GitSourceLimits,
    seal: Arc<[u8; 32]>,
}

impl MysqlGitSourceRegistry {
    pub async fn new(
        pool: MySqlPool,
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
        validate_mysql_v7_prefix(&mut c, true).await?;
        validate_mysql_v7_state(&mut c).await?;
        let fingerprint = registry.source_fingerprint();
        let mut tx = registry.pool.begin().await?;
        let scheduler_fingerprint: String = sqlx::query_scalar(
            "SELECT limits_fingerprint FROM scheduler_state WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if scheduler_fingerprint != scheduler_limits_fingerprint(&registry.scheduler_limits) {
            bail!("MySQL source registry scheduler limits differ from durable fleet limits")
        }
        let stored: String = sqlx::query_scalar(
            "SELECT config_fingerprint FROM git_source_maintenance WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if stored.is_empty() {
            let state:i64=sqlx::query_scalar("SELECT (SELECT generation FROM git_source_acquisition_sequence WHERE id=1)+(SELECT count(*) FROM git_source_roots)+(SELECT count(*) FROM git_source_members)+(SELECT count(*) FROM git_source_acquisitions)+(SELECT count(*) FROM git_source_acquisition_members)+(SELECT count(*) FROM git_source_desires)+(SELECT count(*) FROM branch_source_generations)+(SELECT count(*) FROM branch_source_current)+(SELECT count(*) FROM git_source_consumers)+(SELECT count(*) FROM artifact_intents)+(SELECT count(*) FROM git_source_maintenance WHERE id<>1 OR intent_cursor<>0 OR intent_workspace_cursor<>'' OR acquisition_cursor<>0 OR root_cursor<>'' OR updated_at<>0)").fetch_one(&mut *tx).await?;
            if state != 0 {
                bail!("empty MySQL source registry fingerprint has authoritative state")
            }
            if sqlx::query("UPDATE git_source_maintenance SET config_fingerprint=? WHERE id=1 AND config_fingerprint='' AND intent_cursor=0 AND intent_workspace_cursor='' AND acquisition_cursor=0 AND root_cursor='' AND updated_at=0").bind(&fingerprint).execute(&mut *tx).await?.rows_affected()!=1{bail!("MySQL source registry configuration CAS failed")}
        } else if stored != fingerprint {
            bail!("MySQL source registry limits or authority seal differ from fleet configuration")
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
        let now = mysql_time(&mut tx).await?;
        // Serialize the first observation of an identity as well as retries.
        // Relying on an absent-row gap lock makes correctness depend on the
        // server isolation level (READ COMMITTED does not provide that lock).
        let prior: i64 = sqlx::query_scalar(
            "SELECT generation FROM git_source_acquisition_sequence WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        if let Some(token)=sqlx::query_scalar::<_,String>("SELECT token FROM git_source_acquisitions WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND state IN('held','graph_published') AND expires_at<=? FOR UPDATE").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(now).fetch_optional(&mut *tx).await?{
            sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=? WHERE acquisition_token=? AND state='acquiring'").bind(now).bind(&token).execute(&mut *tx).await?;
            sqlx::query("UPDATE git_source_acquisitions SET state='failed',active_identity=NULL,failure_class='retryable',expires_at=0 WHERE token=? AND state IN('held','graph_published')").bind(&token).execute(&mut *tx).await?;
        }
        if let Some(row)=sqlx::query("SELECT state,root_hash,failure_class,retry_count,acquisition_token FROM git_source_desires WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=? FOR UPDATE").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).fetch_optional(&mut *tx).await?{
            let state:String=row.try_get("state")?;
            if state=="registered"{let root:String=row.try_get("root_hash")?;let (token,generation):(String,i64)=sqlx::query_as("SELECT token,generation FROM git_source_acquisitions WHERE workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND root_hash=? AND state='registered' ORDER BY generation DESC LIMIT 1").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(&root).fetch_one(&mut *tx).await?;tx.commit().await?;return Ok(SourceBeginOutcome::Ready(DurableSourceSnapshot::registered(workspace.into(),repo.into(),commit.into(),root,token,checked_u64(generation,"source generation")?)?))}
            if state=="acquiring"{let token:String=row.try_get("acquisition_token")?;let (generation,state):(i64,String)=sqlx::query_as("SELECT generation,state FROM git_source_acquisitions WHERE token=?").bind(&token).fetch_one(&mut *tx).await?;tx.commit().await?;return Ok(if state=="activation_unknown"{SourceBeginOutcome::ActivationUnknown{token,generation:checked_u64(generation,"source generation")?}}else{SourceBeginOutcome::Deferred{token,generation:checked_u64(generation,"source generation")?}})}
            let class=FailureClass::parse(row.try_get::<String,_>("failure_class")?.as_str())?;let retries=checked_u32(row.try_get("retry_count")?,"source retry count")?;
            if intent==SyncIntent::ObserveMovement||class!=FailureClass::Retryable||retries>=self.scheduler_limits.max_manual_retries{tx.commit().await?;return Ok(SourceBeginOutcome::Failed{class,retries})}
        }
        let generation = prior.checked_add(1).context("source generation overflow")?;
        sqlx::query(
            "UPDATE git_source_acquisition_sequence SET generation=? WHERE id=1 AND generation=?",
        )
        .bind(generation)
        .bind(prior)
        .execute(&mut *tx)
        .await?;
        let token = hex::encode(rand::random::<[u8; 32]>());
        let operation_id = operation_id(workspace, repo, commit, attempt_id, generation);
        let active_identity = source_identity(workspace, repo, commit, source_format_version);
        sqlx::query("INSERT INTO git_source_acquisitions(token,generation,operation_id,active_identity,workspace,repo,commit_oid,source_format_version,owner,attempt_id,expires_at,state) VALUES(?,?,?,?,?,?,?,?,?,?,?,'held')").bind(&token).bind(generation).bind(&operation_id).bind(&active_identity).bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(owner).bind(attempt_id).bind(now+ttl_secs).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO git_source_desires(workspace,repo,commit_oid,source_format_version,state,retry_count,acquisition_token,updated_at) VALUES(?,?,?,?,'acquiring',0,?,?) ON DUPLICATE KEY UPDATE state='acquiring',root_hash=NULL,failure_class=NULL,retry_count=retry_count+1,acquisition_token=VALUES(acquisition_token),updated_at=VALUES(updated_at)").bind(workspace).bind(repo).bind(commit).bind(source_format_version as i64).bind(&token).bind(now).execute(&mut *tx).await?;
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
        let now = mysql_time(&mut tx).await?;
        // Share the same control lock as GC root discovery/retirement so the
        // absence proof and graph publication cannot race one another.
        sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        let sweep: i64 =
            sqlx::query_scalar("SELECT count(*) FROM artifact_gc_sweep WHERE expires_at>?")
                .bind(now)
                .fetch_one(&mut *tx)
                .await?;
        if sweep != 0 {
            bail!("source graph publication is fenced by live GC sweep")
        }
        let changed=sqlx::query("UPDATE git_source_acquisitions SET root_hash=?,root_len=?,object_format=?,semantic_digest=?,object_set_digest=?,object_count=?,total_bytes=?,state='graph_published' WHERE token=? AND generation=? AND operation_id=? AND owner=? AND attempt_id=? AND state='held' AND expires_at>?").bind(&view.root.hash).bind(checked_i64(view.root.len,"root length")?).bind(view.object_format).bind(&view.semantic_digest).bind(&view.object_set_digest).bind(checked_i64(view.object_count,"object count")?).bind(checked_i64(view.total_bytes,"source bytes")?).bind(&prepare.token).bind(prepare.generation as i64).bind(&prepare.operation_id).bind(&prepare.owner).bind(&prepare.attempt_id).bind(now).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            bail!("held source preparation capability was lost")
        }
        for member in &view.members {
            sqlx::query("INSERT INTO git_source_acquisition_members(token,ordinal,child_hash,child_len,kind) VALUES(?,?,?,?,?)").bind(&prepare.token).bind(member.ordinal as i64).bind(&member.blob.hash).bind(checked_i64(member.blob.len,"member length")?).bind(member.kind).execute(&mut *tx).await?;
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
        Ok(sqlx::query("UPDATE git_source_acquisitions SET expires_at=UNIX_TIMESTAMP()+? WHERE token=? AND generation=? AND operation_id=? AND owner=? AND attempt_id=? AND state='held' AND expires_at>UNIX_TIMESTAMP()").bind(ttl).bind(&p.token).bind(p.generation as i64).bind(&p.operation_id).bind(&p.owner).bind(&p.attempt_id).execute(&self.pool).await?.rows_affected()==1)
    }
    pub async fn renew(&self, a: &GitSourceAcquisition, ttl: i64) -> Result<bool> {
        if !(1..=3600).contains(&ttl) {
            bail!("source acquisition TTL is invalid")
        }
        Ok(sqlx::query("UPDATE git_source_acquisitions SET expires_at=UNIX_TIMESTAMP()+? WHERE token=? AND generation=? AND operation_id=? AND state='graph_published' AND expires_at>UNIX_TIMESTAMP()").bind(ttl).bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).execute(&self.pool).await?.rows_affected()==1)
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
        let changed=sqlx::query("UPDATE git_source_acquisitions SET state='failed',active_identity=NULL,failure_class=?,expires_at=0 WHERE token=? AND generation=? AND operation_id=? AND state=?").bind(class.as_str()).bind(token).bind(generation as i64).bind(operation).bind(state).execute(&mut *tx).await?.rows_affected();
        if changed == 0 {
            return Ok(false);
        }
        if sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class=?,acquisition_token=NULL,updated_at=UNIX_TIMESTAMP() WHERE acquisition_token=? AND state='acquiring'").bind(class.as_str()).bind(token).execute(&mut *tx).await?.rows_affected()!=1{bail!("source desire failure settlement lost")}
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
        let registry = self.clone();
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
            self.fail(a, FailureClass::Retryable).await?;
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
        let verification: Result<()> = async {
            loop {
            tokio::select! {result=&mut verify=>{result.context("Git source verifier did not join")??;break},_=cancelled.cancelled()=>{verify_cancel.cancel();verify.await.context("cancelled Git source verifier did not join")??;bail!("Git source registration cancelled")},_=interval.tick()=>if !self.renew(a,60).await?{verify_cancel.cancel();verify.await.context("lease-lost Git source verifier did not join")??;bail!("Git source acquisition lease was lost during verification")}}
            }
            Ok(())
        }
        .await;
        if let Err(error) = verification {
            let _ = self.fail(a, FailureClass::Retryable).await?;
            return Err(error);
        }
        let mut unknown = self.pool.begin().await?;
        if sqlx::query("UPDATE git_source_acquisitions SET state='activation_unknown' WHERE token=? AND generation=? AND operation_id=? AND state='graph_published'").bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).execute(&mut *unknown).await?.rows_affected()!=1{bail!("source registration capability was lost")}
        if let Err(error) = unknown.commit().await {
            let state:Option<String>=sqlx::query_scalar("SELECT state FROM git_source_acquisitions WHERE token=? AND generation=? AND operation_id=?").bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).fetch_optional(&self.pool).await?;
            if state.as_deref() != Some("activation_unknown") {
                let _ = self.fail(a, FailureClass::Retryable).await?;
                return Err(error)
                    .context("source activation-unknown transition acknowledgement was lost");
            }
        }
        let registration:Result<DurableSourceSnapshot>=async{let mut tx=self.pool.begin().await?;let now=mysql_time(&mut tx).await?;
            let descriptor:Option<(String,i64,String,String,String,i64,i64)>=sqlx::query_as("SELECT root_hash,root_len,object_format,semantic_digest,object_set_digest,object_count,total_bytes FROM git_source_acquisitions WHERE token=? AND generation=? AND operation_id=? AND state='activation_unknown' FOR UPDATE").bind(&a.token).bind(a.generation as i64).bind(&a.operation_id).fetch_optional(&mut *tx).await?;let expected=(view.root.hash.clone(),checked_i64(view.root.len,"root length")?,view.object_format.to_owned(),view.semantic_digest.clone(),view.object_set_digest.clone(),checked_i64(view.object_count,"object count")?,checked_i64(view.total_bytes,"source bytes")?);if descriptor!=Some(expected){bail!("source acquisition descriptor differs at registration")}
            let members:Vec<(i64,String,i64,String)>=sqlx::query_as("SELECT ordinal,child_hash,child_len,kind FROM git_source_acquisition_members WHERE token=? ORDER BY ordinal").bind(&a.token).fetch_all(&mut *tx).await?;if members.len()!=view.members.len()||members.iter().zip(&view.members).any(|(got,want)|got.0!=want.ordinal as i64||got.1!=want.blob.hash||got.2!=want.blob.len as i64||got.3!=want.kind){bail!("source acquisition members differ at registration")}
            sqlx::query("INSERT INTO git_source_roots(root_hash,root_len,workspace,repo,commit_oid,source_format_version,object_format,semantic_digest,object_set_digest,object_count,total_bytes,registration_operation,registration_generation,state,created_at,registered_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'registered',?,?)").bind(&view.root.hash).bind(view.root.len as i64).bind(&view.workspace).bind(&view.repo).bind(&view.commit).bind(view.source_format_version as i64).bind(view.object_format).bind(&view.semantic_digest).bind(&view.object_set_digest).bind(view.object_count as i64).bind(view.total_bytes as i64).bind(&a.operation_id).bind(a.generation as i64).bind(now).bind(now).execute(&mut *tx).await?;
            for m in &view.members{sqlx::query("INSERT INTO git_source_members(root_hash,ordinal,child_hash,child_len,kind) VALUES(?,?,?,?,?)").bind(&view.root.hash).bind(m.ordinal as i64).bind(&m.blob.hash).bind(m.blob.len as i64).bind(m.kind).execute(&mut *tx).await?;}
            sqlx::query("UPDATE git_source_acquisitions SET state='registered',active_identity=NULL,expires_at=0 WHERE token=? AND generation=? AND state='activation_unknown'").bind(&a.token).bind(a.generation as i64).execute(&mut *tx).await?;sqlx::query("UPDATE git_source_desires SET state='registered',root_hash=?,failure_class=NULL,acquisition_token=NULL,updated_at=? WHERE acquisition_token=? AND state='acquiring'").bind(&view.root.hash).bind(now).bind(&a.token).execute(&mut *tx).await?;let snapshot=DurableSourceSnapshot::registered(view.workspace.clone(),view.repo.clone(),view.commit.clone(),view.root.hash.clone(),a.token.clone(),a.generation)?;tx.commit().await?;Ok(snapshot)}.await;
        match registration {
            Ok(v) => Ok(v),
            Err(error) => match self.reconcile_activation_unknown(a).await? {
                Some(v) => Ok(v),
                None => Err(error).context("MySQL source registration settled failed"),
            },
        }
    }

    pub async fn reconcile_activation_unknown(
        &self,
        a: &GitSourceAcquisition,
    ) -> Result<Option<DurableSourceSnapshot>> {
        let mut tx = self.pool.begin().await?;
        let root:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_roots WHERE root_hash=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND registration_operation=? AND registration_generation=? AND state='registered'").bind(&a.root.hash).bind(&a.workspace).bind(&a.repo).bind(&a.commit).bind(a.source_format_version as i64).bind(&a.operation_id).bind(a.generation as i64).fetch_one(&mut *tx).await?;
        if root == 1 {
            sqlx::query("UPDATE git_source_acquisitions SET state='registered',active_identity=NULL,expires_at=0 WHERE token=? AND generation=? AND state='activation_unknown'").bind(&a.token).bind(a.generation as i64).execute(&mut *tx).await?;
            sqlx::query("UPDATE git_source_desires SET state='registered',root_hash=?,failure_class=NULL,acquisition_token=NULL,updated_at=UNIX_TIMESTAMP() WHERE acquisition_token=? AND state='acquiring'").bind(&a.root.hash).bind(&a.token).execute(&mut *tx).await?;
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
        sqlx::query("UPDATE git_source_desires SET state='failed',root_hash=NULL,failure_class='retryable',acquisition_token=NULL,updated_at=UNIX_TIMESTAMP() WHERE acquisition_token=? AND state='acquiring'").bind(&a.token).execute(&mut *tx).await?;
        sqlx::query("UPDATE git_source_acquisitions SET state='failed',active_identity=NULL,failure_class='retryable',expires_at=0 WHERE token=? AND generation=? AND state='activation_unknown'").bind(&a.token).bind(a.generation as i64).execute(&mut *tx).await?;
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
        let rows=sqlx::query("WITH objects(hash,len,owner) AS (SELECT root_hash,root_len,CONCAT('r:',root_hash) FROM git_source_roots UNION ALL SELECT child_hash,child_len,CONCAT('r:',root_hash,':',LPAD(ordinal,20,'0')) FROM git_source_members UNION ALL SELECT root_hash,root_len,CONCAT('a:',token) FROM git_source_acquisitions WHERE state='activation_unknown' OR (state='graph_published' AND expires_at>UNIX_TIMESTAMP()) UNION ALL SELECT m.child_hash,m.child_len,CONCAT('a:',m.token,':',LPAD(m.ordinal,20,'0')) FROM git_source_acquisition_members m JOIN git_source_acquisitions a ON a.token=m.token WHERE a.state='activation_unknown' OR (a.state='graph_published' AND a.expires_at>UNIX_TIMESTAMP())) SELECT hash,len,owner FROM objects WHERE hash>? OR (hash=? AND owner>?) ORDER BY hash,owner LIMIT ?").bind(hash).bind(hash).bind(owner).bind(limit as i64).fetch_all(&self.pool).await?;
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
        let row=sqlx::query("SELECT r.root_hash,r.root_len,r.object_format,r.registration_generation,r.registration_operation FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id JOIN git_source_roots r ON r.root_hash=i.source_root_hash WHERE i.artifact_id=? AND i.state='promoted' AND i.workspace=? AND i.repo=? AND i.commit_oid=? AND i.source_format_version=? AND j.state='running' AND j.owner=? AND j.lease_generation=? AND j.lease_expires_at>UNIX_TIMESTAMP() AND r.state='registered' FOR UPDATE").bind(artifact_id).bind(workspace).bind(repo).bind(commit).bind(SOURCE_FORMAT_VERSION as i64).bind(artifact_owner).bind(lease_generation as i64).fetch_optional(&mut *tx).await?.context("promoted artifact does not own a live registered source claim")?;
        let root = CasBlob {
            hash: row.try_get("root_hash")?,
            len: checked_u64(row.try_get("root_len")?, "root length")?,
        };
        let consumer = format!("builder:{artifact_id}:{session_id}");
        sqlx::query("INSERT INTO git_source_consumers(root_hash,consumer_id,session_id,workspace,repo,commit_oid,source_format_version,purpose,expires_at) VALUES(?,?,?,?,?,?,?,'builder',UNIX_TIMESTAMP()+?) ON DUPLICATE KEY UPDATE expires_at=IF(root_hash=VALUES(root_hash) AND consumer_id=VALUES(consumer_id) AND session_id=VALUES(session_id) AND workspace=VALUES(workspace) AND repo=VALUES(repo) AND commit_oid=VALUES(commit_oid) AND source_format_version=VALUES(source_format_version) AND purpose='builder',VALUES(expires_at),expires_at)").bind(&root.hash).bind(&consumer).bind(session_id).bind(workspace).bind(repo).bind(commit).bind(SOURCE_FORMAT_VERSION as i64).bind(ttl).execute(&mut *tx).await?;
        let exact:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_consumers WHERE root_hash=? AND consumer_id=? AND session_id=? AND workspace=? AND repo=? AND commit_oid=? AND source_format_version=? AND purpose='builder' AND expires_at>UNIX_TIMESTAMP()").bind(&root.hash).bind(&consumer).bind(session_id).bind(workspace).bind(repo).bind(commit).bind(SOURCE_FORMAT_VERSION as i64).fetch_one(&mut *tx).await?;
        if exact != 1 {
            bail!("builder source session is already bound to another identity")
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
        Ok(sqlx::query("UPDATE git_source_consumers SET expires_at=UNIX_TIMESTAMP()+? WHERE root_hash=? AND session_id=? AND purpose='builder' AND expires_at>UNIX_TIMESTAMP() AND EXISTS(SELECT 1 FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.artifact_id=? AND i.source_root_hash=git_source_consumers.root_hash AND i.state='promoted' AND j.state='running' AND j.owner=? AND j.lease_generation=? AND j.lease_expires_at>UNIX_TIMESTAMP())").bind(ttl).bind(root).bind(session).bind(artifact_id).bind(owner).bind(generation as i64).execute(&self.pool).await?.rows_affected()==1)
    }
    pub async fn release_builder_claim(&self, root: &str, session: &str) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM git_source_consumers WHERE root_hash=? AND session_id=? AND purpose='builder'").bind(root).bind(session).execute(&self.pool).await?.rows_affected()==1)
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
        let ids: Vec<(i64,String)> = sqlx::query_as(
            "WITH candidates AS (SELECT id,workspace,row_number() OVER(PARTITION BY workspace ORDER BY updated_at,id) round_number FROM artifact_intents WHERE state='deferred') SELECT id,workspace FROM candidates ORDER BY round_number,CASE WHEN workspace>? THEN 0 ELSE 1 END,workspace,id LIMIT ?",
        ).bind(&cursor).bind(scan_limit)
        .fetch_all(&self.pool)
        .await?;
        let mut promoted = 0;
        for (id, candidate_workspace) in ids {
            if promoted >= limit {
                break;
            }
            let mut tx = self.pool.begin().await?;
            sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
                .fetch_one(&mut *tx)
                .await?;
            sqlx::query("UPDATE git_source_maintenance SET intent_cursor=?,intent_workspace_cursor=?,updated_at=UNIX_TIMESTAMP() WHERE id=1").bind(id).bind(&candidate_workspace).execute(&mut *tx).await?;
            let Some(row)=sqlx::query("SELECT workspace,repo,branch,branch_generation,commit_oid,kind,format_version,consumer_id FROM artifact_intents WHERE id=? AND state='deferred' FOR UPDATE").bind(id).fetch_optional(&mut *tx).await? else{tx.commit().await?;continue};
            let workspace: String = row.try_get("workspace")?;
            let kind = ArtifactKind::parse(row.try_get("kind")?)?;
            let repo: &str = row.try_get("repo")?;
            let commit: &str = row.try_get("commit_oid")?;
            let format: i64 = row.try_get("format_version")?;
            let existing:Option<i64>=sqlx::query_scalar("SELECT id FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?").bind(&workspace).bind(repo).bind(commit).bind(kind.as_str()).bind(format).fetch_optional(&mut *tx).await?;
            if existing.is_none()
                && !mysql_capacity(&mut tx, &self.scheduler_limits, &workspace, kind).await?
            {
                // Persist rotation even when this workspace is saturated. A
                // rollback here would pin every invocation to the same blocked
                // prefix and recreate starvation across page boundaries.
                tx.commit().await?;
                continue;
            }
            let artifact = match existing {
                Some(id) => id,
                None => mysql_ensure_job(&mut tx, &workspace, repo, commit, kind, format).await?,
            };
            sqlx::query("UPDATE artifact_intents SET state='promoted',artifact_id=?,updated_at=UNIX_TIMESTAMP() WHERE id=? AND state='deferred'").bind(artifact).bind(id).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?) ON DUPLICATE KEY UPDATE expires_at=VALUES(expires_at)").bind(artifact).bind(row.try_get::<String,_>("consumer_id")?).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *tx).await?;
            mysql_upsert_observation(
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
            tx.commit().await?;
            promoted += 1
        }
        Ok(promoted)
    }

    pub async fn reconcile_terminal_intents(&self, limit: u32) -> Result<u32> {
        if limit == 0 || limit > 512 {
            bail!("intent reconciliation page is invalid")
        }
        let ids:Vec<(i64,i64,String)>=sqlx::query_as("SELECT i.id,i.artifact_id,i.consumer_id FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.state='promoted' AND (j.state='ready' OR (j.state='failed' AND (j.failure_class IN('permanent','dead_letter') OR (j.failure_class='retryable' AND j.retry_count>=?)))) ORDER BY i.id LIMIT ?").bind(self.scheduler_limits.max_manual_retries as i64).bind(limit as i64).fetch_all(&self.pool).await?;
        let mut settled = 0;
        for (id, artifact, consumer) in ids {
            let mut tx = self.pool.begin().await?;
            let terminal:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_intents i JOIN artifact_jobs j ON j.id=i.artifact_id WHERE i.id=? AND i.artifact_id=? AND i.consumer_id=? AND i.state='promoted' AND (j.state='ready' OR (j.state='failed' AND (j.failure_class IN('permanent','dead_letter') OR (j.failure_class='retryable' AND j.retry_count>=?)))) FOR UPDATE").bind(id).bind(artifact).bind(&consumer).bind(self.scheduler_limits.max_manual_retries as i64).fetch_one(&mut *tx).await?;
            if terminal != 1 {
                tx.rollback().await?;
                continue;
            }
            if sqlx::query(
                "DELETE FROM git_source_consumers WHERE consumer_id=? AND purpose='intent'",
            )
            .bind(&consumer)
            .execute(&mut *tx)
            .await?
            .rows_affected()
                != 1
                || sqlx::query(
                    "DELETE FROM artifact_consumers WHERE artifact_id=? AND consumer_id=?",
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
                "DELETE FROM artifact_intents WHERE id=? AND artifact_id=? AND state='promoted'",
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
        let mut changed=sqlx::query("DELETE FROM git_source_consumers WHERE purpose='builder' AND expires_at<=UNIX_TIMESTAMP() ORDER BY expires_at,root_hash,consumer_id LIMIT ?").bind(limit as i64).execute(&mut *tx).await?.rows_affected();
        changed+=sqlx::query("DELETE FROM branch_source_generations WHERE (workspace,repo,branch,generation) IN (SELECT workspace,repo,branch,generation FROM (SELECT g.workspace,g.repo,g.branch,g.generation FROM branch_source_generations g LEFT JOIN branch_source_current c ON c.workspace=g.workspace AND c.repo=g.repo AND c.branch=g.branch AND c.generation=g.generation LEFT JOIN artifact_intents i ON i.workspace=g.workspace AND i.repo=g.repo AND i.branch=g.branch AND i.branch_generation=g.generation WHERE c.workspace IS NULL AND i.id IS NULL ORDER BY g.created_at,g.workspace,g.repo,g.branch,g.generation LIMIT ?) victims)").bind(limit as i64).execute(&mut *tx).await?.rows_affected();
        let cutoff: i64 = sqlx::query_scalar(
            "SELECT GREATEST(0,generation-1024) FROM git_source_acquisition_sequence WHERE id=1",
        )
        .fetch_one(&mut *tx)
        .await?;
        changed+=sqlx::query("DELETE FROM git_source_acquisitions WHERE token IN (SELECT token FROM (SELECT a.token FROM git_source_acquisitions a LEFT JOIN git_source_desires d ON d.acquisition_token=a.token WHERE a.state='failed' AND a.generation<=? AND d.acquisition_token IS NULL ORDER BY a.generation LIMIT ?) victims)").bind(cutoff).bind(limit as i64).execute(&mut *tx).await?.rows_affected();
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
        let sweep: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifact_gc_sweep WHERE expires_at>UNIX_TIMESTAMP()",
        )
        .fetch_one(&mut *tx)
        .await?;
        if sweep != 0 {
            bail!("source root retirement is fenced by live GC sweep")
        }
        let cursor: String = sqlx::query_scalar(
            "SELECT root_cursor FROM git_source_maintenance WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let roots:Vec<String>=sqlx::query_scalar("SELECT r.root_hash FROM git_source_roots r WHERE r.state='registered' AND r.registered_at<=UNIX_TIMESTAMP()-? AND r.root_hash>? AND NOT EXISTS(SELECT 1 FROM branch_source_generations g WHERE g.root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM artifact_intents i WHERE i.source_root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM git_source_consumers c WHERE c.root_hash=r.root_hash) AND NOT EXISTS(SELECT 1 FROM git_source_acquisitions a WHERE a.root_hash=r.root_hash AND a.state IN('held','graph_published','activation_unknown')) ORDER BY r.root_hash LIMIT ? FOR UPDATE").bind(grace_secs).bind(&cursor).bind(limit as i64).fetch_all(&mut *tx).await?;
        if roots.is_empty() {
            if !cursor.is_empty() {
                sqlx::query("UPDATE git_source_maintenance SET root_cursor='',updated_at=UNIX_TIMESTAMP() WHERE id=1").execute(&mut *tx).await?;
            }
            tx.commit().await?;
            return Ok(0);
        }
        for root in &roots {
            sqlx::query("DELETE FROM git_source_desires WHERE root_hash=? AND state='registered'")
                .bind(root)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM git_source_acquisitions WHERE root_hash=? AND state IN('registered','failed')").bind(root).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM git_source_members WHERE root_hash=?")
                .bind(root)
                .execute(&mut *tx)
                .await?;
            if sqlx::query("DELETE FROM git_source_roots WHERE root_hash=? AND state='registered' AND NOT EXISTS(SELECT 1 FROM branch_source_generations WHERE root_hash=?) AND NOT EXISTS(SELECT 1 FROM artifact_intents WHERE source_root_hash=?) AND NOT EXISTS(SELECT 1 FROM git_source_consumers WHERE root_hash=?)").bind(root).bind(root).bind(root).bind(root).execute(&mut *tx).await?.rows_affected()!=1{bail!("source root retirement lost its reference proof")}
        }
        sqlx::query("UPDATE git_source_maintenance SET root_cursor=?,updated_at=UNIX_TIMESTAMP() WHERE id=1").bind(roots.last().unwrap()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(roots.len() as u32)
    }
}

#[async_trait]
impl ArtifactObservation for MysqlGitSourceRegistry {
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
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM scheduler_state WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;
        let registered:i64=sqlx::query_scalar("SELECT count(*) FROM git_source_acquisitions a JOIN git_source_roots r ON r.root_hash=a.root_hash WHERE a.token=? AND a.generation=? AND a.state='registered' AND a.workspace=? AND a.repo=? AND a.commit_oid=? AND a.root_hash=? AND r.state='registered'").bind(source.registration_token()).bind(source.registration_generation() as i64).bind(source.workspace()).bind(source.repo()).bind(source.commit()).bind(source.manifest()).fetch_one(&mut *tx).await?;
        if registered != 1 {
            bail!("source snapshot is not an exact registered capability")
        }
        let current:Option<(i64,String)>=sqlx::query_as("SELECT generation,desired_commit FROM branch_observations WHERE workspace=? AND repo=? AND branch=? FOR UPDATE").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).fetch_optional(&mut *tx).await?;
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
            let deferred:Vec<String>=sqlx::query_scalar("SELECT consumer_id FROM artifact_intents WHERE workspace=? AND repo=? AND branch=? AND state='deferred'").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).fetch_all(&mut *tx).await?;
            for consumer in deferred {
                sqlx::query(
                    "DELETE FROM git_source_consumers WHERE consumer_id=? AND purpose='intent'",
                )
                .bind(consumer)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query("DELETE FROM artifact_intents WHERE workspace=? AND repo=? AND branch=? AND state='deferred'").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO branch_source_generations(workspace,repo,branch,generation,commit_oid,source_format_version,root_hash,created_at) VALUES(?,?,?,?,?,?,?,UNIX_TIMESTAMP())").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).bind(SOURCE_FORMAT_VERSION as i64).bind(source.manifest()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO branch_observations(workspace,repo,branch,generation,desired_commit,updated_at) VALUES(?,?,?,?,?,UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE generation=VALUES(generation),desired_commit=VALUES(desired_commit),updated_at=VALUES(updated_at)").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO branch_source_current(workspace,repo,branch,generation) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE generation=VALUES(generation)").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).execute(&mut *tx).await?;
        } else {
            let exact:i64=sqlx::query_scalar("SELECT count(*) FROM branch_source_generations g JOIN branch_source_current c ON c.workspace=g.workspace AND c.repo=g.repo AND c.branch=g.branch AND c.generation=g.generation WHERE g.workspace=? AND g.repo=? AND g.branch=? AND g.generation=? AND g.commit_oid=? AND g.root_hash=?").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.commit()).bind(source.manifest()).fetch_one(&mut *tx).await?;
            if exact != 1 {
                bail!("same-tip source generation differs from registered capability")
            }
        }
        let mut outcomes = Vec::new();
        for kind in unique {
            if let Some((id,state,artifact))=sqlx::query_as::<_,(i64,String,Option<i64>)>("SELECT id,state,artifact_id FROM artifact_intents WHERE workspace=? AND repo=? AND branch=? AND branch_generation=? AND kind=? AND format_version=?").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(kind.as_str()).bind(format_version as i64).fetch_optional(&mut *tx).await?{if state=="deferred"{outcomes.push((kind,ArtifactIntentOutcome::Deferred(id)));continue}outcomes.push((kind,mysql_job_outcome(&mut tx,artifact.context("promoted intent lacks artifact")?,intent,self.scheduler_limits.max_manual_retries).await?));continue}
            let consumer = format!(
                "{}{}",
                SOURCE_INTENT_CONSUMER_PREFIX,
                hex::encode(rand::random::<[u8; 24]>())
            );
            let session = hex::encode(rand::random::<[u8; 32]>());
            let existing:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?").bind(snapshot.workspace()).bind(snapshot.repo()).bind(source.commit()).bind(kind.as_str()).bind(format_version as i64).fetch_one(&mut *tx).await?;
            let promote = existing == 1
                || mysql_capacity(&mut tx, &self.scheduler_limits, snapshot.workspace(), kind)
                    .await?;
            let artifact = if promote {
                Some(
                    mysql_ensure_job(
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
            let result=sqlx::query("INSERT INTO artifact_intents(workspace,repo,branch,branch_generation,source_root_hash,source_format_version,commit_oid,kind,format_version,state,artifact_id,consumer_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?, ?,UNIX_TIMESTAMP(),UNIX_TIMESTAMP())").bind(snapshot.workspace()).bind(snapshot.repo()).bind(snapshot.branch()).bind(generation as i64).bind(source.manifest()).bind(SOURCE_FORMAT_VERSION as i64).bind(source.commit()).bind(kind.as_str()).bind(format_version as i64).bind(if promote{"promoted"}else{"deferred"}).bind(artifact).bind(&consumer).execute(&mut *tx).await?;
            let intent_id = result.last_insert_id() as i64;
            sqlx::query("INSERT INTO git_source_consumers(root_hash,consumer_id,session_id,workspace,repo,commit_oid,source_format_version,purpose,expires_at) VALUES(?,?,?,?,?,?,?,'intent',?)").bind(source.manifest()).bind(&consumer).bind(session).bind(source.workspace()).bind(source.repo()).bind(source.commit()).bind(SOURCE_FORMAT_VERSION as i64).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *tx).await?;
            if let Some(artifact) = artifact {
                sqlx::query("INSERT INTO artifact_consumers(artifact_id,consumer_id,expires_at) VALUES(?,?,?)").bind(artifact).bind(&consumer).bind(SOURCE_INTENT_RETENTION_EXPIRY).execute(&mut *tx).await?;
                mysql_upsert_observation(
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
                    mysql_job_outcome(
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

async fn mysql_upsert_observation(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    workspace: &str,
    repo: &str,
    branch: &str,
    generation: i64,
    commit: &str,
    kind: ArtifactKind,
    artifact: i64,
    format: i64,
) -> Result<()> {
    sqlx::query("INSERT INTO artifact_observations(workspace,repo,branch,kind,desired_commit,desired_artifact_id,desired_generation,published_artifact_id,format_version,observed_at) VALUES(?,?,?,?,?,?,?,CASE WHEN (SELECT state FROM artifact_jobs WHERE id=?)='ready' THEN ? ELSE NULL END,?,UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE desired_commit=VALUES(desired_commit),desired_artifact_id=VALUES(desired_artifact_id),desired_generation=VALUES(desired_generation),published_artifact_id=CASE WHEN VALUES(published_artifact_id) IS NOT NULL THEN VALUES(published_artifact_id) WHEN artifact_observations.format_version=VALUES(format_version) THEN artifact_observations.published_artifact_id ELSE NULL END,format_version=VALUES(format_version),observed_at=VALUES(observed_at)").bind(workspace).bind(repo).bind(branch).bind(kind.as_str()).bind(commit).bind(artifact).bind(generation).bind(artifact).bind(artifact).bind(format).execute(&mut **tx).await?;
    Ok(())
}

async fn mysql_capacity(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    limits: &SchedulerLimits,
    workspace: &str,
    kind: ArtifactKind,
) -> Result<bool> {
    let total: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running')")
            .fetch_one(&mut **tx)
            .await?;
    let local: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND workspace=?",
    )
    .bind(workspace)
    .fetch_one(&mut **tx)
    .await?;
    let lane: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM artifact_jobs WHERE state IN('queued','running') AND kind=?",
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
async fn mysql_ensure_job(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    workspace: &str,
    repo: &str,
    commit: &str,
    kind: ArtifactKind,
    format: i64,
) -> Result<i64> {
    if let Some(id)=sqlx::query_scalar("SELECT id FROM artifact_jobs WHERE workspace=? AND repo=? AND commit_oid=? AND kind=? AND format_version=?").bind(workspace).bind(repo).bind(commit).bind(kind.as_str()).bind(format).fetch_optional(&mut **tx).await?{return Ok(id)}
    Ok(sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES(?,?,?,?,?,'queued',UNIX_TIMESTAMP(),UNIX_TIMESTAMP())").bind(workspace).bind(repo).bind(commit).bind(kind.as_str()).bind(format).execute(&mut **tx).await?.last_insert_id() as i64)
}
async fn mysql_job_outcome(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    id: i64,
    intent: SyncIntent,
    max_retries: u32,
) -> Result<ArtifactIntentOutcome> {
    let row = sqlx::query("SELECT state,failure_class,retry_count FROM artifact_jobs WHERE id=?")
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
    let mut state: String = row.try_get("state")?;
    let class = row
        .try_get::<Option<String>, _>("failure_class")?
        .map(|v| FailureClass::parse(&v))
        .transpose()?;
    let retries = checked_u32(row.try_get("retry_count")?, "artifact retries")?;
    if state=="failed"&&intent==SyncIntent::EnsureCurrent&&class==Some(FailureClass::Retryable)&&retries<max_retries&&sqlx::query("UPDATE artifact_jobs SET state='queued',owner=NULL,heartbeat_at=NULL,lease_expires_at=NULL,manifest=NULL,error=NULL,failure_class=NULL,retry_count=retry_count+1,updated_at=UNIX_TIMESTAMP() WHERE id=? AND state='failed' AND failure_class='retryable' AND retry_count=?").bind(id).bind(retries as i64).execute(&mut **tx).await?.rows_affected()==1{state="queued".into()}
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
async fn mysql_time(tx: &mut sqlx::Transaction<'_, sqlx::MySql>) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT UNIX_TIMESTAMP()")
        .fetch_one(&mut **tx)
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{
        ClaimedArtifact, CompletionEvidence, CompletionVerifier, validate_evidence,
    };
    use crate::artifact_scheduler_mysql::MysqlArtifactScheduler;
    use crate::git_source::prepared_source_for_registry_test;
    use sqlx::mysql::MySqlPoolOptions;
    struct Accept;
    impl CompletionVerifier for Accept {
        fn identity(&self) -> &'static str {
            "mysql-source-registry-live-v1"
        }
        fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
            validate_evidence(claim, evidence)
        }
    }
    #[tokio::test]
    async fn mysql_source_registry_lifecycle_live() {
        let Some(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL").ok() else {
            if std::env::var_os("RIPCLONE_REQUIRE_MYSQL_TESTS").is_some() {
                panic!("mysql_source_registry_lifecycle_live requires RIPCLONE_TEST_MYSQL_URL")
            }
            eprintln!("SKIP mysql_source_registry_lifecycle_live: RIPCLONE_TEST_MYSQL_URL unset");
            return;
        };
        let pool = MySqlPoolOptions::new()
            .max_connections(12)
            .connect(&url)
            .await
            .unwrap();
        let mut lock = pool.acquire().await.unwrap().detach();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT GET_LOCK('ripclone_mysql_source_registry_test',30)"
            )
            .fetch_one(&mut lock)
            .await
            .unwrap(),
            1
        );
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
            sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE IF EXISTS {table}")))
                .execute(&pool)
                .await
                .unwrap();
        }
        let limits = SchedulerLimits {
            workspace_backlog: 1,
            ..SchedulerLimits::default()
        };
        MysqlArtifactScheduler::from_pool(pool.clone(), limits.clone(), Arc::new(Accept))
            .await
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(temp.path()).unwrap();
        sqlx::query("UPDATE git_source_maintenance SET intent_cursor=1 WHERE id=1")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            MysqlGitSourceRegistry::new(
                pool.clone(),
                storage.clone(),
                limits.clone(),
                GitSourceLimits::default(),
                [7; 32]
            )
            .await
            .is_err(),
            "non-pristine empty fingerprint was adopted"
        );
        sqlx::query("UPDATE git_source_maintenance SET intent_cursor=0 WHERE id=1")
            .execute(&pool)
            .await
            .unwrap();
        let registry = MysqlGitSourceRegistry::new(
            pool.clone(),
            storage.clone(),
            limits.clone(),
            GitSourceLimits::default(),
            [7; 32],
        )
        .await
        .unwrap();
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
        let (acquisition, _) = registry
            .bind_prepared_graph(&permit, &prepared)
            .await
            .unwrap();
        assert!(
            registry
                .register(&acquisition, &prepared, &CancellationToken::new())
                .await
                .is_ok()
        );
        let concurrent_commit = "c".repeat(40);
        let concurrent_source = prepared_source_for_registry_test(
            "ws",
            "o/r",
            &concurrent_commit,
            CasBlob {
                hash: hex::encode(Sha256::digest(pack_bytes)),
                len: 4,
            },
            CasBlob {
                hash: hex::encode(Sha256::digest(index_bytes)),
                len: 5,
            },
        )
        .unwrap();
        let concurrent_view = concurrent_source
            .registry_view(&GitSourceLimits::default())
            .unwrap();
        storage
            .put(&concurrent_view.root.hash, &concurrent_view.root_bytes)
            .unwrap();
        let left = registry.clone();
        let right = registry.clone();
        let (one, two) = tokio::join!(
            left.begin_acquisition(
                "ws",
                "o/r",
                &concurrent_commit,
                1,
                "one",
                "one",
                60,
                SyncIntent::EnsureCurrent
            ),
            right.begin_acquisition(
                "ws",
                "o/r",
                &concurrent_commit,
                1,
                "two",
                "two",
                60,
                SyncIntent::EnsureCurrent
            )
        );
        let one = one.unwrap();
        let two = two.unwrap();
        assert!(matches!(
            (&one, &two),
            (
                SourceBeginOutcome::PermitToPrepare(_),
                SourceBeginOutcome::Deferred { .. }
            ) | (
                SourceBeginOutcome::Deferred { .. },
                SourceBeginOutcome::PermitToPrepare(_)
            )
        ));
        let concurrent_permit = match (one, two) {
            (SourceBeginOutcome::PermitToPrepare(v), _)
            | (_, SourceBeginOutcome::PermitToPrepare(v)) => v,
            _ => unreachable!(),
        };
        let (concurrent_acquisition, _) = registry
            .bind_prepared_graph(&concurrent_permit, &concurrent_source)
            .await
            .unwrap();
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            registry
                .register(&concurrent_acquisition, &concurrent_source, &cancelled)
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM git_source_acquisitions WHERE token=?"
            )
            .bind(&concurrent_acquisition.token)
            .fetch_one(&pool)
            .await
            .unwrap(),
            "failed",
            "cancelled verification left an active acquisition"
        );
        let snapshot = match registry
            .begin_acquisition(
                "ws",
                "o/r",
                &"a".repeat(40),
                1,
                "owner",
                "again",
                60,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap()
        {
            SourceBeginOutcome::Ready(v) => v,
            _ => panic!("expected registered"),
        };
        let before = registry.snapshot("ws", "o/r", "main").await.unwrap();
        let outcome = registry
            .record_tip_and_intents(
                &before,
                &snapshot,
                &[ArtifactKind::Head, ArtifactKind::Files],
                1,
                SyncIntent::EnsureCurrent,
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome,ArtifactObservationOutcome::Recorded{artifacts,..} if artifacts.len()==2)
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM git_source_consumers WHERE purpose='intent' AND expires_at=9223372036854775807").fetch_one(&pool).await.unwrap(),2);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM artifact_observations WHERE workspace='ws' AND repo='o/r' AND branch='main'").fetch_one(&pool).await.unwrap(),1,"only the immediately promoted mode publishes an observation under workspace capacity one");
        sqlx::query("INSERT INTO artifact_gc_sweep(id,owner,expires_at) VALUES(1,'test',UNIX_TIMESTAMP()+60)").execute(&pool).await.unwrap();
        assert!(registry.retire_registered_roots_page(60, 1).await.is_err());
        sqlx::query("DELETE FROM artifact_gc_sweep")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            registry.retire_registered_roots_page(60, 1).await.unwrap(),
            0,
            "live branch and intent roots are not retired"
        );

        // More than 64 old deferred rows in one saturated workspace must not
        // hide a first-round eligible workspace behind the scan prefix.
        let mut planted = pool.acquire().await.unwrap();
        sqlx::query("SET FOREIGN_KEY_CHECKS=0")
            .execute(&mut *planted)
            .await
            .unwrap();
        for ordinal in 0..80i64 {
            sqlx::query("INSERT INTO artifact_intents(workspace,repo,branch,branch_generation,source_root_hash,source_format_version,commit_oid,kind,format_version,state,artifact_id,consumer_id,created_at,updated_at) VALUES('ws','o/r',?,1,?,1,?,'head',?,'deferred',NULL,?,1,1)").bind(format!("blocked-{ordinal}")).bind(&view.root.hash).bind("a".repeat(40)).bind(1000+ordinal).bind(format!("plant-blocked-{ordinal}")).execute(&mut *planted).await.unwrap();
        }
        sqlx::query("INSERT INTO branch_observations(workspace,repo,branch,generation,desired_commit,updated_at) VALUES('z','o/r','eligible',1,?,1)").bind("b".repeat(40)).execute(&mut *planted).await.unwrap();
        sqlx::query("INSERT INTO artifact_intents(workspace,repo,branch,branch_generation,source_root_hash,source_format_version,commit_oid,kind,format_version,state,artifact_id,consumer_id,created_at,updated_at) VALUES('z','o/r','eligible',1,?,1,?,'head',1,'deferred',NULL,'plant-eligible',1,1)").bind(&view.root.hash).bind("b".repeat(40)).execute(&mut *planted).await.unwrap();
        sqlx::query("SET FOREIGN_KEY_CHECKS=1")
            .execute(&mut *planted)
            .await
            .unwrap();
        drop(planted);
        assert_eq!(registry.promote_deferred_page(1).await.unwrap(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM artifact_intents WHERE workspace='z' AND state='promoted'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1,
            "fair promotion skipped the saturated >64-row prefix"
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM artifact_observations WHERE workspace='z' AND branch='eligible'").fetch_one(&pool).await.unwrap(),1,"deferred promotion atomically published observation");
        sqlx::query("DELETE FROM artifact_consumers WHERE consumer_id LIKE 'plant-%'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM artifact_observations WHERE workspace='z'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM artifact_intents WHERE consumer_id LIKE 'plant-%'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM branch_observations WHERE workspace='z'")
            .execute(&pool)
            .await
            .unwrap();
        let deferred=sqlx::query("SELECT id,workspace,repo,commit_oid,kind,format_version FROM artifact_intents WHERE workspace='ws' AND state='deferred' ORDER BY id LIMIT 1").fetch_one(&pool).await.unwrap();
        let existing_artifact=sqlx::query("INSERT INTO artifact_jobs(workspace,repo,commit_oid,kind,format_version,state,created_at,updated_at) VALUES(?,?,?,?,?,'queued',UNIX_TIMESTAMP(),UNIX_TIMESTAMP())")
            .bind(deferred.try_get::<String,_>("workspace").unwrap())
            .bind(deferred.try_get::<String,_>("repo").unwrap())
            .bind(deferred.try_get::<String,_>("commit_oid").unwrap())
            .bind(deferred.try_get::<String,_>("kind").unwrap())
            .bind(deferred.try_get::<i64,_>("format_version").unwrap())
            .execute(&pool)
            .await
            .unwrap()
            .last_insert_id() as i64;
        assert_eq!(
            registry.promote_deferred_page(1).await.unwrap(),
            1,
            "a deferred intent did not reuse an existing job at capacity"
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT artifact_id FROM artifact_intents WHERE id=? AND state='promoted'"
            )
            .bind(deferred.try_get::<i64, _>("id").unwrap())
            .fetch_one(&pool)
            .await
            .unwrap(),
            Some(existing_artifact)
        );

        let promoted:Vec<i64>=sqlx::query_scalar("SELECT artifact_id FROM artifact_intents WHERE workspace='ws' AND state='promoted' ORDER BY id LIMIT 2").fetch_all(&pool).await.unwrap();
        assert_eq!(promoted.len(), 2);
        for (ordinal, artifact) in promoted.iter().enumerate() {
            sqlx::query("UPDATE artifact_jobs SET state='running',owner=?,heartbeat_at=UNIX_TIMESTAMP(),lease_expires_at=UNIX_TIMESTAMP()+60,lease_generation=1,claim_attempts=1 WHERE id=?")
                .bind(format!("builder-{ordinal}"))
                .bind(artifact)
                .execute(&pool)
                .await
                .unwrap();
        }
        let shared_session = "e".repeat(64);
        registry
            .claim_authenticated(
                promoted[0],
                "builder-0",
                1,
                "ws",
                "o/r",
                &"a".repeat(40),
                &shared_session,
                60,
            )
            .await
            .unwrap();
        assert!(
            registry
                .claim_authenticated(
                    promoted[1],
                    "builder-1",
                    1,
                    "ws",
                    "o/r",
                    &"a".repeat(40),
                    &shared_session,
                    60,
                )
                .await
                .is_err(),
            "builder session collision returned authority without an exact lease"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM git_source_consumers WHERE session_id=?"
            )
            .bind(&shared_session)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        sqlx::query("UPDATE git_source_consumers SET expires_at=0 WHERE session_id=?")
            .bind(&shared_session)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            registry.prune_metadata_page(16).await.unwrap() >= 1,
            "expired builder metadata was not pruned"
        );
        validate_mysql_v7_state(&mut pool.acquire().await.unwrap().detach())
            .await
            .unwrap();

        // The forward FKs and row CHECKs deliberately cannot express these
        // cross-row proofs. Startup validation must reject every malformed
        // hybrid, while a rollback proves the negative test is non-destructive.
        let mut corrupt = pool.acquire().await.unwrap().detach();
        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE git_source_acquisitions SET operation_id='planted-operation' WHERE token=?",
        )
        .bind(snapshot.registration_token())
        .execute(&mut corrupt)
        .await
        .unwrap();
        sqlx::query("UPDATE git_source_roots SET registration_operation='planted-operation' WHERE root_hash=?")
            .bind(snapshot.manifest())
            .execute(&mut corrupt)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "non-deterministic acquisition operation provenance was accepted"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET semantic_digest=? WHERE token=?")
            .bind("d".repeat(64))
            .bind(snapshot.registration_token())
            .execute(&mut corrupt)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "registered acquisition/root descriptor disagreement was accepted"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE git_source_acquisition_members SET kind='index' WHERE token=? AND ordinal=0",
        )
        .bind(snapshot.registration_token())
        .execute(&mut corrupt)
        .await
        .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "provisional pack/index parity corruption was accepted"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET object_format='sha256' WHERE token=?")
            .bind(snapshot.registration_token())
            .execute(&mut corrupt)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "registered acquisition/root object-format mismatch was accepted"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        let shared_consumer: String = sqlx::query_scalar(
            "SELECT consumer_id FROM artifact_intents WHERE workspace='ws' ORDER BY id LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query("INSERT INTO artifact_intents(workspace,repo,branch,branch_generation,source_root_hash,source_format_version,commit_oid,kind,format_version,state,artifact_id,consumer_id,created_at,updated_at) SELECT workspace,repo,branch,branch_generation,source_root_hash,source_format_version,commit_oid,'full_history',format_version,'deferred',NULL,?,created_at,updated_at FROM artifact_intents WHERE consumer_id=? LIMIT 1")
            .bind(&shared_consumer)
            .bind(&shared_consumer)
            .execute(&mut corrupt)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "one source consumer was accepted for multiple intents"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query("DELETE FROM git_source_desires WHERE root_hash=?")
            .bind(snapshot.manifest())
            .execute(&mut corrupt)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "registered root and acquisition without a durable desire were accepted"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query("DELETE FROM artifact_intents WHERE workspace='ws'")
            .execute(&mut corrupt)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "orphaned intent source/artifact consumers were accepted"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        sqlx::raw_sql("START TRANSACTION")
            .execute(&mut corrupt)
            .await
            .unwrap();
        sqlx::query("UPDATE git_source_acquisitions SET semantic_digest=? WHERE token=?")
            .bind("A".repeat(64))
            .bind(snapshot.registration_token())
            .execute(&mut corrupt)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_state(&mut corrupt).await.is_err(),
            "uppercase acquisition digest was accepted"
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut corrupt)
            .await
            .unwrap();

        sqlx::raw_sql("CREATE TABLE branch_source_planted(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            validate_mysql_v7_prefix(&mut pool.acquire().await.unwrap().detach(), true)
                .await
                .is_err(),
            "planted source-namespace table was accepted"
        );
        sqlx::raw_sql("DROP TABLE branch_source_planted")
            .execute(&pool)
            .await
            .unwrap();
        validate_mysql_v7_state(&mut corrupt).await.unwrap();
        let _: Option<i64> =
            sqlx::query_scalar("SELECT RELEASE_LOCK('ripclone_mysql_source_registry_test')")
                .fetch_one(&mut lock)
                .await
                .unwrap();
    }
}
