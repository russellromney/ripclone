//! Integration tests that require PostgreSQL, MySQL, or libSQL services.
//!
//! CI executes this harness with one test thread because backend selection is
//! intentionally process-global.

#[path = "common/mod.rs"]
mod common;

/// Reset process-global backend selection before each test in this consolidated
/// harness. CI runs these tests serially, but environment variables otherwise
/// survive from one former test binary to the next.
fn reset_database_backend_env() {
    unsafe {
        for key in [
            "RIPCLONE_QUEUE",
            "RIPCLONE_QUEUE_DB_URL",
            "RIPCLONE_QUEUE_DB_TOKEN",
            "RIPCLONE_METADATA",
            "RIPCLONE_METADATA_DB_URL",
            "RIPCLONE_METADATA_DB_TOKEN",
        ] {
            std::env::remove_var(key);
        }
    }
}

#[path = "suites/network_databases/e2e_metadata_mysql.rs"]
mod e2e_metadata_mysql;
#[path = "suites/network_databases/e2e_metadata_postgres.rs"]
mod e2e_metadata_postgres;
#[path = "suites/network_databases/e2e_worker_libsql.rs"]
mod e2e_worker_libsql;
#[path = "suites/network_databases/e2e_worker_mysql.rs"]
mod e2e_worker_mysql;
#[path = "suites/network_databases/e2e_worker_postgres.rs"]
mod e2e_worker_postgres;
