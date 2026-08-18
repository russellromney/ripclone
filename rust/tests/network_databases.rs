//! Integration tests that require PostgreSQL, MySQL, or libSQL services.
//!
//! CI executes this harness with one test thread because backend selection is
//! intentionally process-global.

#[path = "common/mod.rs"]
mod common;

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
