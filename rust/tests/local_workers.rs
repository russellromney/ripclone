//! Local queue, worker, and dispatcher integration tests.

#[path = "common/mod.rs"]
mod common;

#[path = "suites/local_workers/e2e_api_ref_store.rs"]
mod e2e_api_ref_store;
#[path = "suites/local_workers/e2e_dispatcher.rs"]
mod e2e_dispatcher;
#[path = "suites/local_workers/e2e_farmout_concurrency.rs"]
mod e2e_farmout_concurrency;
#[path = "suites/local_workers/e2e_heartbeat.rs"]
mod e2e_heartbeat;
#[path = "suites/local_workers/e2e_metadata_farmout.rs"]
mod e2e_metadata_farmout;
#[path = "suites/local_workers/e2e_metadata_sqlite.rs"]
mod e2e_metadata_sqlite;
#[path = "suites/local_workers/e2e_sql_queue.rs"]
mod e2e_sql_queue;
#[path = "suites/local_workers/e2e_worker_diskless.rs"]
mod e2e_worker_diskless;
#[path = "suites/local_workers/e2e_worker_idle_exit.rs"]
mod e2e_worker_idle_exit;
#[path = "suites/local_workers/e2e_worker_recovery.rs"]
mod e2e_worker_recovery;
#[path = "suites/local_workers/e2e_worker_sqlite.rs"]
mod e2e_worker_sqlite;
#[path = "suites/local_workers/queue_selection.rs"]
mod queue_selection;
