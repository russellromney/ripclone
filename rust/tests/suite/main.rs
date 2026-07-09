//! Combined integration-test harness.
//!
//! One binary instead of ~50 separate `tests/*.rs` crates. Cargo links each
//! integration test separately; with this many e2e crates that dominated CI
//! wall time (~22 min link vs ~2 min run). Fan-out-only tests (gitea, s3,
//! network DBs) stay as top-level binaries so ci-build can stage them alone.
//!
//! See rust-ci-performance skill: "fewer test binaries" for link-bound suites.

#[path = "../common/mod.rs"]
mod common;

mod archive_bounded;
mod config_backends;
mod docs_cli_surface;
mod e2e_added_repos;
mod e2e_agent_fleet;
mod e2e_async_build;
mod e2e_auth;
mod e2e_billing;
mod e2e_clone_metrics;
mod e2e_compaction;
mod e2e_concurrent_same_repo;
mod e2e_config_clone_mode;
mod e2e_config_global_and_overrides;
mod e2e_config_legacy_token_migration;
mod e2e_config_provider_add_then_clone;
mod e2e_config_sync_defaults;
mod e2e_equivalence;
mod e2e_failure_injection;
mod e2e_farmout_concurrency;
mod e2e_forcepush_rewind;
mod e2e_freshness;
mod e2e_gc_race;
mod e2e_login_logout;
mod e2e_lsm;
mod e2e_matrix_twophase_full;
mod e2e_matrix_twophase_lsm;
mod e2e_metadata_farmout;
mod e2e_metadata_sqlite;
mod e2e_multi_provider;
mod e2e_provider_cli;
mod e2e_remote_helper;
mod e2e_repo_config;
mod e2e_roundtrip;
mod e2e_sql_queue;
mod e2e_sync_at_rev;
mod e2e_sync_phases;
mod e2e_two_phase;
mod e2e_verify_upstream;
mod e2e_version;
mod e2e_webhook;
mod e2e_worker_diskless;
mod e2e_worker_idle_exit;
mod e2e_worker_recovery;
mod e2e_worker_sqlite;
mod head_delta;
mod history_pack_reuse_multipack;
mod lsm_incremental;
mod manifest_tree_proptest;
mod queue_selection;
mod ref_ordering;
mod two_phase_decouple;
