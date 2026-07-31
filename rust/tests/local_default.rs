//! Local end-to-end cases that share the default, non-LSM server configuration.
//!
//! Keeping these cases in one integration-test crate avoids repeatedly linking
//! the full Ripclone dependency graph while retaining module-level test names.

#[path = "common/mod.rs"]
mod common;

#[path = "suites/local_default/e2e_added_repos.rs"]
mod e2e_added_repos;
#[path = "suites/local_default/e2e_agent_fleet.rs"]
mod e2e_agent_fleet;
#[path = "suites/local_default/e2e_async_build.rs"]
mod e2e_async_build;
#[path = "suites/local_default/e2e_billing.rs"]
mod e2e_billing;
#[path = "suites/local_default/e2e_clone_metrics.rs"]
mod e2e_clone_metrics;
#[path = "suites/local_default/e2e_config_global_and_overrides.rs"]
mod e2e_config_global_and_overrides;
#[path = "suites/local_default/e2e_config_legacy_token_migration.rs"]
mod e2e_config_legacy_token_migration;
#[path = "suites/local_default/e2e_failure_injection.rs"]
mod e2e_failure_injection;
#[path = "suites/local_default/e2e_matrix_twophase_full.rs"]
mod e2e_matrix_twophase_full;
#[path = "suites/local_default/e2e_multi_provider.rs"]
mod e2e_multi_provider;
#[path = "suites/local_default/e2e_remote_helper.rs"]
mod e2e_remote_helper;
#[path = "suites/local_default/e2e_repo_config.rs"]
mod e2e_repo_config;
#[path = "suites/local_default/e2e_roundtrip.rs"]
mod e2e_roundtrip;
#[path = "suites/local_default/e2e_verify_upstream.rs"]
mod e2e_verify_upstream;
#[path = "suites/local_default/e2e_version.rs"]
mod e2e_version;
