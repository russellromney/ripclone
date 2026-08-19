//! CLI and wire-contract integration tests.
//!
//! These cases share one bounded harness so Cargo links the Ripclone graph once
//! for this behavior family instead of once per source file.

#[path = "common/mod.rs"]
mod common;

#[path = "suites/local_cli/config_backends.rs"]
mod config_backends;
#[path = "suites/local_cli/e2e_auth.rs"]
mod e2e_auth;
#[path = "suites/local_cli/e2e_cli_contract.rs"]
mod e2e_cli_contract;
#[path = "suites/local_cli/e2e_clone_metrics.rs"]
mod e2e_clone_metrics;
#[path = "suites/local_cli/e2e_config_clone_mode.rs"]
mod e2e_config_clone_mode;
#[path = "suites/local_cli/e2e_config_provider_add_then_clone.rs"]
mod e2e_config_provider_add_then_clone;
#[path = "suites/local_cli/e2e_remote_helper.rs"]
mod e2e_remote_helper;
#[path = "suites/local_cli/e2e_roundtrip.rs"]
mod e2e_roundtrip;
#[path = "suites/local_cli/e2e_verify_upstream.rs"]
mod e2e_verify_upstream;
#[path = "suites/local_cli/e2e_version.rs"]
mod e2e_version;

#[path = "suites/contracts/docs_cli_surface.rs"]
mod docs_cli_surface;
#[path = "suites/contracts/e2e_config_sync_defaults.rs"]
mod e2e_config_sync_defaults;
#[path = "suites/contracts/e2e_login_logout.rs"]
mod e2e_login_logout;
#[path = "suites/contracts/e2e_provider_cli.rs"]
mod e2e_provider_cli;
#[path = "suites/contracts/history_pack_reuse_multipack.rs"]
mod history_pack_reuse_multipack;
#[path = "suites/contracts/lsm_incremental.rs"]
mod lsm_incremental;
#[path = "suites/contracts/manifest_tree_proptest.rs"]
mod manifest_tree_proptest;
#[path = "suites/contracts/ref_ordering.rs"]
mod ref_ordering;
