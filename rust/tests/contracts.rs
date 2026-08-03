//! Integration contracts that do not share mutable server configuration.

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
