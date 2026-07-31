//! Local end-to-end cases that share the LSM-enabled server configuration.

#[path = "common/mod.rs"]
mod common;

#[path = "suites/local_lsm/e2e_forcepush_rewind.rs"]
mod e2e_forcepush_rewind;
#[path = "suites/local_lsm/e2e_lsm.rs"]
mod e2e_lsm;
#[path = "suites/local_lsm/e2e_matrix_twophase_lsm.rs"]
mod e2e_matrix_twophase_lsm;
#[path = "suites/local_lsm/e2e_sync_at_rev.rs"]
mod e2e_sync_at_rev;
#[path = "suites/local_lsm/e2e_webhook.rs"]
mod e2e_webhook;
