//! Local server, storage, and publication integration tests.

#[path = "common/mod.rs"]
mod common;

#[path = "suites/local_server/e2e_equivalence.rs"]
mod lsm_off_e2e_equivalence;
#[path = "suites/local_server/e2e_freshness.rs"]
mod lsm_off_e2e_freshness;
#[path = "suites/local_server/e2e_gc_race.rs"]
mod lsm_off_e2e_gc_race;
#[path = "suites/local_server/e2e_sync_phases.rs"]
mod lsm_off_e2e_sync_phases;
#[path = "suites/local_server/e2e_two_phase.rs"]
mod lsm_off_e2e_two_phase;
#[path = "suites/local_server/archive_bounded.rs"]
mod lsm_on_archive_bounded;
#[path = "suites/local_server/e2e_compaction.rs"]
mod lsm_on_e2e_compaction;
#[path = "suites/local_server/e2e_concurrent_fd_budget.rs"]
mod lsm_on_e2e_concurrent_fd_budget;
#[path = "suites/local_server/e2e_concurrent_same_repo.rs"]
mod lsm_on_e2e_concurrent_same_repo;
#[path = "suites/local_server/e2e_forcepush_rewind.rs"]
mod lsm_on_e2e_forcepush_rewind;
#[path = "suites/local_server/e2e_full_idx_bundle_race.rs"]
mod lsm_on_e2e_full_idx_bundle_race;
#[path = "suites/local_server/e2e_sync_at_rev.rs"]
mod lsm_on_e2e_sync_at_rev;
#[path = "suites/local_server/head_delta.rs"]
mod lsm_on_head_delta;
#[path = "suites/local_server/two_phase_decouple.rs"]
mod lsm_on_two_phase_decouple;
