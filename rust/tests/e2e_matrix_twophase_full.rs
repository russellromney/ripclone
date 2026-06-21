//! Full lifecycle battery for: two-phase publish (depth=1 foreground, full in
//! background), NON-LSM history. Covers depth=1 / depth=0 / files across first
//! sync, re-sync, and multi-commit growth, with background-build polling.

mod common;
use common::*;

#[tokio::test]
async fn matrix_two_phase_non_lsm() {
    setup(true, false, false);
    let server = start_server().await;
    let origin = make_origin("acme", "m_tp_full");
    lifecycle_battery(&server, &origin, true).await;
}
