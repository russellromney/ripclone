//! Full lifecycle battery for: single-phase publish, NON-LSM history (full
//! rebuild each sync). Covers depth=1 / depth=0 / files across first sync,
//! re-sync, and multi-commit growth.

mod common;
use common::*;

#[tokio::test]
async fn matrix_single_phase_non_lsm() {
    setup(false, false, false);
    let server = start_server().await;
    let origin = make_origin("acme", "m_single_full");
    lifecycle_battery(&server, &origin, false).await;
}
