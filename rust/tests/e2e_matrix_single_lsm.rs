//! Full lifecycle battery for: single-phase publish, LSM incremental history
//! (tail + sealed levels reused by hash, compaction). Covers depth=1 / depth=0
//! / files across first sync, re-sync, and multi-commit growth.

mod common;
use common::*;

#[tokio::test]
async fn matrix_single_phase_lsm() {
    setup(false, true, false);
    let server = start_server().await;
    let origin = make_origin("acme", "m_single_lsm");
    lifecycle_battery(&server, &origin, false).await;
}
