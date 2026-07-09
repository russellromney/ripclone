//! Full lifecycle battery for: two-phase publish + LSM incremental history —
//! the production default path (depth=1 fast, full history reused from storage
//! in the background, archive deferred to phase 2). Covers depth=1 / depth=0 /
//! files across first sync, re-sync, and multi-commit growth.

use crate::common::*;

#[tokio::test]
async fn matrix_two_phase_lsm() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "m_tp_lsm");
    lifecycle_battery(&server, &origin).await;
}
