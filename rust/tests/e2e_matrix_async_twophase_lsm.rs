//! Full lifecycle battery routed through the async build queue, over the
//! two-phase + LSM build (the full production config). Verifies the battery
//! passes when `/sync` enqueues onto the background worker (202/retry,
//! survives-disconnect path) rather than building inline.

mod common;
use common::*;

#[tokio::test]
async fn matrix_async_two_phase_lsm() {
    setup(true, true, true);
    let server = start_server().await;
    let origin = make_origin("acme", "m_async_tp_lsm");
    lifecycle_battery(&server, &origin, true).await;
}
