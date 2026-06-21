//! Full lifecycle battery routed through the async build queue, over the
//! single-phase + LSM build. The worker runs the whole build inline (no
//! background phase 2), so depth=0/files are ready as soon as sync returns —
//! exercises the queue with a synchronous-completion build path.

mod common;
use common::*;

#[tokio::test]
async fn matrix_async_single_phase_lsm() {
    setup(false, true, true);
    let server = start_server().await;
    let origin = make_origin("acme", "m_async_single_lsm");
    lifecycle_battery(&server, &origin, false).await;
}
