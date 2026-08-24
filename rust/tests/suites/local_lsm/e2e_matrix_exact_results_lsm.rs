//! Full lifecycle battery for separate Head, Full, and Files results with LSM
//! incremental history. Covers first sync, re-sync, and multi-commit growth.

use crate::common;
use common::*;

#[tokio::test]
async fn matrix_exact_results_lsm() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "exact-results-lsm");
    lifecycle_battery(&server, &origin).await;
}
