//! Full lifecycle battery for separate Head, Full, and Files results with
//! non-LSM history. Covers first sync, re-sync, and multi-commit growth.

use crate::common;
use common::*;

#[tokio::test]
async fn matrix_exact_results_non_lsm() {
    setup(false);
    let server = start_server().await;
    let origin = make_origin("acme", "exact-results-full");
    lifecycle_battery(&server, &origin).await;
}
