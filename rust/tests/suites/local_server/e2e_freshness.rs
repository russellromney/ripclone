//! A completed durable job does not probe the upstream ref or admit follow-up
//! work. A later webhook, poll, or user request is responsible for a later tip.

use crate::common::*;
use ripclone::server::{AdmissionTestProbe, install_admission_test_probe};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn completed_worker_does_not_probe_or_self_enqueue() {
    init(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    let _guard = install_admission_test_probe(probe.clone());
    let server = start_server().await;
    let origin = make_origin("acme", "no-self-enqueue");
    let admitted = origin.commit(&[("f", "admitted\n")], "admitted");
    origin.publish();
    register_added_without_build(&server, "acme/no-self-enqueue")
        .await
        .unwrap();

    let result = server
        .client()
        .sync_repo("acme/no-self-enqueue", None)
        .await
        .unwrap();
    assert_eq!(result.commit, admitted);
    let probes_after_completion = probe.tip_probes.load(Ordering::SeqCst);
    let enqueues_after_completion = probe.enqueue_attempts.load(Ordering::SeqCst);

    let later = origin.commit(&[("f", "later\n")], "later");
    origin.publish();
    tokio::time::sleep(Duration::from_millis(750)).await;

    assert_eq!(
        probe.tip_probes.load(Ordering::SeqCst),
        probes_after_completion,
        "completed worker performed a new upstream ref probe"
    );
    assert_eq!(
        probe.enqueue_attempts.load(Ordering::SeqCst),
        enqueues_after_completion,
        "completed worker admitted follow-up work"
    );
    assert_ne!(result.commit, later);
}
