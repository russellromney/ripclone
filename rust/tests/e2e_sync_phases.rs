//! End-to-end tests for phase-1 sync latency instrumentation.
//!
//! Verifies that `/sync` returns per-stage timings for the phase-1 build path
//! (mirror fetch, HEAD packs, skeleton, files table, prebuilt index, upload,
//! ref publish) and that the `RIPCLONE_BENCH` report path does not panic.

mod common;

use common::*;

fn init_bench() {
    // SAFETY: set once before any server/sync reads the variable.
    unsafe { std::env::set_var("RIPCLONE_BENCH", "1") };
    init(false);
}

fn assert_all_phases_present(phases: &ripclone::server::SyncPhases, label: &str) {
    assert!(
        phases.mirror_fetch_ms.is_some(),
        "{label}: mirror_fetch_ms missing"
    );
    assert!(
        phases.commit_graph_ms.is_some(),
        "{label}: commit_graph_ms missing"
    );
    assert!(
        phases.head_packs_ms.is_some(),
        "{label}: head_packs_ms missing"
    );
    assert!(
        phases.skeleton_build_ms.is_some(),
        "{label}: skeleton_build_ms missing"
    );
    assert!(
        phases.files_table_ms.is_some(),
        "{label}: files_table_ms missing"
    );
    assert!(
        phases.prebuilt_index_ms.is_some(),
        "{label}: prebuilt_index_ms missing"
    );
    assert!(
        phases.upload_p1_ms.is_some(),
        "{label}: upload_p1_ms missing"
    );
    assert!(
        phases.ref_publish_ms.is_some(),
        "{label}: ref_publish_ms missing"
    );
    assert!(
        phases.publish_p1_ms.is_some(),
        "{label}: publish_p1_ms missing"
    );
}

#[tokio::test]
async fn cold_sync_reports_all_phase_timings() {
    init_bench();
    let server = start_server().await;
    let origin = make_origin("acme", "phasescold");
    origin.commit(&[("README.md", "cold\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/phasescold")
        .await
        .expect("add repo");

    let client = reqwest::Client::new();
    let sync_url = format!("{}/v1/repos/github/acme/phasescold/sync", server.url);
    let sync: ripclone::server::SyncResponse = client
        .post(&sync_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("sync request")
        .error_for_status()
        .expect("sync 2xx")
        .json()
        .await
        .expect("sync json");
    assert_eq!(sync.status, "built");
    assert_all_phases_present(&sync.phases, "cold");
}

/// Poll `/sync` until the full clonepack manifest is published (phase 2 done),
/// returning the full `SyncResponse` so callers can inspect phase timings.
async fn sync_response_until_manifest(
    client: &reqwest::Client,
    server: &Server,
    owner: &str,
    repo: &str,
) -> ripclone::server::SyncResponse {
    let url = format!("{}/v1/repos/github/{owner}/{repo}/sync", server.url);
    for _ in 0..160 {
        let resp: ripclone::server::SyncResponse = client
            .post(&url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .send()
            .await
            .expect("sync request")
            .error_for_status()
            .expect("sync 2xx")
            .json()
            .await
            .expect("sync json");
        if !resp.ref_info.clonepack_manifest.is_empty() {
            return resp;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("clonepack manifest never published for {owner}/{repo}");
}

#[tokio::test]
async fn incremental_sync_reports_all_phase_timings() {
    init_bench();
    let server = start_server().await;
    let origin = make_origin("acme", "phasesinc");
    origin.commit(&[("README.md", "v1\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/phasesinc")
        .await
        .expect("add repo");

    let client = reqwest::Client::new();
    let sync_url = format!("{}/v1/repos/github/acme/phasesinc/sync", server.url);

    // Cold sync.
    let cold: ripclone::server::SyncResponse = client
        .post(&sync_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("sync request")
        .error_for_status()
        .expect("sync 2xx")
        .json()
        .await
        .expect("sync json");
    assert_eq!(cold.status, "built");
    assert_all_phases_present(&cold.phases, "cold");

    // Let the background full-history build finish so the next sync's storage
    // amplification report includes history packs.
    let _ = sync_response_until_manifest(&client, &server, "acme", "phasesinc").await;

    // Incremental sync: add a commit and re-sync.
    origin.commit(&[("README.md", "v2\n")], "c2");
    origin.publish();
    let inc: ripclone::server::SyncResponse = client
        .post(&sync_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("sync request")
        .error_for_status()
        .expect("sync 2xx")
        .json()
        .await
        .expect("sync json");
    assert_eq!(inc.status, "built");
    assert_all_phases_present(&inc.phases, "incremental");
    // The incremental push→clonable path should remain in the same ballpark as
    // the cold path on this tiny fixture; the real tripwire is measured on
    // larger repos.
    assert!(
        inc.phases.publish_p1_ms.unwrap_or(u64::MAX) < 5000,
        "incremental push→clonable must stay under the ~5s tripwire on small repos"
    );
}
