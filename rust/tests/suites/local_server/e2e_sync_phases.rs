//! End-to-end tests for phase-1 sync latency instrumentation.
//!
//! Verifies that `/sync` returns per-stage timings for the phase-1 build path
//! (mirror fetch, HEAD packs, skeleton, files table, prebuilt index, upload,
//! ref publish) and that the `RIPCLONE_BENCH` report path does not panic.

use crate::common::*;

fn init_bench() {
    // SAFETY: set once before any server/sync reads the variable.
    unsafe { std::env::set_var("RIPCLONE_BENCH", "1") };
    init(false);
}

fn prometheus_value(text: &str, name: &str) -> u64 {
    text.lines()
        .find_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name).then(|| value.parse().ok()).flatten()
        })
        .unwrap_or_else(|| panic!("metric {name} missing"))
}

async fn phase_metrics(client: &reqwest::Client, server: &Server) -> Vec<u64> {
    let metrics = client
        .get(format!("{}/metrics", server.url))
        .send()
        .await
        .expect("metrics request")
        .error_for_status()
        .expect("metrics 2xx")
        .text()
        .await
        .expect("metrics text");
    [
        "ripclone_sync_mirror_fetch_ms_total",
        "ripclone_sync_commit_graph_ms_total",
        "ripclone_sync_head_packs_ms_total",
        "ripclone_sync_skeleton_build_ms_total",
        "ripclone_sync_files_table_ms_total",
        "ripclone_sync_prebuilt_index_ms_total",
        "ripclone_sync_upload_p1_ms_total",
        "ripclone_sync_ref_publish_ms_total",
        "ripclone_sync_publish_p1_ms_total",
    ]
    .map(|name| prometheus_value(&metrics, name))
    .to_vec()
}

#[tokio::test]
async fn cold_sync_reports_all_phase_timings() {
    init_bench();
    let server = start_server().await;
    let origin = make_origin("acme", "phasescold");
    let c1 = origin.commit(&[("README.md", "cold\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/phasescold")
        .await
        .expect("add repo");

    let client = reqwest::Client::new();
    let admission = server
        .client()
        .admit_sync_repo("acme/phasescold", None)
        .await
        .expect("admit cold sync");
    assert!(admission.accepted);
    assert_eq!(admission.commit, c1);
    let _ = sync_response_until_manifest(&client, &server, "acme", "phasescold", &c1).await;

    // Admission returns before timing data exists. The durable worker records
    // every phase after the accepted build settles; `/metrics` is the public
    // post-completion timing surface.
    let phases = phase_metrics(&client, &server).await;
    assert_eq!(phases.len(), 9);
}

/// Poll the exact pinned metadata path until the full clonepack manifest is
/// published (phase 2 done). This is the readiness wait used after an
/// accepted ordinary admission; it never repeats the moving `/sync` POST.
async fn sync_response_until_manifest(
    client: &reqwest::Client,
    server: &Server,
    owner: &str,
    repo: &str,
    commit: &str,
) -> ripclone::client::RefResponse {
    let url = format!(
        "{}/v1/repos/github/{owner}/{repo}/refs/main%23{commit}?clonepack=full&pinned={commit}",
        server.url
    );
    for _ in 0..160 {
        let response = client
            .get(&url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .header(
                "x-ripclone-protocol",
                ripclone::PROTOCOL_VERSION.to_string(),
            )
            .send()
            .await
            .expect("pinned ref request");
        if response.status() == reqwest::StatusCode::OK {
            let resp: ripclone::client::RefResponse = response.json().await.expect("ref json");
            if !resp.clonepack_manifest.is_empty() {
                return resp;
            }
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
    let c1 = origin.commit(&[("README.md", "v1\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/phasesinc")
        .await
        .expect("add repo");

    let client = reqwest::Client::new();
    // Cold sync.
    let cold = server
        .client()
        .admit_sync_repo("acme/phasesinc", None)
        .await
        .expect("admit cold sync");
    assert!(cold.accepted);
    assert_eq!(cold.commit, c1);

    // Let the background full-history build finish so the next sync's storage
    // amplification report includes history packs.
    let _ = sync_response_until_manifest(&client, &server, "acme", "phasesinc", &c1).await;
    let cold_phases = phase_metrics(&client, &server).await;

    // Incremental sync: add a commit and re-sync.
    let c2 = origin.commit(&[("README.md", "v2\n")], "c2");
    origin.publish();
    let inc = server
        .client()
        .admit_sync_repo("acme/phasesinc", None)
        .await
        .expect("admit incremental sync");
    assert!(inc.accepted);
    assert_eq!(inc.commit, c2);
    let _ = sync_response_until_manifest(&client, &server, "acme", "phasesinc", &c2).await;
    let incremental_phases = phase_metrics(&client, &server).await;
    assert_eq!(incremental_phases.len(), cold_phases.len());
    // The incremental push→clonable path should remain in the same ballpark as
    // the cold path on this tiny fixture; the real tripwire is measured on
    // larger repos.
    let incremental_publish_p1 = incremental_phases[8].saturating_sub(cold_phases[8]);
    assert!(
        incremental_publish_p1 < 5000,
        "incremental push→clonable must stay under the ~5s tripwire on small repos"
    );
}
