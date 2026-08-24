//! End-to-end tests for Head sync latency instrumentation.
//!
//! Verifies that `/sync` returns per-stage timings for the Head build path
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

async fn head_metrics_after_builds(
    client: &reqwest::Client,
    server: &Server,
    expected_builds: u64,
) -> Vec<u64> {
    for _ in 0..160 {
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
        if prometheus_value(&metrics, "ripclone_builds_completed_total") >= expected_builds {
            return [
                "ripclone_sync_mirror_fetch_ms_total",
                "ripclone_sync_commit_graph_ms_total",
                "ripclone_sync_head_packs_ms_total",
                "ripclone_sync_skeleton_build_ms_total",
                "ripclone_sync_files_table_ms_total",
                "ripclone_sync_prebuilt_index_ms_total",
                "ripclone_sync_upload_head_ms_total",
                "ripclone_sync_ref_publish_ms_total",
                "ripclone_sync_publish_head_ms_total",
            ]
            .map(|name| prometheus_value(&metrics, name))
            .to_vec();
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("metrics never recorded {expected_builds} completed builds");
}

#[tokio::test]
async fn cold_sync_reports_all_head_timings() {
    init_bench();
    let server = start_server().await;
    let origin = make_origin("acme", "headtimingcold");
    let c1 = origin.commit(&[("README.md", "cold\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/headtimingcold")
        .await
        .expect("add repo");

    let client = reqwest::Client::new();
    let admission = server
        .client()
        .admit_sync_repo("acme/headtimingcold", None)
        .await
        .expect("admit cold sync");
    assert!(admission.accepted);
    assert_eq!(admission.commit, c1);
    let _ = sync_response_until_manifest(&client, &server, "acme", "headtimingcold", &c1).await;

    // Admission returns before timing data exists. The durable worker records
    // every Head stage after the accepted build settles; `/metrics` is the public
    // post-completion timing surface.
    let timings = head_metrics_after_builds(&client, &server, 1).await;
    assert_eq!(timings.len(), 9);
}

/// Poll the exact pinned metadata path until the full clonepack manifest is
/// published. This is the readiness wait used after an
/// accepted ordinary admission; it never repeats the moving `/sync` POST.
async fn sync_response_until_manifest(
    client: &reqwest::Client,
    server: &Server,
    owner: &str,
    repo: &str,
    commit: &str,
) -> ripclone::client::RefResponse {
    let url = format!(
        "{}/v1/repos/github/{owner}/{repo}/refs/main%23{commit}?result=full&pinned={commit}",
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
async fn incremental_sync_reports_all_head_timings() {
    init_bench();
    let server = start_server().await;
    let origin = make_origin("acme", "headtiminginc");
    let c1 = origin.commit(&[("README.md", "v1\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/headtiminginc")
        .await
        .expect("add repo");

    let client = reqwest::Client::new();
    // Cold sync.
    let cold = server
        .client()
        .admit_sync_repo("acme/headtiminginc", None)
        .await
        .expect("admit cold sync");
    assert!(cold.accepted);
    assert_eq!(cold.commit, c1);

    // Let the background full-history build finish so the next sync's storage
    // amplification report includes history packs.
    let _ = sync_response_until_manifest(&client, &server, "acme", "headtiminginc", &c1).await;
    let cold_timings = head_metrics_after_builds(&client, &server, 1).await;

    // Incremental sync: add a commit and re-sync.
    let c2 = origin.commit(&[("README.md", "v2\n")], "c2");
    origin.publish();
    let inc = server
        .client()
        .admit_sync_repo("acme/headtiminginc", None)
        .await
        .expect("admit incremental sync");
    assert!(inc.accepted);
    assert_eq!(inc.commit, c2);
    let _ = sync_response_until_manifest(&client, &server, "acme", "headtiminginc", &c2).await;
    let incremental_timings = head_metrics_after_builds(&client, &server, 2).await;
    assert_eq!(incremental_timings.len(), cold_timings.len());
    // The incremental push→clonable path should remain in the same ballpark as
    // the cold path on this tiny fixture; the real tripwire is measured on
    // larger repos.
    let incremental_publish_head = incremental_timings[8].saturating_sub(cold_timings[8]);
    assert!(
        incremental_publish_head < 5000,
        "incremental push→clonable must stay under the ~5s tripwire on small repos"
    );
}
