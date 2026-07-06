//! End-to-end tests for the `/v1/repos/{provider}/{owner}/{repo}/status` billing endpoint.

mod common;

use common::*;

/// Helper: GET /v1/repos/{provider}/{owner}/{repo}/status with optional query params.
async fn get_status(
    server: &Server,
    owner: &str,
    repo: &str,
    query: Option<&str>,
) -> serde_json::Value {
    let mut url = format!("{}/v1/repos/github/{owner}/{repo}/status", server.url);
    if let Some(q) = query {
        url.push('?');
        url.push_str(q);
    }
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("status request")
        .error_for_status()
        .expect("status 2xx");
    resp.json().await.expect("status json")
}

fn prometheus_value(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (metric, value) = line.split_once(' ')?;
        (metric == name).then(|| value.parse().ok()).flatten()
    })
}

#[tokio::test]
async fn status_reports_zero_for_unsynced_repo() {
    init(false);
    let server = start_server().await;

    let status = get_status(&server, "acme", "nosync", None).await;
    assert_eq!(status["owner"], "acme");
    assert_eq!(status["repo"], "nosync");
    assert!(status["refs"].as_array().unwrap().is_empty());
    assert_eq!(status["total_bytes"], 0);
    assert_eq!(status["total_unique_bytes"], 0);
    assert!(!status["regions"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn status_reports_nonzero_bytes_after_sync() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "billing");
    origin.commit(&[("a.txt", "hello world\n")], "c1");
    origin.publish();

    // Wait for the full clonepack to publish (phase 2) so all artifacts are
    // accounted for in the byte totals.
    sync_until_manifest(&server, "acme", "billing").await;

    let status = get_status(&server, "acme", "billing", None).await;
    // The async build persists the ref under both the resolved branch (`main`)
    // and the literal `HEAD` alias (so any process can resolve `/sync HEAD` from
    // the shared metadata store), so two ref rows appear for the one commit.
    let refs = status["refs"].as_array().unwrap();
    assert_eq!(refs.len(), 2, "HEAD alias + resolved branch");
    let branch = refs
        .iter()
        .find(|r| r["branch"] == "main")
        .expect("resolved main ref present");
    assert!(branch["bytes"].as_u64().unwrap() > 0);
    assert_eq!(branch["bytes"], branch["unique_bytes"]);
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    // The HEAD alias and `main` share the same artifacts, so the repo total
    // dedups them.
    assert_eq!(status["total_bytes"], status["total_unique_bytes"]);
    assert!(status["regions"][0]["unique_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn sync_response_reports_phase_timings_and_status_reports_build_ms() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "synctiming");
    origin.commit(&[("README.md", "sync timings\n")], "c1");
    origin.publish();

    let client = reqwest::Client::new();
    let sync_url = format!("{}/v1/repos/github/acme/synctiming/sync", server.url);
    let sync_resp = client
        .post(&sync_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("sync request")
        .error_for_status()
        .expect("sync 2xx");
    let sync: ripclone::server::SyncResponse = sync_resp.json().await.expect("sync response json");
    assert_eq!(sync.status, "built");
    assert!(!sync.ref_info.commit.is_empty(), "sync response commit");
    assert!(
        sync.phases.mirror_fetch_ms.is_some(),
        "mirror fetch timing should be present"
    );
    assert!(
        sync.phases.publish_p1_ms.is_some(),
        "phase-1 publish timing should be present"
    );
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
    assert_eq!(
        prometheus_value(&metrics, "ripclone_sync_publish_p1_ms_total"),
        sync.phases.publish_p1_ms,
        "phase timings should feed /metrics without RIPCLONE_BENCH"
    );

    let mut build_ms = None;
    for _ in 0..80 {
        let status = get_status(&server, "acme", "synctiming", None).await;
        build_ms = status["refs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["branch"] == "main")
            .and_then(|entry| entry["build_ms"].as_u64());
        if build_ms.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(build_ms.is_some(), "status should report build_ms");
}

#[tokio::test]
async fn status_public_fork_is_free() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "forkbilling");
    origin.commit(&[("a.txt", "hello world\n")], "c1");
    origin.publish();

    let client = server.client();
    client
        .sync_repo("acme/forkbilling", None)
        .await
        .expect("sync");

    let status = get_status(
        &server,
        "acme",
        "forkbilling",
        Some("public=true&fork_of=upstream/repo"),
    )
    .await;
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    assert_eq!(status["total_unique_bytes"], 0);
    assert_eq!(status["refs"][0]["unique_bytes"], 0);
    assert_eq!(status["regions"][0]["unique_bytes"], 0);
}

#[tokio::test]
async fn status_shape_is_backwards_compatible() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "compat");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();

    let client = server.client();
    client.sync_repo("acme/compat", None).await.expect("sync");

    let status = get_status(&server, "acme", "compat", None).await;
    // Fields ripclone-cloud already parses must exist.
    assert!(status["refs"].is_array());
    assert!(status["refs"][0]["branch"].is_string());
    assert!(status["refs"][0]["commit"].is_string());
    assert!(status["refs"][0]["bytes"].is_u64());
    assert!(status["total_bytes"].is_u64());
    // New additive fields.
    assert!(status["refs"][0]["unique_bytes"].is_u64());
    assert!(status["total_unique_bytes"].is_u64());
    assert!(status["regions"].is_array());
    assert!(status["regions"][0]["region"].is_string());
    assert!(status["regions"][0]["unique_bytes"].is_u64());
}
