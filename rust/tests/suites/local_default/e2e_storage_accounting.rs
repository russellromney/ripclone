//! End-to-end tests for the `/v1/repos/{provider}/{owner}/{repo}/status` endpoint
//! (repo sync status + byte-usage accounting).

use crate::common;

use common::*;
use prost::Message;
use ripclone::clonepack::{ChunkRef, ClonepackManifest, hash_from_hex};
use ripclone::provider::RepoId;

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
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
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
    let origin = make_origin("acme", "storage-accounting");
    let commit = origin.commit(&[("a.txt", "hello world\n")], "c1");
    origin.publish();

    // Wait for Full to publish so all artifacts are
    // accounted for in the byte totals.
    sync_until_full_ready(&server, "acme", "storage-accounting").await;

    let status = get_status(&server, "acme", "storage-accounting", None).await;
    let refs = status["refs"].as_array().unwrap();
    assert_eq!(refs.len(), 1, "one exact result for the synced commit");
    let result = &refs[0];
    assert_eq!(result["commit"], commit);
    assert!(result.get("branch").is_none());
    assert!(result["bytes"].as_u64().unwrap() > 0);
    assert_eq!(result["bytes"], result["unique_bytes"]);
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    assert_eq!(status["total_bytes"], status["total_unique_bytes"]);
    assert!(status["regions"][0]["unique_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn status_includes_retained_historical_artifacts_in_deduplicated_union() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "historical-storage-accounting");
    let current = origin.commit(&[("a.txt", "shared artifact bytes\n")], "c1");
    origin.publish();
    sync_until_full_ready(&server, "acme", "historical-storage-accounting").await;

    let before = get_status(&server, "acme", "historical-storage-accounting", None).await;
    let store = server_ref_store(&server).await;
    let repo_id = RepoId::github("acme/historical-storage-accounting");
    let info = store
        .load_result(&repo_id, &current)
        .await
        .expect("load exact result")
        .expect("ordinary sync publishes an exact result");
    let storage = ripclone::storage::local(&server.storage_dir).expect("open local storage");
    let moving_manifest_bytes = storage
        .get(&info.full.as_ref().expect("Full result").clonepack.manifest)
        .expect("read moving full manifest");
    let mut historical_manifest = ClonepackManifest::decode(moving_manifest_bytes.as_slice())
        .expect("decode moving full manifest");
    assert!(
        historical_manifest.metadata_chunk.is_some()
            || !historical_manifest.archive_chunks.is_empty()
            || !historical_manifest.head_blobs_chunks.is_empty()
            || !historical_manifest.packs.is_empty(),
        "historical fixture must retain at least one shared chunk"
    );
    let historical_only_bytes = b"historical-only-artifact-bytes".repeat(97);
    let historical_only_hash = ripclone::cas::hash(&historical_only_bytes);
    storage
        .put(&historical_only_hash, &historical_only_bytes)
        .expect("store historical-only chunk");
    historical_manifest.archive_chunks.push(ChunkRef {
        hash: hash_from_hex(&historical_only_hash).expect("decode historical-only hash"),
        len: historical_only_bytes.len() as u64,
    });
    let historical_commit = "f".repeat(40);
    historical_manifest.commit = historical_commit.clone();
    let historical_manifest_bytes = historical_manifest.encode_to_vec();
    let historical_manifest_hash = ripclone::cas::hash(&historical_manifest_bytes);
    storage
        .put(&historical_manifest_hash, &historical_manifest_bytes)
        .expect("store historical-only manifest");

    let mut historical_info = info;
    historical_info.commit = historical_commit.clone();
    let historical_full = historical_info.full.as_mut().expect("Full result");
    historical_full.clonepack.commit = historical_commit.clone();
    historical_full.clonepack.manifest = historical_manifest_hash.clone();
    store
        .save_result(&repo_id, &historical_info)
        .await
        .expect("seed retained historical row");

    let after = get_status(&server, "acme", "historical-storage-accounting", None).await;
    let refs = after["refs"].as_array().expect("status refs");
    assert_eq!(refs.len(), 2);
    assert!(refs.iter().any(|entry| entry["commit"] == current));
    assert!(
        refs.iter()
            .any(|entry| entry["commit"] == historical_commit)
    );
    assert!(refs.iter().all(|entry| entry.get("branch").is_none()));
    let expected_delta =
        historical_manifest_bytes.len() as u64 + historical_only_bytes.len() as u64;
    let expected_total = before["total_bytes"].as_u64().unwrap() + expected_delta;
    assert_eq!(
        after["total_bytes"].as_u64().unwrap(),
        expected_total,
        "exact result artifacts count in the deduplicated union while shared hashes count once"
    );
    assert_eq!(
        after["total_unique_bytes"].as_u64().unwrap(),
        expected_total
    );
    assert_eq!(
        after["regions"][0]["unique_bytes"].as_u64().unwrap(),
        expected_total,
        "regional accounting uses the same deduplicated reachable-hash union"
    );
}

#[tokio::test]
async fn accepted_sync_reports_build_timings_through_metrics_and_status() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "synctiming");
    origin.commit(&[("README.md", "sync timings\n")], "c1");
    origin.publish();

    server
        .client()
        .add_repo("acme/synctiming")
        .await
        .expect("add synctiming");
    let c2 = origin.commit(&[("README.md", "sync timings\nupdated\n")], "c2");
    origin.publish();
    let client = reqwest::Client::new();
    let before_metrics = client
        .get(format!("{}/metrics", server.url))
        .send()
        .await
        .expect("metrics request")
        .error_for_status()
        .expect("metrics 2xx")
        .text()
        .await
        .expect("metrics text");
    let before_publish_head =
        prometheus_value(&before_metrics, "ripclone_sync_publish_head_ms_total").unwrap_or(0);
    // Admission returns before build timing data exists. The completed build
    // publishes those timings through metrics and status instead.
    let sync_url = format!(
        "{}/v1/repos/github/acme/synctiming/sync?rev={c2}",
        server.url
    );
    let sync_resp = client
        .post(&sync_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("sync response");
    assert_eq!(sync_resp.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: serde_json::Value = sync_resp.json().await.expect("accepted response json");
    assert_eq!(accepted["commit"], c2);

    sync_until_full_ready(&server, "acme", "synctiming").await;
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
    let after_publish_head = prometheus_value(&metrics, "ripclone_sync_publish_head_ms_total")
        .expect("Head publish metric present");
    assert!(
        after_publish_head > before_publish_head,
        "completed Head timing should feed /metrics without RIPCLONE_BENCH"
    );

    let status = get_status(&server, "acme", "synctiming", None).await;
    let exact = status["refs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["commit"] == c2)
        .expect("status reports exact commit");
    assert_eq!(exact["head"], true);
    assert_eq!(exact["full"], true);
    assert_eq!(exact["files"], true);
}

#[tokio::test]
async fn status_public_fork_has_zero_unique_byte_allocation() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "fork-storage-accounting");
    origin.commit(&[("a.txt", "hello world\n")], "c1");
    origin.publish();

    let client = server.client();
    client
        .add_repo("acme/fork-storage-accounting")
        .await
        .expect("add fork storage-accounting fixture");
    client
        .sync_repo("acme/fork-storage-accounting", None)
        .await
        .expect("sync");

    let status = get_status(
        &server,
        "acme",
        "fork-storage-accounting",
        Some("public=true&fork_of=upstream/repo"),
    )
    .await;
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    assert_eq!(status["total_unique_bytes"], 0);
    assert_eq!(status["refs"][0]["unique_bytes"], 0);
    assert_eq!(status["regions"][0]["unique_bytes"], 0);
}

#[tokio::test]
async fn status_shape_reports_exact_results_and_storage() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "compat");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();

    let client = server.client();
    client.add_repo("acme/compat").await.expect("add compat");
    client.sync_repo("acme/compat", None).await.expect("sync");

    let status = get_status(&server, "acme", "compat", None).await;
    // Exact-result and storage-accounting fields must exist without a checkout name.
    assert!(status["refs"].is_array());
    assert!(status["refs"][0].get("branch").is_none());
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
