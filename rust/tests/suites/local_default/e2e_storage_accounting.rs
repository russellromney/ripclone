//! End-to-end tests for the `/v1/repos/{provider}/{owner}/{repo}/status` endpoint
//! (repo sync status + byte-usage accounting).

use crate::common;

use common::*;
use prost::Message;
use ripclone::clonepack::{ChunkRef, ClonepackManifest, hash_from_hex};
use ripclone::provider::RepoId;
use ripclone::ref_store::{FileRefStore, RefStore};

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

    // Wait for the full clonepack to publish (phase 2) so all artifacts are
    // accounted for in the byte totals.
    sync_until_manifest(&server, "acme", "storage-accounting").await;

    let status = get_status(&server, "acme", "storage-accounting", None).await;
    // Ordinary publication persists only the resolved moving branch (`main`)
    // and the literal `HEAD` selector used by other processes. Immutable job
    // identity must not become a public synthetic branch.
    let refs = status["refs"].as_array().unwrap();
    assert_eq!(refs.len(), 2, "HEAD plus the moving source branch");
    let branch = refs
        .iter()
        .find(|r| r["branch"] == "main")
        .expect("resolved main ref present");
    assert!(
        refs.iter()
            .all(|r| r["branch"] != ripclone::ref_store::exact_ref_key("main", &commit)),
        "internal exact result must not appear in public status"
    );
    assert!(branch["bytes"].as_u64().unwrap() > 0);
    assert_eq!(branch["bytes"], branch["unique_bytes"]);
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    // HEAD and main share the same artifacts, so the repo total dedups them.
    assert_eq!(status["total_bytes"], status["total_unique_bytes"]);
    assert!(status["regions"][0]["unique_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn status_includes_retained_historical_artifacts_in_deduplicated_union() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "historical-storage-accounting");
    origin.commit(&[("a.txt", "shared artifact bytes\n")], "c1");
    origin.publish();
    sync_until_manifest(&server, "acme", "historical-storage-accounting").await;

    let before = get_status(&server, "acme", "historical-storage-accounting", None).await;
    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/historical-storage-accounting");
    let info = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load moving main")
        .expect("moving main exists");
    let historical_key = ripclone::ref_store::exact_ref_key("main", &info.commit);
    let ordinary_exact = store
        .load_branch(&repo_id, &historical_key)
        .await
        .expect("load ordinary exact row")
        .expect("ordinary sync publishes an exact result");
    assert!(ordinary_exact.internal_exact_result);
    assert_eq!(ordinary_exact.commit, info.commit);
    let storage = ripclone::storage::local(&server.storage_dir).expect("open local storage");
    let moving_manifest_bytes = storage
        .get(&info.full_clonepack.manifest)
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
    let historical_manifest_bytes = historical_manifest.encode_to_vec();
    let historical_manifest_hash = ripclone::cas::hash(&historical_manifest_bytes);
    storage
        .put(&historical_manifest_hash, &historical_manifest_bytes)
        .expect("store historical-only manifest");

    let mut historical_info = info.clone();
    historical_info.internal_exact_result = true;
    historical_info.full_clonepack.manifest = historical_manifest_hash.clone();
    store
        .save_branch(&repo_id, &historical_key, &historical_info)
        .await
        .expect("seed retained historical row");

    let after = get_status(&server, "acme", "historical-storage-accounting", None).await;
    let refs = after["refs"].as_array().expect("status refs");
    assert!(refs.iter().any(|entry| entry["branch"] == "main"));
    assert!(
        refs.iter().all(|entry| entry["branch"] != historical_key),
        "internal exact rows must not appear as source refs: {refs:?}"
    );
    let expected_delta =
        historical_manifest_bytes.len() as u64 + historical_only_bytes.len() as u64;
    let expected_total = before["total_bytes"].as_u64().unwrap() + expected_delta;
    assert_eq!(
        after["total_bytes"].as_u64().unwrap(),
        expected_total,
        "internal exact artifacts count in the deduplicated union while shared hashes count once"
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
async fn sync_response_reports_phase_timings_and_status_reports_build_ms() {
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
    let before_publish_p1 =
        prometheus_value(&before_metrics, "ripclone_sync_publish_p1_ms_total").unwrap_or(0);
    // Ordinary `/sync` now returns exact admission before build timing data
    // exists. Exercise the unchanged explicit-revision ready payload for this
    // phase-timing/metrics regression.
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
    let after_publish_p1 = prometheus_value(&metrics, "ripclone_sync_publish_p1_ms_total")
        .expect("publish p1 metric present");
    assert_eq!(
        after_publish_p1 - before_publish_p1,
        sync.phases.publish_p1_ms.unwrap_or(0),
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
async fn status_shape_reports_current_source_refs_and_storage() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "compat");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();

    let client = server.client();
    client.add_repo("acme/compat").await.expect("add compat");
    client.sync_repo("acme/compat", None).await.expect("sync");

    let status = get_status(&server, "acme", "compat", None).await;
    // Fields downstream consumers of the status endpoint parse must exist.
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
