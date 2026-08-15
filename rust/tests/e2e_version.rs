//! End-to-end coverage for version reporting and protocol enforcement against a
//! real in-process server (the user-facing surface of the version-reconciliation
//! work).

mod common;

use common::*;

/// Positive: `/v1/version` is served by a real server with no credentials and
/// reports this build's version + wire protocol.
#[tokio::test]
async fn version_endpoint_is_served_without_auth() {
    init(false);
    let server = start_server().await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v1/version", server.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["protocol"], ripclone::PROTOCOL_VERSION);
}

/// Positive: a real client sends `x-ripclone-protocol = PROTOCOL_VERSION`, so the
/// server's protocol guard must let a normal sync through. Guards against the
/// header accidentally breaking the authenticated path.
#[tokio::test]
async fn current_protocol_client_can_sync() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "compat");
    origin.commit(&[("a.txt", "hi\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/compat")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/compat", None)
        .await
        .expect("a current-protocol client must pass the guard and sync");
}

/// Any declared wire version other than the current one fails clearly instead
/// of selecting another implementation.
#[tokio::test]
async fn server_rejects_mismatched_or_invalid_protocol() {
    init(false);
    let server = start_server().await;
    let client = reqwest::Client::new();
    for header in ["1", "999", "not-a-number"] {
        let resp = client
            .get(format!("{}/v1/repos/acme/x/refs/main", server.url))
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .header("x-ripclone-protocol", header)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            426,
            "protocol header {header:?} must be rejected"
        );
        let body = resp.text().await.unwrap();
        assert!(body.contains("protocol"), "clear mismatch: {body}");
    }
}
