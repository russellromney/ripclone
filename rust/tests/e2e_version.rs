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
    server
        .client()
        .sync_repo("acme", "compat", None, None)
        .await
        .expect("a current-protocol client must pass the guard and sync");
}

/// Negative: a client advertising a protocol newer than the server understands
/// is rejected with 426 Upgrade Required (the guard runs before auth).
#[tokio::test]
async fn server_rejects_too_new_protocol_client() {
    init(false);
    let server = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/v1/repos/acme/x/refs/main", server.url))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "999")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 426);
}

/// Negative edge: a missing or unparseable protocol header is treated as a legacy
/// client and allowed through the guard (never 426), so the header can't lock out
/// older or misconfigured clients.
#[tokio::test]
async fn server_allows_missing_or_unparseable_protocol() {
    init(false);
    let server = start_server().await;
    let client = reqwest::Client::new();
    for header in [Some("not-a-number"), Some("99999999999999999999"), None] {
        let mut req = client
            .get(format!("{}/v1/repos/acme/x/refs/main", server.url))
            .header("Authorization", format!("Ripclone {}", token_hash()));
        if let Some(h) = header {
            req = req.header("x-ripclone-protocol", h);
        }
        let resp = req.send().await.unwrap();
        assert_ne!(
            resp.status().as_u16(),
            426,
            "protocol header {header:?} must not be rejected as too-new"
        );
    }
}
