//! End-to-end coverage for version reporting and the sole current wire contract
//! against a real in-process server.

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

/// Missing, malformed, and mismatched wire declarations all fail clearly.
#[tokio::test]
async fn server_requires_the_current_protocol() {
    init(false);
    let server = start_server().await;
    let client = reqwest::Client::new();
    for header in [Some("1"), Some("999"), Some("not-a-number"), None] {
        let mut request = client
            .get(format!("{}/v1/repos/acme/x/refs/main", server.url))
            .header("Authorization", format!("Ripclone {}", token_hash()));
        if let Some(header) = header {
            request = request.header("x-ripclone-protocol", header);
        }
        let resp = request.send().await.unwrap();
        assert_eq!(
            resp.status().as_u16(),
            426,
            "protocol declaration {header:?} must be rejected"
        );
        let body = resp.text().await.unwrap();
        assert!(body.contains("protocol"), "clear mismatch: {body}");
    }
}
