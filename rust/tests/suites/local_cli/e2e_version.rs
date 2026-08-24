//! End-to-end coverage for version reporting and the sole current wire contract
//! against a real in-process server.

use crate::common::*;

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

/// Malformed and mismatched explicit wire declarations fail clearly. An absent
/// declaration is an unversioned caller of the same implementation.
#[tokio::test]
async fn server_rejects_explicit_wrong_protocol() {
    init(false);
    let server = start_server().await;
    let client = reqwest::Client::new();
    for header in ["1", "999", "not-a-number"] {
        let request = client
            .get(format!(
                "{}/v1/repos/github/acme/x/refs/main?result=full",
                server.url
            ))
            .header("Authorization", format!("Ripclone {}", token_hash()));
        let request = request.header("x-ripclone-protocol", header);
        let resp = request.send().await.unwrap();
        assert_eq!(
            resp.status().as_u16(),
            426,
            "protocol declaration {header} must be rejected"
        );
        let body = resp.text().await.unwrap();
        assert!(body.contains("protocol"), "clear mismatch: {body}");
    }
}

#[tokio::test]
async fn missing_protocol_header_uses_current_implementation() {
    init(false);
    let server = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/v1/repos/github/acme/x/status", server.url))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status().as_u16(), 426);
}
