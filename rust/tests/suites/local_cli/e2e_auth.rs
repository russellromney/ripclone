//! End-to-end tests for the session-token (JWT) auth flow: the login page, the
//! `/v1/auth/login` exchange, using a `Bearer` token on a protected route,
//! `/v1/auth/refresh`, and the loopback/open-redirect handling.

use crate::common::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::StatusCode;
use reqwest::redirect::Policy;
use ripclone::server::ArtifactBarrier;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .build()
        .unwrap()
}

/// Read the `exp` claim from a JWT minted by the server.
fn token_exp(token: &str) -> u64 {
    let payload = token.split('.').nth(1).expect("JWT payload");
    let decoded = URL_SAFE_NO_PAD.decode(payload).expect("base64url payload");
    let claims: serde_json::Value = serde_json::from_slice(&decoded).expect("JWT JSON");
    claims["exp"].as_u64().expect("exp claim")
}

/// Mint a session token by posting the correct secret with a loopback callback
/// and reading it out of the redirect `Location`.
async fn mint_token(server: &Server) -> String {
    let resp = no_redirect_client()
        .post(format!("{}/v1/auth/login", server.url))
        .form(&[
            ("secret", TOKEN),
            ("callback", "http://127.0.0.1:0/"),
            ("state", "xyz"),
        ])
        .send()
        .await
        .expect("login request");
    assert_eq!(
        resp.status(),
        StatusCode::SEE_OTHER,
        "login redirects to callback"
    );
    let loc = resp
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .expect("location header")
        .to_string();
    assert!(loc.contains("state=xyz"), "callback echoes state: {loc}");
    let token = loc
        .split_once("token=")
        .and_then(|(_, rest)| rest.split('&').next())
        .expect("token in redirect")
        .to_string();
    assert!(!token.is_empty());
    token
}

#[tokio::test]
async fn login_page_is_served_unauthenticated() {
    init(false);
    let server = start_server().await;
    let resp = reqwest::get(format!("{}/login", server.url))
        .await
        .expect("login page");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Server token"), "renders the login form");
}

#[tokio::test]
async fn bearer_token_authorizes_a_protected_route() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "authrepo");
    origin.commit(&[("a.txt", "hi\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/authrepo")
        .await
        .expect("add authrepo");
    server
        .client()
        .sync_repo("acme/authrepo", None)
        .await
        .expect("sync");

    let token = mint_token(&server).await;
    let url = format!("{}/v1/repos/github/acme/authrepo/status", server.url);

    // Valid bearer → authorized (200).
    let ok = reqwest::Client::new()
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap();
    assert_eq!(
        ok.status(),
        StatusCode::OK,
        "valid session token is accepted"
    );

    // No credential → 401.
    let anon = reqwest::Client::new()
        .get(&url)
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), StatusCode::UNAUTHORIZED);

    // Garbage bearer → 401.
    let bad = reqwest::Client::new()
        .get(&url)
        .header("Authorization", "Bearer not.a.jwt")
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::UNAUTHORIZED);

    // The shared server token remains a current authentication mechanism.
    let shared_token = reqwest::Client::new()
        .get(&url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap();
    assert_eq!(shared_token.status(), StatusCode::OK);
}

#[tokio::test]
async fn wrong_secret_is_rejected() {
    init(false);
    let server = start_server().await;
    let resp = no_redirect_client()
        .post(format!("{}/v1/auth/login", server.url))
        .form(&[
            ("secret", "not-the-token"),
            ("callback", "http://127.0.0.1:0/"),
            ("state", "s"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn refresh_issues_a_fresh_token() {
    init(false);
    let server = start_server().await;
    let token = mint_token(&server).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/v1/auth/refresh", server.url))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let fresh = body["token"].as_str().expect("refreshed token");
    assert!(!fresh.is_empty());
    assert!(body["expires_in"].as_u64().unwrap() > 0);

    // Refresh without a valid bearer is rejected by the auth layer.
    let anon = reqwest::Client::new()
        .post(format!("{}/v1/auth/refresh", server.url))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn non_loopback_callback_is_refused() {
    init(false);
    let server = start_server().await;
    let resp = no_redirect_client()
        .post(format!("{}/v1/auth/login", server.url))
        .form(&[
            ("secret", TOKEN),
            ("callback", "http://evil.example/steal"),
            ("state", "s"),
        ])
        .send()
        .await
        .unwrap();
    // No redirect, no token leak — the secret was correct but the callback is
    // rejected as a non-loopback target.
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let loc = resp.headers().get("location");
    assert!(loc.is_none(), "must not redirect to a non-loopback host");

    // Userinfo bypass: a browser connects to evil.example, not 127.0.0.1.
    let bypass = no_redirect_client()
        .post(format!("{}/v1/auth/login", server.url))
        .form(&[
            ("secret", TOKEN),
            ("callback", "http://127.0.0.1:8080@evil.example/"),
            ("state", "s"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(bypass.status(), StatusCode::BAD_REQUEST);
    assert!(bypass.headers().get("location").is_none());
}

/// A bearer (session-token) client can complete an archive clone whose chunks
/// are fetched through the authenticated gateway path (no presigned URLs) — the
/// streaming extractor must send the session token, not re-derive a hash header.
#[tokio::test]
async fn bearer_client_clones_through_the_gateway() {
    init(false);
    // Exercise the archive-extraction path (gateway artifact fetch with auth).
    // SAFETY: only this test clones in this binary; the var is read per-clone.
    unsafe { std::env::set_var("RIPCLONE_EXTRACT_ARCHIVE", "1") };
    let server = start_server().await;
    let origin = make_origin("acme", "bearergw");
    origin.commit(&[("a.txt", "via gateway\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/bearergw")
        .await
        .expect("add bearergw");
    server
        .client()
        .sync_repo("acme/bearergw", None)
        .await
        .expect("sync");

    let token = mint_token(&server).await;
    let client = ripclone::client::Client::new_with_bearer(server.url.clone(), token)
        .with_provider("github");
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    client
        .install_repo_with_mode_at(
            "acme/bearergw",
            "HEAD",
            None,
            &target,
            ripclone::mode::CloneMode::Files,
            Some(ripclone::mode::clonepack_kind_for_depth(0)),
            None,
        )
        .await
        .expect("bearer clone via gateway artifact fetch");
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "via gateway\n"
    );
    unsafe { std::env::remove_var("RIPCLONE_EXTRACT_ARCHIVE") };
}

/// A bearer token that expires mid-clone must not leave a partial tree. We
/// deterministically stall the first gateway artifact response mid-body, sleep
/// past the JWT TTL, then close the connection. The client's retry re-issues
/// the request with the now-expired token and gets 401, so the clone fails
/// cleanly without materializing a partial tree.
#[tokio::test]
async fn expired_bearer_token_fails_clone_cleanly() {
    init(false);

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let barrier = ArtifactBarrier {
        after_bytes: 16,
        target: ripclone::server::BarrierTarget::FirstLargeBody,
        entered: Arc::new(std::sync::Mutex::new(Some(entered_tx))),
        proceed: Arc::new(std::sync::Mutex::new(Some(proceed_rx))),
        close_on_proceed: true,
        range_behavior: ripclone::server::ArtifactRangeBehavior::Normal,
        range_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
        artifact_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
        max_chunk_sent: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        consumed: Arc::new(AtomicBool::new(false)),
    };
    let server = start_server_with_barrier(barrier).await;
    let origin = make_origin("acme", "jwtexp");
    origin.commit(&[("a.txt", "x\n"), ("b.txt", "y\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/jwtexp")
        .await
        .expect("add jwtexp");
    server
        .client()
        .sync_repo("acme/jwtexp", None)
        .await
        .expect("sync");

    // Short-lived session tokens: the TTL is read at issuance, so it must be set
    // before minting the token and kept until the clone has finished.
    unsafe { std::env::set_var("RIPCLONE_JWT_TTL_SECS", "4") };
    let token = mint_token(&server).await;
    let token_expires_at = token_exp(&token);
    let client = ripclone::client::Client::new_with_bearer(server.url.clone(), token)
        .with_provider("github");

    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let clone_target = target.clone();
    let repo_path = "acme/jwtexp".to_string();
    let mut clone_task = tokio::spawn(async move {
        client
            .install_repo_with_mode_at(
                &repo_path,
                "HEAD",
                None,
                &clone_target,
                ripclone::mode::CloneMode::Files,
                Some("full"),
                None,
            )
            .await
    });

    // Wait until the server has sent the first bytes and is stalled mid-body,
    // then wait until the JWT has definitely expired before releasing the barrier.
    if !matches!(
        tokio::time::timeout(Duration::from_secs(20), entered_rx).await,
        Ok(Ok(()))
    ) {
        clone_task.abort();
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut clone_task).await;
        unsafe { std::env::remove_var("RIPCLONE_JWT_TTL_SECS") };
        panic!("clone did not reach the artifact barrier within 20 seconds");
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs();
    if token_expires_at > now {
        tokio::time::sleep(Duration::from_secs(token_expires_at - now + 1)).await;
    }
    proceed_tx.send(()).expect("release barrier");

    let res = match tokio::time::timeout(Duration::from_secs(20), &mut clone_task).await {
        Ok(joined) => joined.expect("clone task joined"),
        Err(_) => {
            clone_task.abort();
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut clone_task).await;
            unsafe { std::env::remove_var("RIPCLONE_JWT_TTL_SECS") };
            panic!("expired-bearer clone did not finish within 20 seconds");
        }
    };
    unsafe { std::env::remove_var("RIPCLONE_JWT_TTL_SECS") };

    assert!(
        res.is_err(),
        "clone with an expired bearer token must fail, got {res:?}"
    );
    let err = res.unwrap_err();
    let err_text = err.to_string();
    assert!(
        err_text.contains("401")
            || err_text.to_lowercase().contains("unauthorized")
            || err.chain().any(|e| {
                let s = e.to_string();
                s.contains("401") || s.to_lowercase().contains("unauthorized")
            }),
        "expected 401/unauthorized in error chain, got: {err:#}"
    );
    assert!(
        !target.exists(),
        "failed clone must not leave a partial tree at target"
    );
}

#[tokio::test]
async fn paste_mode_shows_the_token() {
    init(false);
    let server = start_server().await;
    // No callback → the page shows the token for copy-paste.
    let resp = reqwest::Client::new()
        .post(format!("{}/v1/auth/login", server.url))
        .form(&[("secret", TOKEN)])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Signed in"), "shows the success/token page");
    assert!(body.contains("eyJ"), "contains a JWT (starts with eyJ)");
}
