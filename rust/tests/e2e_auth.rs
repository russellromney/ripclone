//! End-to-end tests for the session-token (JWT) auth flow: the login page, the
//! `/v1/auth/login` exchange, using a `Bearer` token on a protected route,
//! `/v1/auth/refresh`, and the loopback/open-redirect handling.

mod common;

use common::*;
use reqwest::StatusCode;
use reqwest::redirect::Policy;

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .build()
        .unwrap()
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
        .sync_repo("acme/authrepo", None)
        .await
        .expect("sync");

    let token = mint_token(&server).await;
    let url = format!("{}/v1/repos/github/acme/authrepo/status", server.url);

    // Valid bearer → authorized (200).
    let ok = reqwest::Client::new()
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        ok.status(),
        StatusCode::OK,
        "valid session token is accepted"
    );

    // No credential → 401.
    let anon = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(anon.status(), StatusCode::UNAUTHORIZED);

    // Garbage bearer → 401.
    let bad = reqwest::Client::new()
        .get(&url)
        .header("Authorization", "Bearer not.a.jwt")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::UNAUTHORIZED);

    // The existing shared-token scheme still works (backward compatible).
    let legacy = reqwest::Client::new()
        .get(&url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .unwrap();
    assert_eq!(legacy.status(), StatusCode::OK);
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
