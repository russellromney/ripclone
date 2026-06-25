//! End-to-end test for `ripclone login` and `ripclone logout`.
//!
//! Spawns a minimal device-flow mock server, runs the CLI as a subprocess with
//! an isolated $HOME, and verifies that the server URL lands in
//! `~/.config/ripclone/config.toml` while the token lands in the secure token
//! store (keyring → file fallback).

use axum::{Json, Router, routing::post};
use ripclone::auth::token_store::{FileTokenStore, TokenStore};
use serde::{Deserialize, Serialize};
use std::process::Command;

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

#[derive(Serialize)]
struct DeviceStart {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    interval: u64,
    expires_in: u64,
}

#[derive(Deserialize, Serialize)]
struct DevicePoll {
    status: String,
    token: Option<String>,
}

async fn device_start() -> Json<DeviceStart> {
    Json(DeviceStart {
        device_code: "dc_test".into(),
        user_code: "uc_test".into(),
        verification_uri: "http://localhost/verify".into(),
        verification_uri_complete: "http://localhost/verify?code=uc_test".into(),
        interval: 1,
        expires_in: 60,
    })
}

async fn device_token() -> Json<DevicePoll> {
    Json(DevicePoll {
        status: "approved".into(),
        token: Some("rc_test_token_123".into()),
    })
}

fn token_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".config").join("ripclone").join("tokens.json")
}

fn config_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".config").join("ripclone").join("config.toml")
}

#[tokio::test]
async fn login_saves_url_in_config_and_token_in_store() {
    let app = Router::new()
        .route("/cli/device", post(device_start))
        .route("/cli/device/token", post(device_token));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let home = tempfile::tempdir().unwrap();
    let url = format!("http://{addr}");

    let home_path = home.path().to_path_buf();
    let bin = ripclone_bin();
    let url_for_login = url.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(&bin)
            .arg("login")
            .env("HOME", &home_path)
            .env("RIPCLONE_SERVER", &url_for_login)
            .env("RIPCLONE_NO_BROWSER", "1")
            .env("RIPCLONE_TOKEN_STORE", "file")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn ripclone login")
    })
    .await
    .expect("login subprocess panicked");

    assert!(
        output.status.success(),
        "login failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let config_text = std::fs::read_to_string(config_path(home.path())).unwrap();
    assert!(
        config_text.contains(&url),
        "config.toml should contain the server URL: {config_text}"
    );
    assert!(
        !config_text.contains("rc_test_token_123"),
        "config.toml must not contain the token: {config_text}"
    );

    let store = FileTokenStore::new(token_path(home.path()));
    assert_eq!(
        store.get("server").unwrap().as_deref(),
        Some("rc_test_token_123"),
        "server token should be in the token store"
    );

    // Logout should remove the token but leave the server URL in config.
    let home_path = home.path().to_path_buf();
    let bin = ripclone_bin();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(&bin)
            .arg("logout")
            .env("HOME", &home_path)
            .env("RIPCLONE_TOKEN_STORE", "file")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn ripclone logout")
    })
    .await
    .expect("logout subprocess panicked");

    assert!(
        output.status.success(),
        "logout failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        store.get("server").unwrap().is_none(),
        "server token should be removed after logout"
    );
    let config_text = std::fs::read_to_string(config_path(home.path())).unwrap();
    assert!(
        config_text.contains(&url),
        "config.toml should still contain the server URL after logout: {config_text}"
    );
}
