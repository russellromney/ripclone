//! End-to-end test for self-host `ripclone login` and `ripclone auth logout`.
//!
//! Runs the CLI as a subprocess with an isolated $HOME and verifies that a
//! non-cloud `ripclone login` uses the self-host paste flow.

use ripclone::auth::token_store::{FileTokenStore, TokenStore};
use std::io::Write;
use std::process::Command;

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

fn token_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".config").join("ripclone").join("tokens.json")
}

fn config_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".config").join("ripclone").join("config.toml")
}

#[tokio::test]
async fn self_host_login_saves_url_and_session_token() {
    let home = tempfile::tempdir().unwrap();
    let url = "http://127.0.0.1:59321".to_string();

    let home_path = home.path().to_path_buf();
    let bin = ripclone_bin();
    let url_for_login = url.clone();
    let output = tokio::task::spawn_blocking(move || {
        let mut child = Command::new(&bin)
            .arg("login")
            .env("HOME", &home_path)
            .env("RIPCLONE_SERVER", &url_for_login)
            .env("RIPCLONE_NO_BROWSER", "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn ripclone login");
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"self_host_session_token\n")
            .unwrap();
        child.wait_with_output().expect("wait for ripclone login")
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
        !config_text.contains("self_host_session_token"),
        "config.toml must not contain the token: {config_text}"
    );

    let store = FileTokenStore::new(token_path(home.path()));
    let session_key = format!("session:{url}");
    assert_eq!(
        store.get(&session_key).unwrap().as_deref(),
        Some("self_host_session_token"),
        "session token should be in the token store"
    );

    // Auth logout should remove the session token but leave the server URL in config.
    let home_path = home.path().to_path_buf();
    let bin = ripclone_bin();
    let url_for_logout = url.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(&bin)
            .args(["auth", "logout"])
            .env("HOME", &home_path)
            .env("RIPCLONE_SERVER", &url_for_logout)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn ripclone auth logout")
    })
    .await
    .expect("logout subprocess panicked");

    assert!(
        output.status.success(),
        "auth logout failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        store.get(&session_key).unwrap().is_none(),
        "session token should be removed after auth logout"
    );
    let config_text = std::fs::read_to_string(config_path(home.path())).unwrap();
    assert!(
        config_text.contains(&url),
        "config.toml should still contain the server URL after logout: {config_text}"
    );
}
