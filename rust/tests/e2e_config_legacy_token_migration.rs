//! End-to-end test for legacy `config.json` token migration.
//!
//! Writes a new TOML config with the server URL and an old JSON config with the
//! raw token, then runs `ripclone sync` without any server-token env var. The
//! CLI should fall back to the legacy JSON token, hash it, and authenticate.

mod common;

use common::*;
use std::process::Command;

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

fn write_legacy_config(home: &std::path::Path, server_url: &str, token: &str) {
    let dir = home.join(".config").join("ripclone");
    std::fs::create_dir_all(&dir).unwrap();

    let toml = format!(r#"server = "{server_url}""#);
    std::fs::write(dir.join("config.toml"), toml).unwrap();

    let json = format!(r#"{{"token":"{token}","server":"{server_url}"}}"#);
    std::fs::write(dir.join("config.json"), json).unwrap();
}

#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn legacy_config_json_token_still_authenticates() {
    setup(false);

    let origin = make_origin("acme", "migrate");
    origin.commit(&[("README.md", "legacy token migration\n")], "c1");
    origin.publish();

    let server = start_server().await;
    let home = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    write_legacy_config(home.path(), &server.url, TOKEN);

    let output = tokio::task::spawn_blocking(move || {
        Command::new(ripclone_bin())
            .arg("sync")
            .arg("acme/migrate")
            .current_dir(cwd.path())
            .env("HOME", home.path())
            // Intentionally do NOT set RIPCLONE_SERVER or RIPCLONE_TOKEN.
            // The CLI must read both from the legacy+new config files.
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn ripclone sync")
    })
    .await
    .expect("subprocess panicked");

    assert!(
        output.status.success(),
        "sync with legacy token failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
