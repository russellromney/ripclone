//! End-to-end test for legacy `config.json` token migration.
//!
//! Writes a new TOML config with the server URL and an old JSON config with the
//! raw token, then runs `ripclone sync` without any server-token env var. The
//! CLI should fall back to the legacy JSON token, hash it, and authenticate.

use crate::common;

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

fn legacy_config_command(bin: &str, cwd: &std::path::Path, home: &std::path::Path) -> Command {
    let mut command = Command::new(bin);
    command
        .current_dir(cwd)
        .env("HOME", home)
        // The shared test setup and parallel server fixtures use these variables.
        // Remove them from this child so only the legacy+new config files can
        // provide its server and token.
        .env_remove("RIPCLONE_CONFIG")
        .env_remove("RIPCLONE_PROVIDERS")
        .env_remove("RIPCLONE_SERVER")
        .env_remove("RIPCLONE_SERVER_TOKEN")
        .env_remove("RIPCLONE_SERVER_TOKEN_HASH");
    command
}

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

    let bin = ripclone_bin();
    let home_path = home.path().to_path_buf();
    let cwd_path = cwd.path().to_path_buf();

    let add_output = tokio::task::spawn_blocking({
        let bin = bin.clone();
        let home_path = home_path.clone();
        let cwd_path = cwd_path.clone();
        move || {
            legacy_config_command(&bin, &cwd_path, &home_path)
                .arg("add")
                .arg("acme/migrate")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output()
                .expect("spawn ripclone add")
        }
    })
    .await
    .expect("subprocess panicked");

    assert!(
        add_output.status.success(),
        "add with legacy token failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&add_output.stdout),
        String::from_utf8_lossy(&add_output.stderr)
    );

    let output = tokio::task::spawn_blocking(move || {
        legacy_config_command(&ripclone_bin(), &cwd_path, &home_path)
            .arg("sync")
            .arg("acme/migrate")
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
