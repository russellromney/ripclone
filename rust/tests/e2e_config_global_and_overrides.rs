//! End-to-end test for global `~/.config/ripclone/config.toml` defaults and
//! CLI flag overrides.
//!
//! Uses a real ripclone server + local origin. The global config requests
//! `files` mode at `depth = 1`; we verify a bare `ripclone clone` honors that,
//! then verify `--mode editable --depth 0` overrides the config.

mod common;

use common::*;
use std::process::Command;

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

fn write_global_config(home: &std::path::Path, server_url: &str) {
    let dir = home.join(".config").join("ripclone");
    std::fs::create_dir_all(&dir).unwrap();
    let text = format!(
        r#"server = "{server_url}"
default_provider = "github"

[clone]
depth = 1
mode = "files"
"#
    );
    std::fs::write(dir.join("config.toml"), text).unwrap();
}

async fn run_ripclone(
    bin: &str,
    home: &std::path::Path,
    cwd: &std::path::Path,
    server_url: &str,
    args: &[&str],
) -> std::process::Output {
    let bin = bin.to_string();
    let home = home.to_path_buf();
    let cwd = cwd.to_path_buf();
    let server_url = server_url.to_string();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        Command::new(&bin)
            .args(&args)
            .current_dir(&cwd)
            .env("HOME", &home)
            .env("RIPCLONE_SERVER", &server_url)
            .env("RIPCLONE_TOKEN", TOKEN)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn ripclone")
    })
    .await
    .expect("subprocess panicked")
}

fn editable_pack_entry_count(target: &std::path::Path) -> usize {
    target
        .join(".git")
        .join("objects")
        .join("pack")
        .read_dir()
        .unwrap()
        .filter_map(|e| e.ok())
        .count()
}

#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn global_config_defaults_and_cli_overrides() {
    setup(false);

    let origin = make_origin("acme", "config");
    origin.commit(&[("README.md", "global config test\n")], "c1");
    origin.publish();

    let server = start_server().await;
    let home = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    write_global_config(home.path(), &server.url);

    let bin = ripclone_bin();

    // Sync first so artifacts exist for the clone.
    let sync_out = run_ripclone(
        &bin,
        home.path(),
        cwd.path(),
        &server.url,
        &["sync", "acme/config"],
    )
    .await;
    assert!(
        sync_out.status.success(),
        "sync failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&sync_out.stdout),
        String::from_utf8_lossy(&sync_out.stderr)
    );

    // 1. Bare clone with no flags → config says files mode + depth 1.
    let files_out = run_ripclone(
        &bin,
        home.path(),
        cwd.path(),
        &server.url,
        &["clone", "acme/config", "files_clone"],
    )
    .await;
    assert!(
        files_out.status.success(),
        "files clone failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&files_out.stdout),
        String::from_utf8_lossy(&files_out.stderr)
    );

    let files_target = cwd.path().join("files_clone");
    assert_eq!(
        std::fs::read_to_string(files_target.join("README.md")).unwrap(),
        "global config test\n"
    );
    assert!(
        !files_target.join(".git").exists(),
        "files-mode clone should materialize only files, not a git repository"
    );

    // 2. Override mode and depth via CLI flags → editable full clone.
    let editable_out = run_ripclone(
        &bin,
        home.path(),
        cwd.path(),
        &server.url,
        &[
            "clone",
            "--mode",
            "editable",
            "--depth",
            "0",
            "acme/config",
            "editable_clone",
        ],
    )
    .await;
    assert!(
        editable_out.status.success(),
        "editable clone failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&editable_out.stdout),
        String::from_utf8_lossy(&editable_out.stderr)
    );

    let editable_target = cwd.path().join("editable_clone");
    assert_eq!(
        std::fs::read_to_string(editable_target.join("README.md")).unwrap(),
        "global config test\n"
    );
    assert!(
        editable_pack_entry_count(&editable_target) > 2,
        "editable clone should install additional blob packs beyond the skeleton"
    );
}
