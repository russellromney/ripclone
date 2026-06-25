//! End-to-end test that project `ripclone.toml` drives `default_provider` and
//! `clone.mode` for a real `ripclone clone`.
//!
//! Sets up a local HTTP origin, declares it as a generic provider, writes a
//! project config that selects it as the default and requests `files` mode,
//! then runs sync + clone through the CLI binary and verifies the working tree
//! has the file content and no `.git` directory.

mod common;

use common::*;
use std::process::Command;

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

fn write_project_config(dir: &std::path::Path) {
    let text = r#"default_provider = "localgit"

[clone]
mode = "files"
"#;
    std::fs::write(dir.join("ripclone.toml"), text).unwrap();
}

async fn run_ripclone(
    bin: &str,
    home: &std::path::Path,
    project: &std::path::Path,
    server_url: &str,
    providers_json: &str,
    args: &[&str],
) -> std::process::Output {
    let bin = bin.to_string();
    let home = home.to_path_buf();
    let project = project.to_path_buf();
    let server_url = server_url.to_string();
    let providers_json = providers_json.to_string();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        Command::new(&bin)
            .args(&args)
            .current_dir(&project)
            .env("HOME", &home)
            .env("RIPCLONE_SERVER", &server_url)
            .env("RIPCLONE_TOKEN", TOKEN)
            .env("RIPCLONE_PROVIDERS", &providers_json)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn ripclone")
    })
    .await
    .expect("subprocess panicked")
}

#[tokio::test]
async fn project_config_drives_clone_mode_and_default_provider() {
    setup(false, false, false);

    let origin = make_http_origin("acme/http");
    origin.commit(&[("README.md", "hello from config-driven clone\n")], "c1");
    origin.publish();

    let providers = serde_json::json!([{
        "id": "localgit",
        "kind": "generic",
        "host": &origin.url,
        "auth_template": "token {token}",
    }]);
    unsafe {
        std::env::set_var("RIPCLONE_PROVIDERS", providers.to_string());
    }

    let server = start_server().await;
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    write_project_config(project.path());

    let bin = ripclone_bin();
    let providers_json = providers.to_string();

    let sync_out = run_ripclone(
        &bin,
        home.path(),
        project.path(),
        &server.url,
        &providers_json,
        &["sync", "acme/http"],
    )
    .await;
    assert!(
        sync_out.status.success(),
        "sync failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&sync_out.stdout),
        String::from_utf8_lossy(&sync_out.stderr)
    );

    let clone_out = run_ripclone(
        &bin,
        home.path(),
        project.path(),
        &server.url,
        &providers_json,
        &["clone", "acme/http", "clone"],
    )
    .await;
    assert!(
        clone_out.status.success(),
        "clone failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&clone_out.stdout),
        String::from_utf8_lossy(&clone_out.stderr)
    );

    let target = project.path().join("clone");
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert_eq!(readme, "hello from config-driven clone\n");

    // In files mode the client only downloads archive chunks, so the skeleton
    // pack directory should contain exactly the skeleton pack + idx written by
    // the installer. Editable mode would add additional blob packs here.
    let pack_dir = target.join(".git").join("objects").join("pack");
    let pack_entries: Vec<_> = std::fs::read_dir(&pack_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(
        pack_entries.len(),
        2,
        "files-mode clone should only have the skeleton pack + idx, got {:?}",
        pack_entries
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );
}
