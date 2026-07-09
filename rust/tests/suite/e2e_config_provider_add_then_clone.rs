//! End-to-end test for the full provider-management workflow.
//!
//! 1. `ripclone provider add localgit ...` writes the provider and token into
//!    the global `config.toml`.
//! 2. A project `ripclone.toml` selects that provider as the default.
//! 3. `ripclone sync` + `ripclone clone` run through the real server using the
//!    configured provider, without any provider env vars.

use crate::common::*;
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
    cwd: &std::path::Path,
    server_url: Option<&str>,
    args: &[&str],
) -> std::process::Output {
    let bin = bin.to_string();
    let home = home.to_path_buf();
    let cwd = cwd.to_path_buf();
    let server_url = server_url.map(|s| s.to_string());
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new(&bin);
        cmd.args(&args)
            .current_dir(&cwd)
            .env("HOME", &home)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(url) = server_url {
            cmd.env("RIPCLONE_SERVER", url);
        }
        cmd.output().expect("spawn ripclone")
    })
    .await
    .expect("subprocess panicked")
}

#[tokio::test]
async fn provider_add_then_config_driven_clone() {
    setup(false);

    let origin = make_http_origin("acme/provider_add");
    origin.commit(&[("README.md", "provider add workflow\n")], "c1");
    origin.publish();

    let home = tempfile::tempdir().unwrap();
    // The server loads the provider registry from $HOME, so point the whole
    // test process at the isolated home directory before adding the provider
    // and starting the server.
    unsafe {
        std::env::set_var("HOME", home.path());
    }

    let project = tempfile::tempdir().unwrap();
    write_project_config(project.path());
    let bin = ripclone_bin();

    // Add the provider via the CLI before starting the server, so the server
    // loads the freshly-written global config.
    let add_out = run_ripclone(
        &bin,
        home.path(),
        project.path(),
        None,
        &[
            "provider",
            "add",
            "localgit",
            "--kind",
            "generic",
            "--host",
            &origin.url,
            "--auth-template",
            "token {token}",
            "--token",
            "test-token",
        ],
    )
    .await;
    assert!(
        add_out.status.success(),
        "provider add failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&add_out.stdout),
        String::from_utf8_lossy(&add_out.stderr)
    );

    let server = start_server().await;

    let repo_add_out = run_ripclone(
        &bin,
        home.path(),
        project.path(),
        Some(&server.url),
        &["add", "acme/provider_add"],
    )
    .await;
    assert!(
        repo_add_out.status.success(),
        "repo add failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&repo_add_out.stdout),
        String::from_utf8_lossy(&repo_add_out.stderr)
    );

    let sync_out = run_ripclone(
        &bin,
        home.path(),
        project.path(),
        Some(&server.url),
        &["sync", "acme/provider_add"],
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
        Some(&server.url),
        &["clone", "acme/provider_add", "clone"],
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
    assert_eq!(readme, "provider add workflow\n");
    assert!(
        !target.join(".git").exists(),
        "files-mode clone should materialize only files, not a git repository"
    );
}
