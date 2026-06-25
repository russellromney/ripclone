//! End-to-end test that project `ripclone.toml` drives `default_provider` and
//! `clone.depth` for a `ripclone sync`.
//!
//! A mock server captures the request path and query; no real upstream or build
//! is needed.

use axum::{Json, Router, extract::Path, extract::Query, routing::post};
use serde::Serialize;
use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, Mutex};

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

#[derive(Serialize, Default)]
struct RefResponse {
    owner: String,
    repo: String,
    provider: String,
    host: String,
    origin_url: String,
    branch: String,
    default_branch: String,
    commit: String,
    parent_commit: Option<String>,
    full_pack: String,
    clonepack_manifest: String,
    metadata_chunk: String,
}

#[derive(Clone, Default)]
struct Capture {
    path: String,
    query: HashMap<String, String>,
}

async fn sync_capture(
    Path(path): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    state: axum::extract::State<Arc<Mutex<Capture>>>,
) -> Json<RefResponse> {
    let mut capture = state.lock().unwrap();
    capture.path = path;
    capture.query = query.clone();
    Json(RefResponse {
        owner: "owner".into(),
        repo: "repo".into(),
        provider: "my-gitea".into(),
        host: "gitea.example.com".into(),
        origin_url: "https://gitea.example.com/owner/repo.git".into(),
        branch: "main".into(),
        default_branch: "main".into(),
        commit: "abc123".into(),
        parent_commit: None,
        full_pack: "pack".into(),
        clonepack_manifest: "manifest".into(),
        metadata_chunk: "metadata".into(),
    })
}

fn write_project_config(dir: &std::path::Path) {
    let text = r#"default_provider = "my-gitea"

[clone]
depth = 5

[providers.my-gitea]
kind = "gitea"
host = "https://gitea.example.com"
"#;
    std::fs::write(dir.join("ripclone.toml"), text).unwrap();
}

#[tokio::test]
async fn project_config_drives_default_provider_and_depth() {
    let capture = Arc::new(Mutex::new(Capture::default()));
    let app = Router::new()
        .route("/{*path}", post(sync_capture))
        .with_state(capture.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    write_project_config(project.path());

    let url = format!("http://{addr}");
    let home_path = home.path().to_path_buf();
    let project_path = project.path().to_path_buf();
    let bin = ripclone_bin();
    let url_for_sync = url.clone();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(&bin)
            .arg("sync")
            .arg("owner/repo")
            .current_dir(&project_path)
            .env("HOME", &home_path)
            .env("RIPCLONE_SERVER", &url_for_sync)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn ripclone sync")
    })
    .await
    .expect("sync subprocess panicked");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "sync failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        stderr
    );

    let captured = capture.lock().unwrap();
    assert_eq!(
        captured.path, "v1/repos/my-gitea/owner/repo/sync",
        "request should use the configured default_provider"
    );
    assert_eq!(
        captured.query.get("depth").map(String::as_str),
        Some("5"),
        "request should use the configured clone.depth"
    );
}
