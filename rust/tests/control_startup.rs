//! Removed and incomplete control configuration fails before the server binds,
//! opens control state, or creates artifact and mirror directories.

use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const REMOVED: &[(&str, &str)] = &[
    ("RIPCLONE_METADATA", "postgres"),
    ("RIPCLONE_METADATA_DB_URL", "postgres://decoy/metadata"),
    ("RIPCLONE_METADATA_DB_TOKEN", "decoy-metadata-token"),
    ("RIPCLONE_QUEUE", "mysql"),
    ("RIPCLONE_QUEUE_DB_URL", "mysql://decoy/queue"),
    ("RIPCLONE_QUEUE_DB_TOKEN", "decoy-queue-token"),
    ("RIPCLONE_DISPATCH", "http"),
    ("RIPCLONE_DISPATCH_CMD", "/bin/false"),
    ("RIPCLONE_DISPATCH_CMD_ARGS", "--decoy"),
    ("RIPCLONE_DISPATCH_INTERVAL_SECS", "1"),
    ("RIPCLONE_DISPATCH_MAX_WORKERS", "9"),
    ("RIPCLONE_DISPATCH_TOKEN", "decoy-dispatch-token"),
    ("RIPCLONE_DISPATCH_URL", "http://127.0.0.1:9"),
    ("RIPCLONE_HEARTBEAT_URL", "http://127.0.0.1:9"),
    ("RIPCLONE_RECHECK_MAX", "3"),
    ("RIPCLONE_REF_CACHE_TTL_SECS", "30"),
];

fn server_command(root: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ripclone-server"));
    command
        .env_clear()
        .env("RIPCLONE_SERVER_TOKEN", "startup-proof")
        .arg("--cas-dir")
        .arg(root.join("cas"))
        .arg("--repo-root")
        .arg(root.join("repos"))
        .arg("--control-db")
        .arg(root.join("control.db"))
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg("0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

async fn run_bounded(mut command: tokio::process::Command) -> std::process::Output {
    tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .expect("invalid configuration must fail within five seconds")
        .expect("spawn binary")
}

fn assert_no_runtime_side_effects(root: &Path) {
    for path in [
        root.join("cas"),
        root.join("repos"),
        root.join("control.db"),
        root.join("control.db.owner"),
    ] {
        assert!(
            !path.exists(),
            "unexpected startup side effect: {}",
            path.display()
        );
    }
}

#[tokio::test]
async fn every_removed_environment_selector_fails_without_side_effects() {
    for &(key, value) in REMOVED {
        let root = tempfile::tempdir().unwrap();
        let mut command = server_command(root.path());
        command.env(key, value);
        let output = run_bounded(command).await;
        assert!(
            !output.status.success(),
            "{key} unexpectedly started the server"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(key), "{key} was not named in: {stderr}");
        assert_no_runtime_side_effects(root.path());
    }
}

#[tokio::test]
async fn incomplete_turso_pairs_fail_without_side_effects() {
    for &(key, value) in &[
        ("RIPCLONE_TURSO_DATABASE_URL", "libsql://decoy.invalid"),
        ("RIPCLONE_TURSO_AUTH_TOKEN", "decoy-turso-token"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut command = server_command(root.path());
        command.env(key, value);
        let output = run_bounded(command).await;
        assert!(!output.status.success());
        assert_no_runtime_side_effects(root.path());
    }
}

#[tokio::test]
async fn explicitly_empty_control_values_fail_without_side_effects() {
    for key in [
        "RIPCLONE_CONTROL_DB_PATH",
        "RIPCLONE_TURSO_DATABASE_URL",
        "RIPCLONE_TURSO_AUTH_TOKEN",
    ] {
        for value in ["", "   "] {
            let root = tempfile::tempdir().unwrap();
            let mut command = server_command(root.path());
            command.env(key, value);
            let output = run_bounded(command).await;
            assert!(!output.status.success(), "{key}={value:?} was accepted");
            assert!(String::from_utf8_lossy(&output.stderr).contains(key));
            assert_no_runtime_side_effects(root.path());
        }
    }

    let root = tempfile::tempdir().unwrap();
    let mut command = server_command(root.path());
    command
        .env("RIPCLONE_TURSO_DATABASE_URL", "")
        .env("RIPCLONE_TURSO_AUTH_TOKEN", "");
    let output = run_bounded(command).await;
    assert!(
        !output.status.success(),
        "empty Turso pair downgraded to SQLite"
    );
    assert_no_runtime_side_effects(root.path());
}

#[tokio::test]
async fn removed_config_sections_fail_without_side_effects() {
    for contents in [
        "[metadata]\nbackend = 'file'\n",
        "[queue]\nbackend = 'sqlite'\nurl = 'decoy.db'\n",
        "[control]\nturso_url = 'libsql://decoy.invalid'\n",
        "[control]\nturso_token = 'decoy-token'\n",
        "[control]\npath = ''\n",
        "[control]\nturso_url = ''\nturso_token = 'decoy-token'\n",
        "[control]\nturso_url = 'libsql://decoy.invalid'\nturso_token = ''\n",
    ] {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("config.toml");
        std::fs::write(&config, contents).unwrap();
        let mut command = server_command(root.path());
        command.env("RIPCLONE_CONFIG", &config);
        let output = run_bounded(command).await;
        assert!(
            !output.status.success(),
            "config unexpectedly started server: {contents}"
        );
        assert_no_runtime_side_effects(root.path());
    }
}

#[tokio::test]
async fn empty_environment_override_does_not_fall_back_to_valid_config() {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("config.toml");
    std::fs::write(
        &config,
        "[control]\nturso_url = 'libsql://decoy.invalid'\nturso_token = 'decoy-token'\n",
    )
    .unwrap();
    let mut command = server_command(root.path());
    command
        .env("RIPCLONE_CONFIG", &config)
        .env("RIPCLONE_TURSO_AUTH_TOKEN", "");
    let output = run_bounded(command).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("RIPCLONE_TURSO_AUTH_TOKEN"));
    assert_no_runtime_side_effects(root.path());
}

#[tokio::test]
async fn rejected_startup_neither_contacts_s3_nor_mutates_existing_control_rows() {
    let root = tempfile::tempdir().unwrap();
    let control_path = root.path().join("control.db");
    let control = ripclone::control::ControlDb::open(
        &control_path,
        None,
        ripclone::queue::default_size_classes(),
    )
    .await
    .unwrap();
    let queue = control.queue();
    let job_id = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("acme/preserved"),
            admitted_commit: "1111111111111111111111111111111111111111".to_string(),
            repo_config: ripclone::repo_config::RepoConfig::default(),
            credential: None,
            size_bytes: None,
        })
        .await
        .unwrap()
        .job_id
        .unwrap();
    queue
        .heartbeat_at("preserved-worker", None, 1_000)
        .await
        .unwrap();
    drop(queue);
    drop(control);

    let s3_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let endpoint = format!("http://{}", s3_listener.local_addr().unwrap());
    let mut command = server_command(root.path());
    command
        .env("RIPCLONE_QUEUE", "removed")
        .env("RIPCLONE_S3_ENDPOINT", endpoint)
        .env("RIPCLONE_S3_BUCKET", "must-not-touch")
        .env("AWS_ACCESS_KEY_ID", "decoy")
        .env("AWS_SECRET_ACCESS_KEY", "decoy");
    let output = run_bounded(command).await;
    assert!(!output.status.success());
    assert!(
        tokio::time::timeout(Duration::from_millis(250), s3_listener.accept())
            .await
            .is_err(),
        "rejected startup contacted the configured S3 endpoint"
    );

    let database = libsql::Builder::new_local(&control_path)
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let mut jobs = connection
        .query("SELECT id, status FROM jobs ORDER BY id", ())
        .await
        .unwrap();
    let row = jobs.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), job_id);
    assert_eq!(row.get::<String>(1).unwrap(), "queued");
    assert!(jobs.next().await.unwrap().is_none());
    let mut workers = connection
        .query(
            "SELECT worker_id, current_job, last_heartbeat FROM workers",
            (),
        )
        .await
        .unwrap();
    let row = workers.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "preserved-worker");
    assert_eq!(row.get::<Option<i64>>(1).unwrap(), None);
    assert_eq!(row.get::<i64>(2).unwrap(), 1_000);
    assert!(workers.next().await.unwrap().is_none());
    assert!(!root.path().join("cas").exists());
    assert!(!root.path().join("repos").exists());
}

#[tokio::test]
async fn worker_rejects_server_control_credentials_before_scratch_creation() {
    let server_only = [
        ("RIPCLONE_CONTROL_DB_PATH", "decoy-control.db"),
        ("RIPCLONE_TURSO_DATABASE_URL", "libsql://decoy.invalid"),
        ("RIPCLONE_TURSO_AUTH_TOKEN", "decoy-turso-token"),
    ];
    for &(key, value) in REMOVED.iter().chain(server_only.iter()) {
        let root = tempfile::tempdir().unwrap();
        let cas = root.path().join("worker-cas");
        let repos = root.path().join("worker-repos");
        let decoy = root
            .path()
            .join(PathBuf::from(value).file_name().unwrap_or_default());
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ripclone-worker"));
        command
            .env_clear()
            .env(key, &decoy)
            .arg("--cas-dir")
            .arg(&cas)
            .arg("--repo-root")
            .arg(&repos)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = run_bounded(command).await;
        assert!(!output.status.success(), "worker accepted {key}");
        assert!(!cas.exists(), "worker created CAS for {key}");
        assert!(!repos.exists(), "worker created repo scratch for {key}");
        assert!(!decoy.exists(), "worker opened decoy path for {key}");
    }
}

#[tokio::test]
async fn second_server_is_rejected_before_its_listener_or_work_paths() {
    let root = tempfile::tempdir().unwrap();
    let first_port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let second_port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let control = root.path().join("control.db");
    let mut first = tokio::process::Command::new(env!("CARGO_BIN_EXE_ripclone-server"));
    first
        .env_clear()
        .env("RIPCLONE_SERVER_TOKEN", "owner-proof")
        .arg("--cas-dir")
        .arg(root.path().join("first-cas"))
        .arg("--repo-root")
        .arg(root.path().join("first-repos"))
        .arg("--control-db")
        .arg(&control)
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(first_port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut first = first.spawn().unwrap();
    let ready = format!("http://127.0.0.1:{first_port}/readyz");
    for attempt in 0..200 {
        if reqwest::get(&ready)
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        assert!(attempt < 199, "first server did not become ready");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let second_cas = root.path().join("second-cas");
    let second_repos = root.path().join("second-repos");
    let mut second = tokio::process::Command::new(env!("CARGO_BIN_EXE_ripclone-server"));
    second
        .env_clear()
        .env("RIPCLONE_SERVER_TOKEN", "owner-proof")
        .arg("--cas-dir")
        .arg(&second_cas)
        .arg("--repo-root")
        .arg(&second_repos)
        .arg("--control-db")
        .arg(&control)
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(second_port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = run_bounded(second).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("already owned by another server"));
    assert!(!second_cas.exists());
    assert!(!second_repos.exists());
    let listener = std::net::TcpListener::bind(("127.0.0.1", second_port))
        .expect("second server never bound its listener");
    drop(listener);
    first.kill().await.unwrap();
    let _ = first.wait().await;
}
