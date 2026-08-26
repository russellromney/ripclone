//! End-to-end tests for repository build settings in the server-owned control
//! database: persistence, fail-closed admission, immutable job snapshots, and
//! the removal of branch overrides and artifact-metadata storage.

use crate::common;

use common::*;
use ripclone::mode::CloneMode;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

fn admin_url(server: &Server, owner: &str, repo: &str, branch: Option<&str>) -> String {
    let mut url = format!("{}/v1/admin/config/{owner}/{repo}", server.url);
    if let Some(b) = branch {
        url.push_str(&format!("?branch={b}"));
    }
    url
}

async fn admin_put(
    server: &Server,
    owner: &str,
    repo: &str,
    branch: Option<&str>,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(admin_url(server, owner, repo, branch))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .json(&body)
        .send()
        .await
        .expect("admin put request")
}

async fn admin_get(
    server: &Server,
    owner: &str,
    repo: &str,
    branch: Option<&str>,
) -> reqwest::Response {
    reqwest::Client::new()
        .get(admin_url(server, owner, repo, branch))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("admin get request")
}

async fn spawn_persistent_server(root: &Path, port: u16) -> tokio::process::Child {
    spawn_persistent_server_env(root, port, &[]).await
}

async fn spawn_persistent_server_env(
    root: &Path,
    port: u16,
    extra_env: &[(&str, &str)],
) -> tokio::process::Child {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ripclone-server"));
    command
        .env_clear()
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_CONFIG", root.join("missing-config.toml"))
        .env(
            "RIPCLONE_ORIGIN_BASE",
            format!("file://{}", origin_root().display()),
        )
        .env("RIPCLONE_TRUST_GATEWAY", "1")
        .env("RIPCLONE_NO_CACHE", "1")
        .env("RIPCLONE_RATE_LIMIT_BURST", "1000000")
        .env("RIPCLONE_RATE_LIMIT_PER_SEC", "1000000")
        .env("RIPCLONE_POLL_INTERVAL_SECS", "3600")
        .arg("--cas-dir")
        .arg(root.join("cas"))
        .arg("--repo-root")
        .arg(root.join("repos"))
        .arg("--control-db")
        .arg(root.join("control.db"))
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().unwrap();
    let ready = format!("http://127.0.0.1:{port}/readyz");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if reqwest::get(&ready)
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("persistent server became ready within the bound");
    child
}

async fn exact_row_json(control_path: &Path, repo_key: &str, commit: &str) -> String {
    let database = libsql::Builder::new_local(control_path)
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let mut rows = connection
        .query(
            "SELECT data FROM results WHERE repo_key = ?1 AND commit_id = ?2",
            libsql::params![repo_key, commit],
        )
        .await
        .unwrap();
    rows.next()
        .await
        .unwrap()
        .expect("exact result row exists")
        .get::<String>(0)
        .unwrap()
}

async fn control_counts(path: &Path) -> (i64, i64, i64, i64) {
    let database = libsql::Builder::new_local(path).build().await.unwrap();
    let connection = database.connect().unwrap();
    let mut counts = Vec::new();
    for table in ["repository_configs", "jobs", "workers", "results"] {
        let mut rows = connection
            .query(&format!("SELECT COUNT(*) FROM {table}"), ())
            .await
            .unwrap();
        counts.push(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap());
    }
    (counts[0], counts[1], counts[2], counts[3])
}

fn files_under(root: &Path) -> Vec<PathBuf> {
    fn visit(root: &Path, files: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                visit(&path, files);
            } else {
                files.push(path);
            }
        }
    }
    let mut files = Vec::new();
    visit(root, &mut files);
    files.sort();
    files
}

#[tokio::test]
async fn admin_config_round_trips_and_removed_branch_query_has_no_side_effects() {
    init(false);
    let server = start_server().await;

    // Absent until written.
    assert_eq!(
        admin_get(&server, "acme", "cfg", None).await.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    // Write a repo-level config.
    let resp = admin_put(
        &server,
        "acme",
        "cfg",
        None,
        serde_json::json!({ "compression_level": 9, "archive_chunk_size": 12 }),
    )
    .await;
    assert!(
        resp.status().is_success(),
        "put repo config: {:?}",
        resp.status()
    );

    // Read it back.
    let got: serde_json::Value = admin_get(&server, "acme", "cfg", None)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(got["compression_level"], 9);
    assert_eq!(got["archive_chunk_size"], 12);

    let counts_before = control_counts(&server.control_db).await;
    let artifacts_before = files_under(&server.storage_dir);

    // Branch-level overrides are removed, including an explicitly empty query.
    let resp = admin_put(
        &server,
        "acme",
        "cfg",
        Some("release"),
        serde_json::json!({ "compression_level": 19 }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = resp.text().await.unwrap();
    assert!(error.contains("branch-level repository configuration"));
    assert_eq!(
        admin_get(&server, "acme", "cfg", Some("release"))
            .await
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(
        admin_get(&server, "acme", "cfg", Some("")).await.status(),
        reqwest::StatusCode::BAD_REQUEST
    );

    // The repo-level config is unchanged by the branch write.
    let repo_cfg: serde_json::Value = admin_get(&server, "acme", "cfg", None)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(repo_cfg["compression_level"], 9);
    assert_eq!(control_counts(&server.control_db).await, counts_before);
    assert_eq!(files_under(&server.storage_dir), artifacts_before);
    assert!(
        files_under(&server.storage_dir)
            .iter()
            .all(|path| !path.to_string_lossy().contains("repo-config"))
    );
}

#[tokio::test]
async fn admin_rejects_invalid_config() {
    init(false);
    let server = start_server().await;

    // Compression level out of range.
    let resp = admin_put(
        &server,
        "acme",
        "bad",
        None,
        serde_json::json!({ "compression_level": 99 }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Three structural variants are beyond what the build can emit today.
    let resp = admin_put(
        &server,
        "acme",
        "bad",
        None,
        serde_json::json!({
            "clonepack_depths": [
                { "name": "shallow", "depth": 1 },
                { "name": "recent", "depth": 50 },
                { "name": "full", "depth": null }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Nothing got stored.
    assert_eq!(
        admin_get(&server, "acme", "bad", None).await.status(),
        reqwest::StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn repository_config_failures_admit_no_work_or_artifacts() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "config-failure");
    origin.commit(&[("a.txt", "never built\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/config-failure")
        .await
        .unwrap();
    assert!(
        admin_put(
            &server,
            "acme",
            "config-failure",
            None,
            serde_json::json!({ "compression_level": 3 }),
        )
        .await
        .status()
        .is_success()
    );

    let database = libsql::Builder::new_local(&server.control_db)
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let repo_key = ripclone::provider::RepoId::github("acme/config-failure").storage_key();
    let baseline_counts = control_counts(&server.control_db).await;
    let baseline_artifacts = files_under(&server.storage_dir);

    async fn assert_admission_fails_without_state(
        server: &Server,
        baseline_counts: (i64, i64, i64, i64),
        baseline_artifacts: &[PathBuf],
    ) {
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            server.client().admit_sync_repo("acme/config-failure", None),
        )
        .await
        .expect("failed config admission returned within the bound")
        .expect_err("bad repository config must reject admission");
        assert!(
            error.to_string().contains("repository config read failed"),
            "unexpected admission error: {error:#}"
        );
        assert_eq!(control_counts(&server.control_db).await, baseline_counts);
        assert_eq!(files_under(&server.storage_dir), baseline_artifacts);
    }

    connection
        .execute(
            "UPDATE repository_configs SET data = ?1 WHERE repo_key = ?2",
            libsql::params!["not-json", repo_key.as_str()],
        )
        .await
        .unwrap();
    assert_admission_fails_without_state(&server, baseline_counts, &baseline_artifacts).await;

    connection
        .execute(
            "UPDATE repository_configs SET data = ?1 WHERE repo_key = ?2",
            libsql::params![r#"{"compression_level":99}"#, repo_key.as_str()],
        )
        .await
        .unwrap();
    assert_admission_fails_without_state(&server, baseline_counts, &baseline_artifacts).await;

    connection
        .execute(
            "ALTER TABLE repository_configs RENAME TO unavailable_repository_configs",
            (),
        )
        .await
        .unwrap();
    let counts_without_config_table = (
        baseline_counts.0,
        baseline_counts.1,
        baseline_counts.2,
        baseline_counts.3,
    );
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        server.client().admit_sync_repo("acme/config-failure", None),
    )
    .await
    .expect("database read failure returned within the bound")
    .expect_err("missing repository config table must reject admission");
    assert!(error.to_string().contains("repository config read failed"));
    let mut rows = connection
        .query(
            "SELECT
                (SELECT COUNT(*) FROM unavailable_repository_configs),
                (SELECT COUNT(*) FROM jobs),
                (SELECT COUNT(*) FROM workers),
                (SELECT COUNT(*) FROM results)",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        (
            row.get::<i64>(0).unwrap(),
            row.get::<i64>(1).unwrap(),
            row.get::<i64>(2).unwrap(),
            row.get::<i64>(3).unwrap(),
        ),
        counts_without_config_table
    );
    assert_eq!(files_under(&server.storage_dir), baseline_artifacts);
    connection
        .execute(
            "ALTER TABLE unavailable_repository_configs RENAME TO repository_configs",
            (),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn configured_compression_still_clones_correctly() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "compress");
    origin.commit(&[("a.txt", "one\n"), ("dir/b.txt", "bee\n")], "c1");
    origin.commit(&[("a.txt", "two\n")], "c2");
    origin.publish();

    // Configure a non-default compression level for this repo.
    let resp = admin_put(
        &server,
        "acme",
        "compress",
        None,
        serde_json::json!({ "compression_level": 3 }),
    )
    .await;
    assert!(resp.status().is_success());

    // The build reads the config; the clone must still be byte-correct.
    let (_g, c) = sync_and_clone(&server, &origin, 0, CloneMode::Editable).await;
    assert_eq!(read(&c, "a.txt"), "two\n");
    assert_eq!(read(&c, "dir/b.txt"), "bee\n");
    assert_eq!(git(&c, &["rev-list", "--count", "HEAD"]), "2");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    // Files mode (uses the archive built at the configured level) is correct too.
    let (_g2, f) = sync_and_clone(&server, &origin, 0, CloneMode::Files).await;
    assert_eq!(read(&f, "a.txt"), "two\n");
}

#[tokio::test]
async fn repository_config_survives_server_restart_and_drives_embedded_job_snapshot() {
    init(false);
    let root = tempfile::tempdir().unwrap();
    let origin = make_origin("acme", "restart-config");
    let commit = origin.commit(&[("value.txt", "persisted config\n")], "configured");
    origin.publish();

    let first_port = free_port();
    let first_url = format!("http://127.0.0.1:{first_port}");
    let mut first = spawn_persistent_server(root.path(), first_port).await;
    let stored = reqwest::Client::new()
        .post(format!("{first_url}/v1/admin/config/acme/restart-config"))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .json(&serde_json::json!({ "compression_level": 3 }))
        .send()
        .await
        .unwrap();
    assert!(stored.status().is_success());
    first.kill().await.unwrap();
    first.wait().await.unwrap();

    let second_port = free_port();
    let second_url = format!("http://127.0.0.1:{second_port}");
    let mut second = spawn_persistent_server(root.path(), second_port).await;
    let persisted: serde_json::Value = reqwest::Client::new()
        .get(format!("{second_url}/v1/admin/config/acme/restart-config"))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(persisted["compression_level"], 3);

    let client = ripclone::client::Client::new_with_token(second_url, Some(token_hash()));
    let ready = tokio::time::timeout(
        Duration::from_secs(60),
        client.add_repo("acme/restart-config"),
    )
    .await
    .expect("embedded build completed within the bound")
    .expect("embedded build with persisted config succeeded");
    assert_eq!(ready.commit, commit);
    let clone = root.path().join("clone");
    client
        .install_repo_with_mode_at(
            "acme/restart-config",
            "HEAD",
            None,
            &clone,
            CloneMode::Files,
            Some("full"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(read(&clone, "value.txt"), "persisted config\n");

    let database = libsql::Builder::new_local(root.path().join("control.db"))
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let mut rows = connection
        .query(
            "SELECT repo_config FROM jobs WHERE path = ?1 ORDER BY id DESC LIMIT 1",
            ["acme/restart-config"],
        )
        .await
        .unwrap();
    let snapshot: ripclone::repo_config::RepoConfig = serde_json::from_str(
        &rows
            .next()
            .await
            .unwrap()
            .expect("embedded build has a durable job")
            .get::<String>(0)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(snapshot.compression_level, Some(3));
    assert!(
        files_under(&root.path().join("cas"))
            .iter()
            .all(|path| !path.to_string_lossy().contains("repo-config"))
    );
    second.kill().await.unwrap();
    second.wait().await.unwrap();
}

#[tokio::test]
async fn published_local_results_and_artifacts_survive_cache_retention_restart() {
    init(false);
    let root = tempfile::tempdir().unwrap();
    let origin = make_origin("acme", "durable-local");
    let commit = origin.commit(
        &[
            ("value.txt", "durable B\n"),
            ("nested/file.txt", "files B\n"),
        ],
        "B",
    );
    origin.publish();

    let first_port = free_port();
    let first_url = format!("http://127.0.0.1:{first_port}");
    let mut first = spawn_persistent_server(root.path(), first_port).await;
    let first_client = ripclone::client::Client::new_with_token(first_url, Some(token_hash()));
    first_client
        .add_repo("acme/durable-local")
        .await
        .expect("build durable local B");
    for result in [
        ripclone::ExactResultKind::Head,
        ripclone::ExactResultKind::Full,
        ripclone::ExactResultKind::Files,
    ] {
        let ready = first_client
            .resolve_exact_result("acme/durable-local", "HEAD", result, Some(&commit))
            .await
            .expect("all exact B results become ready");
        assert_eq!(ready.commit, commit);
        assert_eq!(ready.result, result);
    }
    first.kill().await.unwrap();
    first.wait().await.unwrap();

    let control_path = root.path().join("control.db");
    let repo_key = ripclone::provider::RepoId::github("acme/durable-local").storage_key();
    let row_before = exact_row_json(&control_path, &repo_key, &commit).await;
    let artifacts_before = files_under(&root.path().join("cas"));
    assert!(
        !artifacts_before.is_empty(),
        "local build published artifacts"
    );
    let old = filetime::FileTime::from_system_time(
        std::time::SystemTime::now() - Duration::from_secs(2 * 24 * 60 * 60),
    );
    for artifact in &artifacts_before {
        filetime::set_file_mtime(artifact, old).unwrap();
    }

    let second_port = free_port();
    let second_url = format!("http://127.0.0.1:{second_port}");
    let mut second = spawn_persistent_server_env(
        root.path(),
        second_port,
        &[
            ("RIPCLONE_RETENTION_INTERVAL_SECS", "1"),
            ("RIPCLONE_RETENTION_MAX_AGE_DAYS", "1"),
            ("RIPCLONE_RETENTION_MAX_GB", "1"),
        ],
    )
    .await;
    let client = ripclone::client::Client::new_with_token(second_url.clone(), Some(token_hash()));
    for (name, mode, clonepack) in [
        ("head", CloneMode::Editable, "shallow"),
        ("full", CloneMode::Editable, "full"),
        ("files", CloneMode::Files, "full"),
    ] {
        let target = root.path().join(format!("clone-{name}"));
        let outcome = client
            .install_repo_with_mode_at(
                "acme/durable-local",
                "HEAD",
                Some(&commit),
                &target,
                mode,
                Some(clonepack),
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("clone exact B {name}: {error:#}"));
        assert_eq!(outcome.commit, commit);
        assert_eq!(read(&target, "value.txt"), "durable B\n");
    }

    assert_eq!(
        exact_row_json(&control_path, &repo_key, &commit).await,
        row_before,
        "restart and exact clones must not mutate published B"
    );
    for artifact in &artifacts_before {
        assert!(
            artifact.exists(),
            "local cache settings must not delete durable artifact {}",
            artifact.display()
        );
    }

    let removed = reqwest::Client::new()
        .delete(format!(
            "{second_url}/v1/repos/github/acme/durable-local/add"
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .unwrap();
    assert!(removed.status().is_success());
    assert_eq!(
        exact_row_json(&control_path, &repo_key, &commit).await,
        row_before,
        "removing repository admission must retain exact B"
    );
    for artifact in &artifacts_before {
        assert!(artifact.exists());
    }

    second.kill().await.unwrap();
    second.wait().await.unwrap();
}

#[tokio::test]
async fn unconfigured_repo_clones_like_today() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "default");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();

    // No config stored → default behavior, clone works as before.
    let (_g, c) = sync_and_clone(&server, &origin, 0, CloneMode::Editable).await;
    assert_eq!(read(&c, "a.txt"), "hello\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
}
