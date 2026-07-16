use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue, JobState, SqlJobQueue, SqliteDb};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

const REMOVED: &[&str] = &["mysql", "postgres", "postgresql", "libsql", "sqld"];
const REMOVED_URLS: &[&str] = &[
    "mysql://removed",
    "postgres://removed",
    "postgresql://removed",
    "libsql://removed",
    "sqld://removed",
    "https://removed-libsql.example",
];

fn product_bin(name: &str) -> PathBuf {
    if let Some(dir) = std::env::var_os("RIPCLONE_BIN_DIR") {
        return PathBuf::from(dir).join(name);
    }
    match name {
        "ripclone-server" => PathBuf::from(env!("CARGO_BIN_EXE_ripclone-server")),
        "ripclone-worker" => PathBuf::from(env!("CARGO_BIN_EXE_ripclone-worker")),
        _ => unreachable!(),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(path: &Path, selector: &str, value: &str, queue_db: &Path) {
    let body = match selector {
        "metadata" => format!(
            "[metadata]\nbackend = \"{value}\"\nurl = \"ignored\"\n\n[queue]\nbackend = \"local\"\n"
        ),
        "queue" => format!(
            "[metadata]\nbackend = \"file\"\n\n[queue]\nbackend = \"{value}\"\nurl = \"{}\"\n",
            queue_db.display()
        ),
        _ => unreachable!(),
    };
    std::fs::write(path, body).unwrap();
}

fn rejected_launch(
    binary: &str,
    selector: &str,
    source: &str,
    value: &str,
    root: &Path,
    queue_db: &Path,
    s3_port: u16,
) -> Output {
    let case = root.join(format!("{binary}-{selector}-{source}-{value}"));
    let cas = case.join("cas");
    let repos = case.join("repos");
    let cfg = case.join("config.toml");
    std::fs::create_dir_all(&case).unwrap();
    let config_value = if source == "env" {
        if selector == "metadata" {
            "file"
        } else {
            "sqlite"
        }
    } else {
        value
    };
    config(&cfg, selector, config_value, queue_db);
    let port = free_port();

    let mut cmd = Command::new(product_bin(binary));
    cmd.env_clear()
        .env("RIPCLONE_CONFIG", &cfg)
        .env(
            "RIPCLONE_S3_ENDPOINT",
            format!("http://127.0.0.1:{s3_port}"),
        )
        .env("RIPCLONE_S3_REGION", "us-east-1")
        .env("RIPCLONE_S3_BUCKET", "must-not-change")
        .env("AWS_ACCESS_KEY_ID", "sentinel")
        .env("AWS_SECRET_ACCESS_KEY", "sentinel")
        .env("RIPCLONE_QUEUE_DB_URL", queue_db)
        .env("RIPCLONE_METADATA_DB_URL", case.join("fallback-meta.db"));
    if source == "env" {
        let key = if selector == "metadata" {
            "RIPCLONE_METADATA"
        } else {
            "RIPCLONE_QUEUE"
        };
        cmd.env(key, value);
    }
    if binary == "ripclone-server" {
        cmd.args(["--host", "127.0.0.1", "--port", &port.to_string()])
            .arg("--cas-dir")
            .arg(&cas)
            .arg("--repo-root")
            .arg(&repos);
    } else {
        cmd.arg("--cas-dir")
            .arg(&cas)
            .arg("--repo-root")
            .arg(&repos);
    }
    let output = cmd.output().expect("launch rejected process");
    assert!(
        !output.status.success(),
        "{binary}/{selector}/{source}/{value}"
    );
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostic.contains("unsupported"), "{diagnostic}");
    assert!(diagnostic.contains(selector), "{diagnostic}");
    assert!(diagnostic.contains(value), "{diagnostic}");
    assert!(
        !cas.exists(),
        "rejected startup created fallback CAS: {}",
        cas.display()
    );
    assert!(
        !repos.exists(),
        "rejected startup created fallback refs: {}",
        repos.display()
    );
    assert!(
        !case.join("fallback-meta.db").exists(),
        "rejected startup opened fallback metadata"
    );
    assert!(
        TcpListener::bind(("127.0.0.1", port)).is_ok(),
        "rejected startup left a listener alive on {port}"
    );
    output
}

struct StaleHarness<'a> {
    root: &'a Path,
    queue_db: &'a Path,
    s3_port: u16,
}

fn rejected_stale_launch(
    binary: &str,
    selector: &str,
    source: &str,
    setting: &str,
    value: &str,
    harness: &StaleHarness<'_>,
) {
    let safe_value = value.replace([':', '/'], "-");
    let case = harness.root.join(format!(
        "stale-{binary}-{selector}-{source}-{setting}-{safe_value}"
    ));
    let cas = case.join("cas");
    let repos = case.join("repos");
    let cfg = case.join("config.toml");
    std::fs::create_dir_all(&case).unwrap();

    let mut metadata = "backend = \"file\"\n".to_string();
    let mut queue = if selector == "metadata" || setting == "token" {
        format!(
            "backend = \"sqlite\"\nurl = {:?}\n",
            harness.queue_db.display().to_string()
        )
    } else {
        "backend = \"local\"\n".to_string()
    };
    if source == "config" {
        let target = if selector == "metadata" {
            &mut metadata
        } else {
            &mut queue
        };
        target.push_str(&format!("{setting} = {value:?}\n"));
    }
    std::fs::write(&cfg, format!("[metadata]\n{metadata}\n[queue]\n{queue}")).unwrap();

    let port = free_port();
    let mut cmd = Command::new(product_bin(binary));
    cmd.env_clear()
        .env("RIPCLONE_CONFIG", &cfg)
        .env(
            "RIPCLONE_S3_ENDPOINT",
            format!("http://127.0.0.1:{}", harness.s3_port),
        )
        .env("RIPCLONE_S3_REGION", "us-east-1")
        .env("RIPCLONE_S3_BUCKET", "must-not-change")
        .env("AWS_ACCESS_KEY_ID", "sentinel")
        .env("AWS_SECRET_ACCESS_KEY", "sentinel");
    if source == "env" {
        let key = match (selector, setting) {
            ("metadata", "url") => "RIPCLONE_METADATA_DB_URL",
            ("queue", "url") => "RIPCLONE_QUEUE_DB_URL",
            ("metadata", "token") => "RIPCLONE_METADATA_DB_TOKEN",
            ("queue", "token") => "RIPCLONE_QUEUE_DB_TOKEN",
            _ => unreachable!(),
        };
        cmd.env(key, value);
    }
    if binary == "ripclone-server" {
        cmd.args(["--host", "127.0.0.1", "--port", &port.to_string()])
            .arg("--cas-dir")
            .arg(&cas)
            .arg("--repo-root")
            .arg(&repos);
    } else {
        cmd.arg("--cas-dir")
            .arg(&cas)
            .arg("--repo-root")
            .arg(&repos);
    }
    let output = cmd.output().expect("launch stale-config process");
    assert!(
        !output.status.success(),
        "{binary}/{selector}/{source}/{setting}/{value} started"
    );
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(
        diagnostic.to_ascii_lowercase().contains(setting),
        "missing {setting} diagnostic for {binary}/{selector}/{source}: {diagnostic}"
    );
    assert!(
        !cas.exists(),
        "stale startup created CAS: {}",
        cas.display()
    );
    assert!(
        !repos.exists(),
        "stale startup created refs: {}",
        repos.display()
    );
    assert!(
        TcpListener::bind(("127.0.0.1", port)).is_ok(),
        "stale startup left a listener alive on {port}"
    );
}

#[test]
fn supported_environment_values_override_removed_config_values() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path().join("config.toml");
    std::fs::write(
        &cfg,
        "[metadata]\nbackend = \"postgres\"\nurl = \"ignored\"\n\n[queue]\nbackend = \"mysql\"\nurl = \"ignored\"\n",
    )
    .unwrap();
    let output = Command::new(product_bin("ripclone-worker"))
        .env_clear()
        .env("RIPCLONE_CONFIG", &cfg)
        .env("RIPCLONE_METADATA", "sqlite")
        .env("RIPCLONE_METADATA_DB_URL", tmp.path().join("meta.db"))
        .env("RIPCLONE_QUEUE", "sqlite")
        .env("RIPCLONE_QUEUE_DB_URL", tmp.path().join("queue.db"))
        .env("RIPCLONE_IDLE_EXIT_SECS", "1")
        .arg("--idle-poll-ms")
        .arg("20")
        .arg("--cas-dir")
        .arg(tmp.path().join("cas"))
        .arg("--repo-root")
        .arg(tmp.path().join("repos"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "supported environment override failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(tmp.path().join("meta.db").exists());
    assert!(tmp.path().join("queue.db").exists());
}

#[tokio::test]
async fn removed_database_values_fail_before_any_side_effect() {
    let tmp = tempfile::tempdir().unwrap();
    let queue_path = tmp.path().join("sentinel-queue.db");
    let queue = Arc::new(
        SqlJobQueue::new(Box::new(
            SqliteDb::connect(queue_path.to_str().unwrap())
                .await
                .unwrap(),
        ))
        .await
        .unwrap(),
    );
    let enqueued = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("sentinel/job"),
            branch: "main".into(),
            initialization_attempt_id: None,
            rev: None,
            credential: None,
            recheck: 0,
            size_bytes: None,
        })
        .await
        .unwrap();
    let job_id = enqueued.job_id.unwrap();

    let s3 = TcpListener::bind("127.0.0.1:0").unwrap();
    s3.set_nonblocking(true).unwrap();
    let s3_port = s3.local_addr().unwrap().port();
    for binary in ["ripclone-server", "ripclone-worker"] {
        for selector in ["metadata", "queue"] {
            for source in ["env", "config"] {
                for value in REMOVED {
                    rejected_launch(
                        binary,
                        selector,
                        source,
                        value,
                        tmp.path(),
                        &queue_path,
                        s3_port,
                    );
                    assert!(
                        matches!(queue.job_status(job_id).await.unwrap(), JobState::Pending),
                        "{binary}/{selector}/{source}/{value} claimed or mutated sentinel job"
                    );
                }
            }
        }
    }

    assert!(
        matches!(s3.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
        "rejected startup contacted or mutated the sentinel S3 endpoint"
    );
    assert!(TcpStream::connect(("127.0.0.1", s3_port)).is_ok());
}

#[tokio::test]
async fn removed_database_urls_and_tokens_fail_before_any_side_effect() {
    let tmp = tempfile::tempdir().unwrap();
    let queue_path = tmp.path().join("stale-sentinel-queue.db");
    let queue = Arc::new(
        SqlJobQueue::new(Box::new(
            SqliteDb::connect(queue_path.to_str().unwrap())
                .await
                .unwrap(),
        ))
        .await
        .unwrap(),
    );
    let enqueued = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("sentinel/stale-job"),
            branch: "main".into(),
            initialization_attempt_id: None,
            rev: None,
            credential: None,
            recheck: 0,
            size_bytes: None,
        })
        .await
        .unwrap();
    let job_id = enqueued.job_id.unwrap();
    let s3 = TcpListener::bind("127.0.0.1:0").unwrap();
    s3.set_nonblocking(true).unwrap();
    let s3_port = s3.local_addr().unwrap().port();
    let harness = StaleHarness {
        root: tmp.path(),
        queue_db: &queue_path,
        s3_port,
    };

    for binary in ["ripclone-server", "ripclone-worker"] {
        for selector in ["metadata", "queue"] {
            for source in ["env", "config"] {
                for url in REMOVED_URLS {
                    rejected_stale_launch(binary, selector, source, "url", url, &harness);
                    assert!(
                        matches!(queue.job_status(job_id).await.unwrap(), JobState::Pending),
                        "{binary}/{selector}/{source}/url/{url} claimed or mutated sentinel job"
                    );
                }
                rejected_stale_launch(binary, selector, source, "token", "removed-token", &harness);
                assert!(
                    matches!(queue.job_status(job_id).await.unwrap(), JobState::Pending),
                    "{binary}/{selector}/{source}/token claimed or mutated sentinel job"
                );
            }
        }
    }

    assert!(
        matches!(s3.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
        "stale startup contacted or mutated the sentinel S3 endpoint"
    );
    assert!(TcpStream::connect(("127.0.0.1", s3_port)).is_ok());
}
