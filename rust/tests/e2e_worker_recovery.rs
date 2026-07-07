//! End-to-end worker crash recovery across durable queue/metadata/storage
//! combinations.
//!
//! These tests kill a real `ripclone-worker` process after it has claimed a
//! build but before it can ack. They fail if stale claims are not reclaimed, if a
//! late/stale worker result can win, or if the recovered ref points at missing
//! artifacts instead of cloneable bytes.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DELAY_MS: &str = "30000";

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn new(keys: &[&'static str]) -> Self {
        Self {
            saved: keys.iter().map(|&k| (k, std::env::var(k).ok())).collect(),
        }
    }

    fn set(&self, key: &'static str, value: impl AsRef<str>) {
        unsafe { std::env::set_var(key, value.as_ref()) };
    }

    fn remove(&self, key: &'static str) {
        unsafe { std::env::remove_var(key) };
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.drain(..) {
            match value {
                Some(v) => unsafe { std::env::set_var(key, v) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

fn recovery_env_keys() -> Vec<&'static str> {
    vec![
        "RIPCLONE_QUEUE",
        "RIPCLONE_QUEUE_DB_URL",
        "RIPCLONE_QUEUE_DB_TOKEN",
        "RIPCLONE_QUEUE_STALE_SECS",
        "RIPCLONE_QUEUE_MAX_ATTEMPTS",
        "RIPCLONE_SYNC_MAX_ATTEMPTS",
        "RIPCLONE_METADATA",
        "RIPCLONE_METADATA_DB_URL",
        "RIPCLONE_METADATA_DB_TOKEN",
        "RIPCLONE_TEST_ARCHIVE_DELAY_MS",
        "RIPCLONE_S3_ENDPOINT",
        "RIPCLONE_S3_REGION",
        "RIPCLONE_S3_BUCKET",
        "RIPCLONE_S3_PREFIX",
        "RIPCLONE_S3_CACHE_DIR",
        "RIPCLONE_REMOTE_GC_INTERVAL_SECS",
        "RIPCLONE_RETENTION_INTERVAL_SECS",
        "RIPCLONE_REF_CACHE_TTL_SECS",
    ]
}

async fn sqlite_pool(path: &str) -> SqlitePool {
    let opts = SqliteConnectOptions::from_str(path)
        .expect("parse sqlite path")
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(5));
    SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("open queue sqlite db")
}

async fn sqlite_job_status(pool: &SqlitePool) -> Option<(String, i64)> {
    let row = sqlx::query("SELECT status, attempts FROM jobs ORDER BY id LIMIT 1")
        .fetch_optional(pool)
        .await
        .expect("query sqlite job");
    row.map(|r| (r.get::<String, _>(0), r.get::<i64, _>(1)))
}

async fn wait_sqlite_claimed(pool: &SqlitePool) {
    for _ in 0..200 {
        if let Some((status, attempts)) = sqlite_job_status(pool).await
            && status == "claimed"
            && attempts == 1
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker never claimed sqlite job");
}

async fn wait_sqlite_done_after_reclaim(pool: &SqlitePool) {
    for _ in 0..240 {
        if let Some((status, attempts)) = sqlite_job_status(pool).await
            && status == "done"
            && attempts >= 2
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "sqlite job did not finish after reclaim; last={:?}",
        sqlite_job_status(pool).await
    );
}

async fn recover_after_killed_worker_with_sqlite_queue(
    queue_db: &str,
    server: &Server,
    repo: &str,
    want: &str,
) {
    let pool = sqlite_pool(queue_db).await;
    let worker1 = spawn_worker(&server.cas_dir, &server.repo_root);
    let client = server.client();
    let repo_path = format!("acme/{repo}");
    let sync_task = tokio::spawn(async move { client.sync_repo(&repo_path, None).await });

    wait_sqlite_claimed(&pool).await;
    worker1.kill_and_wait();

    unsafe { std::env::remove_var("RIPCLONE_TEST_ARCHIVE_DELAY_MS") };
    let _worker2 = spawn_worker(&server.cas_dir, &server.repo_root);

    let resp = sync_task
        .await
        .expect("sync task joined")
        .expect("replacement worker should finish the reclaimed job");
    assert_eq!(resp.commit, want, "sync returns the recovered commit");
    wait_sqlite_done_after_reclaim(&pool).await;

    let (_g, c) = wait_repo_cloneable(server, "acme", repo, "1").await;
    assert_eq!(read(&c, "a.txt"), "recovered\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    let (_fg, files) = clone_only(server, "acme", repo, 0, CloneMode::Files)
        .await
        .expect("files clone after recovered archive build");
    assert_eq!(read(&files, "a.txt"), "recovered\n");
}

#[tokio::test]
async fn worker_kill_mid_build_reclaims_sqlite_queue_sqlite_metadata() {
    let _lock = ENV_LOCK.lock().await;
    let keys = recovery_env_keys();
    let env = EnvGuard::new(&keys);

    let dir = tempfile::tempdir().expect("recovery db dir");
    let queue_db = dir.path().join("queue.db").to_string_lossy().to_string();
    let meta_db = dir.path().join("meta.db").to_string_lossy().to_string();
    env.set("RIPCLONE_QUEUE", "sqlite");
    env.set("RIPCLONE_QUEUE_DB_URL", &queue_db);
    env.set("RIPCLONE_QUEUE_STALE_SECS", "1");
    env.set("RIPCLONE_QUEUE_MAX_ATTEMPTS", "4");
    env.set("RIPCLONE_SYNC_MAX_ATTEMPTS", "40");
    env.set("RIPCLONE_METADATA", "sqlite");
    env.set("RIPCLONE_METADATA_DB_URL", &meta_db);
    env.set("RIPCLONE_TEST_ARCHIVE_DELAY_MS", DELAY_MS);
    init(false);

    let server = start_server().await;
    let origin = make_origin("acme", "recover-sqlite");
    let want = origin.commit(&[("a.txt", "recovered\n")], "c1");
    origin.publish();

    recover_after_killed_worker_with_sqlite_queue(&queue_db, &server, "recover-sqlite", &want)
        .await;
}

fn sqld_available() -> bool {
    Command::new("sqld")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

struct Proc(Child);
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn start_sqld(port: u16, data: &Path) -> Proc {
    let child = Command::new("sqld")
        .arg("--http-listen-addr")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--db-path")
        .arg(data)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sqld");
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Proc(child);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("sqld did not become ready on port {port}");
}

async fn libsql_job_status(url: &str) -> Option<(String, i64)> {
    let db = libsql::Builder::new_remote(url.to_string(), "dev".to_string())
        .build()
        .await
        .expect("open libsql probe");
    let conn = db.connect().expect("connect libsql probe");
    let mut rows = conn
        .query("SELECT status, attempts FROM jobs ORDER BY id LIMIT 1", ())
        .await
        .expect("query libsql job");
    rows.next().await.expect("read libsql row").map(|r| {
        (
            r.get::<String>(0).expect("status"),
            r.get::<i64>(1).expect("attempts"),
        )
    })
}

async fn wait_libsql_claimed(url: &str) {
    for _ in 0..200 {
        if let Some((status, attempts)) = libsql_job_status(url).await
            && status == "claimed"
            && attempts == 1
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker never claimed libsql job");
}

async fn wait_libsql_done_after_reclaim(url: &str) {
    for _ in 0..240 {
        if let Some((status, attempts)) = libsql_job_status(url).await
            && status == "done"
            && attempts >= 2
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "libsql job did not finish after reclaim; last={:?}",
        libsql_job_status(url).await
    );
}

#[tokio::test]
async fn worker_kill_mid_build_reclaims_libsql_queue_and_metadata() {
    let _lock = ENV_LOCK.lock().await;
    if !sqld_available() {
        eprintln!("SKIP: sqld not installed; install it to run libsql recovery e2e");
        return;
    }
    let keys = recovery_env_keys();
    let env = EnvGuard::new(&keys);

    let data = tempfile::tempdir().expect("sqld data dir");
    let port = free_port();
    let _sqld = start_sqld(port, data.path());
    let url = format!("http://127.0.0.1:{port}");

    env.set("RIPCLONE_QUEUE", "libsql");
    env.set("RIPCLONE_QUEUE_DB_URL", &url);
    env.set("RIPCLONE_QUEUE_DB_TOKEN", "dev");
    env.set("RIPCLONE_QUEUE_STALE_SECS", "1");
    env.set("RIPCLONE_QUEUE_MAX_ATTEMPTS", "4");
    env.set("RIPCLONE_SYNC_MAX_ATTEMPTS", "40");
    env.set("RIPCLONE_METADATA", "libsql");
    env.set("RIPCLONE_METADATA_DB_URL", &url);
    env.set("RIPCLONE_METADATA_DB_TOKEN", "dev");
    env.set("RIPCLONE_TEST_ARCHIVE_DELAY_MS", DELAY_MS);
    init(false);

    let server = start_server().await;
    let origin = make_origin("acme", "recover-libsql");
    let want = origin.commit(&[("a.txt", "recovered\n")], "c1");
    origin.publish();

    let worker1 = spawn_worker(&server.cas_dir, &server.repo_root);
    let client = server.client();
    let sync_task =
        tokio::spawn(async move { client.sync_repo("acme/recover-libsql", None).await });

    wait_libsql_claimed(&url).await;
    worker1.kill_and_wait();

    env.remove("RIPCLONE_TEST_ARCHIVE_DELAY_MS");
    let _worker2 = spawn_worker(&server.cas_dir, &server.repo_root);

    let resp = sync_task
        .await
        .expect("sync task joined")
        .expect("replacement libsql worker should finish the reclaimed job");
    assert_eq!(resp.commit, want);
    wait_libsql_done_after_reclaim(&url).await;

    let (_g, c) = wait_repo_cloneable(&server, "acme", "recover-libsql", "1").await;
    assert_eq!(read(&c, "a.txt"), "recovered\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
}

fn s3_env() -> Option<(String, String, String)> {
    let endpoint = std::env::var("RIPCLONE_S3_ENDPOINT")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::env::var("AWS_ENDPOINT_URL_S3")
                .ok()
                .filter(|s| !s.is_empty())
        })?;
    let bucket = std::env::var("RIPCLONE_S3_BUCKET")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("BUCKET_NAME").ok().filter(|s| !s.is_empty()))?;
    let region = std::env::var("RIPCLONE_S3_REGION")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("AWS_REGION").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "us-east-1".to_string());
    Some((endpoint, bucket, region))
}

fn unique_s3_prefix() -> String {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("e2e-worker-recovery/{ns}-{}/", std::process::id())
}

#[tokio::test]
async fn worker_kill_mid_build_reclaims_s3_storage_and_metadata() {
    let _lock = ENV_LOCK.lock().await;
    let Some((endpoint, bucket, region)) = s3_env() else {
        eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
        return;
    };
    let keys = recovery_env_keys();
    let env = EnvGuard::new(&keys);

    let dir = tempfile::tempdir().expect("s3 recovery temp");
    let queue_db = dir.path().join("queue.db").to_string_lossy().to_string();
    let cache_dir: PathBuf = dir.path().join("s3-cache");
    let prefix = unique_s3_prefix();
    env.set("RIPCLONE_QUEUE", "sqlite");
    env.set("RIPCLONE_QUEUE_DB_URL", &queue_db);
    env.set("RIPCLONE_QUEUE_STALE_SECS", "1");
    env.set("RIPCLONE_QUEUE_MAX_ATTEMPTS", "4");
    env.set("RIPCLONE_SYNC_MAX_ATTEMPTS", "40");
    env.set("RIPCLONE_METADATA", "s3");
    env.set("RIPCLONE_S3_ENDPOINT", &endpoint);
    env.set("RIPCLONE_S3_BUCKET", &bucket);
    env.set("RIPCLONE_S3_REGION", &region);
    env.set("RIPCLONE_S3_PREFIX", &prefix);
    env.set("RIPCLONE_S3_CACHE_DIR", cache_dir.to_string_lossy());
    env.set("RIPCLONE_REMOTE_GC_INTERVAL_SECS", "0");
    env.set("RIPCLONE_RETENTION_INTERVAL_SECS", "999999");
    env.set("RIPCLONE_REF_CACHE_TTL_SECS", "0");
    env.set("RIPCLONE_TEST_ARCHIVE_DELAY_MS", DELAY_MS);
    init(false);

    let server = start_server().await;
    let origin = make_origin("acme", "recover-s3");
    let want = origin.commit(&[("a.txt", "recovered\n")], "c1");
    origin.publish();

    recover_after_killed_worker_with_sqlite_queue(&queue_db, &server, "recover-s3", &want).await;
}
