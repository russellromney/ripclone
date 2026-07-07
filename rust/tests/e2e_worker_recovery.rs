//! End-to-end worker crash recovery across durable queue/metadata/storage
//! combinations.
//!
//! These tests kill a real `ripclone-worker` process after it has claimed a
//! build but before it can ack. They fail if stale claims are not reclaimed, if a
//! late/stale worker result can win, or if the recovered ref points at missing
//! artifacts instead of cloneable bytes.

mod common;

use anyhow::{Context, Result};
use common::*;
use ripclone::provider::RepoId;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DELAY_MS: &str = "60000";

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
        "RIPCLONE_TEST_SYNC_MAX_ATTEMPTS",
        "RIPCLONE_SYNC_WAIT_SECS",
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

async fn wait_archive_building(server: &Server, repo: &str, commit: &str) {
    let url = format!("{}/v1/repos/github/acme/{repo}/status", server.url);
    let client = reqwest::Client::new();
    let mut last = String::new();
    for _ in 0..600 {
        let resp = client
            .get(&url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .send()
            .await
            .expect("status request");
        let status = resp.status();
        let text = resp.text().await.expect("status body");
        last = format!("{status} {text}");
        if status.is_success() {
            let body: serde_json::Value = serde_json::from_str(&text).expect("status json");
            if body["refs"].as_array().is_some_and(|refs| {
                refs.iter().any(|r| {
                    r["branch"] != "HEAD"
                        && r["commit"] == commit
                        && r["build_status"] == "archive building"
                        && r["warm"] == true
                        && r["manifest"].as_str().is_some_and(|m| !m.is_empty())
                })
            }) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("repo never reached observable archive-building state before worker kill: {last}");
}

async fn wait_archive_settled(server: &Server, repo: &str, commit: &str) {
    let url = format!("{}/v1/repos/github/acme/{repo}/status", server.url);
    let client = reqwest::Client::new();
    let mut last = String::new();
    for _ in 0..360 {
        let resp = client
            .get(&url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .send()
            .await
            .expect("status request");
        let status = resp.status();
        let text = resp.text().await.expect("status body");
        last = format!("{status} {text}");
        if status.is_success() {
            let body: serde_json::Value = serde_json::from_str(&text).expect("status json");
            if body["refs"].as_array().is_some_and(|refs| {
                refs.iter().any(|r| {
                    r["branch"] != "HEAD"
                        && r["commit"] == commit
                        && (r["build_status"].is_null() || r["build_status"] == "done")
                        && r["warm"] == true
                        && r["manifest"].as_str().is_some_and(|m| !m.is_empty())
                })
            }) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("reclaimed worker job did not settle archive build for {repo}@{commit}: {last}");
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
    wait_archive_building(server, repo, want).await;
    worker1.kill_and_wait();

    unsafe { std::env::remove_var("RIPCLONE_TEST_ARCHIVE_DELAY_MS") };
    let _worker2 = spawn_worker(&server.cas_dir, &server.repo_root);

    let resp = tokio::time::timeout(Duration::from_secs(180), sync_task)
        .await
        .expect("replacement worker did not wake the sync waiter after reclaim")
        .expect("sync task joined")
        .expect("replacement worker should finish the reclaimed job");
    assert_eq!(resp.commit, want, "sync returns the recovered commit");
    wait_sqlite_done_after_reclaim(&pool).await;

    let (_g, c) = wait_repo_cloneable(server, "acme", repo, "1").await;
    assert_eq!(read(&c, "a.txt"), "recovered\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    wait_archive_settled(server, repo, want).await;
    let (_fg, files) = clone_only(server, "acme", repo, 0, ripclone::mode::CloneMode::Files)
        .await
        .expect("files clone after recovered archive build");
    assert_eq!(read(&files, "a.txt"), "recovered\n");
}

#[tokio::test]
async fn worker_kill_mid_build_reclaims_sqlite_queue_sqlite_metadata() {
    // Fails if a killed worker can leave a claimed sqlite job unreclaimed, if a
    // replacement worker cannot finish the same commit, or if the recovered ref
    // points at missing/corrupt artifacts.
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
    env.set("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "40");
    env.set("RIPCLONE_SYNC_WAIT_SECS", "120");
    env.set("RIPCLONE_METADATA", "sqlite");
    env.set("RIPCLONE_METADATA_DB_URL", &meta_db);
    env.set("RIPCLONE_TEST_ARCHIVE_DELAY_MS", DELAY_MS);
    init(false);

    let server = start_server().await;
    let origin = make_origin("acme", "recover-sqlite");
    let want = origin.commit(&[("a.txt", "recovered\n")], "c1");
    origin.publish();

    register_added_without_build(&server, "acme/recover-sqlite")
        .await
        .expect("add repo");
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
    let proc = Proc(
        Command::new("sqld")
            .arg("--http-listen-addr")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--db-path")
            .arg(data)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sqld"),
    );
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return proc;
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
    // Fails if libsql-backed queue claims are not reclaimed after a worker dies
    // during the observable archive-build phase, or if libsql metadata publishes
    // a recovered ref whose bytes are not cloneable.
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
    env.set("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "40");
    env.set("RIPCLONE_SYNC_WAIT_SECS", "120");
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
    wait_archive_building(&server, "recover-libsql", &want).await;
    worker1.kill_and_wait();

    env.remove("RIPCLONE_TEST_ARCHIVE_DELAY_MS");
    let _worker2 = spawn_worker(&server.cas_dir, &server.repo_root);

    let resp = tokio::time::timeout(Duration::from_secs(180), sync_task)
        .await
        .expect("replacement libsql worker did not wake the sync waiter after reclaim")
        .expect("sync task joined")
        .expect("replacement libsql worker should finish the reclaimed job");
    assert_eq!(resp.commit, want);
    wait_libsql_done_after_reclaim(&url).await;

    let (_g, c) = wait_repo_cloneable(&server, "acme", "recover-libsql", "1").await;
    assert_eq!(read(&c, "a.txt"), "recovered\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    wait_archive_settled(&server, "recover-libsql", &want).await;
    let (_fg, files) = clone_only(
        &server,
        "acme",
        "recover-libsql",
        0,
        ripclone::mode::CloneMode::Files,
    )
    .await
    .expect("files clone after recovered libsql archive build");
    assert_eq!(read(&files, "a.txt"), "recovered\n");
}

#[derive(Clone)]
struct S3Env {
    endpoint: String,
    bucket: String,
    region: String,
}

fn s3_env() -> Option<S3Env> {
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
    Some(S3Env {
        endpoint,
        bucket,
        region,
    })
}

fn unique_s3_prefix() -> String {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("e2e-worker-recovery/{ns}-{}/", std::process::id())
}

async fn cleanup_s3_prefix(env: &S3Env, prefix: &str) -> Result<()> {
    let client = s3::Client::builder(&env.endpoint)
        .context("create S3 cleanup builder")?
        .region(&env.region)
        .auth(s3::Auth::from_env().context("S3 auth for cleanup")?)
        .build()
        .context("build cleanup S3 client")?;

    let mut keys = Vec::new();
    let mut continuation = None::<String>;
    loop {
        let mut req = client
            .objects()
            .list_v2(&env.bucket)
            .prefix(prefix)
            .context("set cleanup list prefix")?;
        if let Some(token) = continuation.take() {
            req = req
                .continuation_token(token)
                .context("set cleanup continuation token")?;
        }
        let output = req.send().await.context("S3 list for cleanup")?;
        keys.extend(output.contents.into_iter().map(|obj| obj.key));
        if !output.is_truncated {
            break;
        }
        continuation = output.next_continuation_token;
        if continuation.is_none() {
            break;
        }
    }

    for chunk in keys.chunks(1000) {
        client
            .objects()
            .delete_objects(&env.bucket)
            .objects(chunk.to_vec())
            .context("build cleanup delete batch")?
            .quiet(true)
            .send()
            .await
            .context("S3 cleanup delete_objects")?;
    }
    Ok(())
}

async fn cleanup_s3_repo_refs(env: &S3Env, owner: &str, repo: &str) -> Result<()> {
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    let storage_key = repo_id.storage_key();
    let client = s3::Client::builder(&env.endpoint)
        .context("create S3 cleanup builder")?
        .region(&env.region)
        .auth(s3::Auth::from_env().context("S3 auth for cleanup")?)
        .build()
        .context("build cleanup S3 client")?;

    let head_key = format!("refs/{storage_key}.json");
    let branch_prefix = format!("refs/{storage_key}/");
    let mut keys = vec![head_key];
    let mut continuation = None::<String>;
    loop {
        let mut req = client
            .objects()
            .list_v2(&env.bucket)
            .prefix(&branch_prefix)
            .context("set cleanup ref list prefix")?;
        if let Some(token) = continuation.take() {
            req = req
                .continuation_token(token)
                .context("set cleanup ref continuation token")?;
        }
        let output = req.send().await.context("S3 list refs for cleanup")?;
        keys.extend(output.contents.into_iter().map(|obj| obj.key));
        if !output.is_truncated {
            break;
        }
        continuation = output.next_continuation_token;
        if continuation.is_none() {
            break;
        }
    }

    for chunk in keys.chunks(1000) {
        client
            .objects()
            .delete_objects(&env.bucket)
            .objects(chunk.to_vec())
            .context("build cleanup ref delete batch")?
            .quiet(true)
            .send()
            .await
            .context("S3 cleanup ref delete_objects")?;
    }
    Ok(())
}

struct S3CleanupGuard {
    env: S3Env,
    prefix: String,
    owner_repo: Option<(String, String)>,
}

impl S3CleanupGuard {
    fn new(env: S3Env, prefix: String) -> Self {
        Self {
            env,
            prefix,
            owner_repo: None,
        }
    }

    fn track_repo(&mut self, owner: &str, repo: &str) {
        self.owner_repo = Some((owner.to_string(), repo.to_string()));
    }
}

impl Drop for S3CleanupGuard {
    fn drop(&mut self) {
        let env = self.env.clone();
        let prefix = self.prefix.clone();
        let owner_repo = self.owner_repo.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("cleanup runtime");
            if let Err(e) = rt.block_on(cleanup_s3_prefix(&env, &prefix)) {
                eprintln!("cleanup_s3_prefix failed: {e:#}");
            }
            if let Some((owner, repo)) = owner_repo
                && let Err(e) = rt.block_on(cleanup_s3_repo_refs(&env, &owner, &repo))
            {
                eprintln!("cleanup_s3_repo_refs failed: {e:#}");
            }
        })
        .join()
        .ok();
    }
}

#[tokio::test]
async fn worker_kill_mid_build_reclaims_s3_storage_and_metadata() {
    // Fails if S3-backed metadata/storage publishes a half-written ref after a
    // worker dies during archive build, if the replacement worker cannot reclaim
    // the sqlite queue job, or if the recovered clone bytes are corrupt.
    let _lock = ENV_LOCK.lock().await;
    let Some(s3) = s3_env() else {
        eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
        return;
    };
    let keys = recovery_env_keys();
    let env = EnvGuard::new(&keys);

    let dir = tempfile::tempdir().expect("s3 recovery temp");
    let queue_db = dir.path().join("queue.db").to_string_lossy().to_string();
    let cache_dir: PathBuf = dir.path().join("s3-cache");
    let prefix = unique_s3_prefix();
    let mut cleanup = S3CleanupGuard::new(s3.clone(), prefix.clone());
    cleanup.track_repo("acme", "recover-s3");
    env.set("RIPCLONE_QUEUE", "sqlite");
    env.set("RIPCLONE_QUEUE_DB_URL", &queue_db);
    env.set("RIPCLONE_QUEUE_STALE_SECS", "1");
    env.set("RIPCLONE_QUEUE_MAX_ATTEMPTS", "4");
    env.set("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "40");
    env.set("RIPCLONE_SYNC_WAIT_SECS", "120");
    env.set("RIPCLONE_METADATA", "s3");
    env.set("RIPCLONE_S3_ENDPOINT", &s3.endpoint);
    env.set("RIPCLONE_S3_BUCKET", &s3.bucket);
    env.set("RIPCLONE_S3_REGION", &s3.region);
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
