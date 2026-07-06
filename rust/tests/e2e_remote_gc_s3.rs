//! Real S3/Tigris end-to-end tests for remote GC and storage billing.
//!
//! These tests are ignored by default because they need credentials for an
//! S3-compatible store. Run them explicitly with:
//!
//!   RIPCLONE_S3_ENDPOINT=https://... RIPCLONE_S3_BUCKET=... \
//!     AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     cargo test --test e2e_remote_gc_s3 -- --ignored

mod common;

use anyhow::{Context, Result};
use common::*;
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use ripclone::ref_store::{CachingRefStore, RefStore, S3RefStore};
use ripclone::remote_gc::{GcConfig, RemoteGc};
use ripclone::server::run_server;
use ripclone::storage::{S3Storage, StorageBackend};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[derive(Clone)]
struct S3Env {
    endpoint: String,
    region: String,
    bucket: String,
}

/// Serializes server startup and env-var mutation across tests in this binary.
static SERVER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static PREFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

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
        region,
        bucket,
    })
}

fn unique_prefix() -> String {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    let seq = PREFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("e2e-remote-gc/{ns}-{pid}-{seq}/")
}

fn repo_suffix(prefix: &str) -> String {
    prefix
        .trim_start_matches("e2e-remote-gc/")
        .trim_end_matches('/')
        .to_string()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_server(port: u16) {
    for _ in 0..400 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("server on port {port} did not become ready");
}

async fn start_s3_server(env: &S3Env, prefix: &str) -> Server {
    start_s3_server_faulting(env, prefix, 0).await
}

/// Start the in-process server backed by the real S3-compatible store, failing
/// the first `fail_first` artifact GETs via `RIPCLONE_TEST_FAIL_FIRST_FETCHES`.
/// The fault threshold is set under `SERVER_LOCK` and removed once the server
/// has consumed it, so parallel test binaries cannot observe the same env var.
async fn start_s3_server_faulting(env: &S3Env, prefix: &str, fail_first: usize) -> Server {
    let _lock = SERVER_LOCK.lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_S3_ENDPOINT", &env.endpoint);
        std::env::set_var("RIPCLONE_S3_BUCKET", &env.bucket);
        std::env::set_var("RIPCLONE_S3_REGION", &env.region);
        std::env::set_var("RIPCLONE_S3_PREFIX", prefix);
        std::env::set_var("RIPCLONE_REMOTE_GC_INTERVAL_SECS", "0");
        std::env::set_var("RIPCLONE_RETENTION_INTERVAL_SECS", "999999");
        if fail_first > 0 {
            std::env::set_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES", fail_first.to_string());
        }
    }
    common::init(false);

    let dir = tempfile::tempdir().expect("server temp dir");
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    std::fs::create_dir_all(&cas_dir).unwrap();
    std::fs::create_dir_all(&repo_root).unwrap();
    unsafe {
        std::env::set_var("RIPCLONE_S3_CACHE_DIR", cas_dir.to_str().unwrap());
    }

    let port = free_port();
    let (cas_dir2, repo_root2) = (cas_dir.clone(), repo_root.clone());
    tokio::spawn(async move {
        let _ = run_server(&cas_dir2, &repo_root2, "127.0.0.1", port).await;
    });
    wait_for_server(port).await;

    if fail_first > 0 {
        unsafe {
            std::env::remove_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES");
        }
    }

    Server {
        url: format!("http://127.0.0.1:{port}"),
        cas_dir: cas_dir.clone(),
        storage_dir: cas_dir,
        repo_root,
        _dir: dir,
    }
}

fn make_s3_storage(env: &S3Env, prefix: &str) -> Result<Arc<S3Storage>> {
    let s3 = S3Storage::new(
        &env.endpoint,
        &env.region,
        &env.bucket,
        Some(prefix),
        s3::Auth::from_env().context("S3 auth from env")?,
        None,
    )
    .context("create S3 storage")?;
    Ok(Arc::new(s3))
}

fn make_s3_ref_store(storage: Arc<S3Storage>) -> Arc<dyn RefStore> {
    Arc::new(CachingRefStore::new(S3RefStore::new(storage)))
}

async fn cleanup_prefix(env: &S3Env, prefix: &str) -> Result<()> {
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
        for obj in output.contents {
            keys.push(obj.key);
        }
        if !output.is_truncated {
            break;
        }
        continuation = output.next_continuation_token;
        if continuation.is_none() {
            break;
        }
    }

    for chunk in keys.chunks(1000) {
        let chunk: Vec<String> = chunk.to_vec();
        client
            .objects()
            .delete_objects(&env.bucket)
            .objects(&chunk)
            .context("build cleanup delete batch")?
            .quiet(true)
            .send()
            .await
            .context("S3 cleanup delete_objects")?;
    }
    Ok(())
}

async fn cleanup_repo_refs(env: &S3Env, owner: &str, repo: &str) -> Result<()> {
    let repo_id = ripclone::provider::RepoId::github(format!("{owner}/{repo}"));
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
        for obj in output.contents {
            keys.push(obj.key);
        }
        if !output.is_truncated {
            break;
        }
        continuation = output.next_continuation_token;
        if continuation.is_none() {
            break;
        }
    }

    for chunk in keys.chunks(1000) {
        let chunk: Vec<String> = chunk.to_vec();
        client
            .objects()
            .delete_objects(&env.bucket)
            .objects(&chunk)
            .context("build cleanup ref delete batch")?
            .quiet(true)
            .send()
            .await
            .context("S3 cleanup ref delete_objects")?;
    }
    Ok(())
}

/// Ensures the S3 prefix (and optional ref JSON) are deleted even if a test
/// panics. Call `disable()` after an explicit successful cleanup to avoid
/// running twice.
struct CleanupGuard {
    env: S3Env,
    prefix: String,
    owner_repo: Option<(String, String)>,
    disabled: bool,
}

impl CleanupGuard {
    fn new(env: S3Env, prefix: String) -> Self {
        Self {
            env,
            prefix,
            owner_repo: None,
            disabled: false,
        }
    }

    fn track_repo(&mut self, owner: &str, repo: &str) {
        self.owner_repo = Some((owner.to_string(), repo.to_string()));
    }

    fn disable(&mut self) {
        self.disabled = true;
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.disabled {
            return;
        }
        let env = self.env.clone();
        let prefix = self.prefix.clone();
        let owner_repo = self.owner_repo.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("cleanup runtime");
            if let Err(e) = rt.block_on(cleanup_prefix(&env, &prefix)) {
                eprintln!("cleanup_prefix failed: {e:#}");
            }
            if let Some((owner, repo)) = owner_repo
                && let Err(e) = rt.block_on(cleanup_repo_refs(&env, &owner, &repo))
            {
                eprintln!("cleanup_repo_refs failed: {e:#}");
            }
        })
        .join()
        .ok();
    }
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Poll until `grace` has elapsed since `start`, timing out at 10 s past the
/// grace window. This replaces fixed sleeps with a bounded poll so tests don't
/// wait longer than necessary on fast backends.
async fn wait_for_grace_since(start: Instant, grace: Duration) {
    let deadline = start + grace + Duration::from_secs(10);
    while Instant::now() < start + grace && Instant::now() < deadline {
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        Instant::now() >= start + grace,
        "grace {grace:?} never elapsed since {start:?}"
    );
}

async fn get_status(
    server: &Server,
    owner: &str,
    repo: &str,
    query: Option<&str>,
) -> serde_json::Value {
    let mut url = format!("{}/v1/repos/github/{owner}/{repo}/status", server.url);
    if let Some(q) = query {
        url.push('?');
        url.push_str(q);
    }
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("status request");
    let status = resp.status();
    let body = resp.text().await.expect("status text");
    if !status.is_success() {
        eprintln!("status endpoint returned {status}: {body}");
    }
    assert!(status.is_success(), "status 2xx");
    serde_json::from_str(&body).expect("status json")
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn remote_gc_deletes_orphans_on_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcorphan-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "hello world\n")], "c1");
    origin.publish();

    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let storage = make_s3_storage(&env, &prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage.clone());
    let reachable_data = b"i-am-reachable";
    let reachable_hash = sha256_hex(reachable_data);
    storage
        .put(&reachable_hash, reachable_data)
        .expect("put reachable");
    let reachable_repo = RepoId::github(format!("acme/{repo}-gc-reachable"));
    let reachable_info = ripclone::RefInfo {
        commit: "reachable".to_string(),
        default_branch: "HEAD".to_string(),
        metadata_chunk: reachable_hash.clone(),
        ..Default::default()
    };
    ref_store
        .save(&reachable_repo, &reachable_info)
        .await
        .expect("save reachable ref");

    // Age the reachable object relative to the orphan we are about to inject.
    let reachable_at = Instant::now();
    wait_for_grace_since(reachable_at, Duration::from_secs(1)).await;

    let orphan_data = b"i-am-an-orphan";
    let orphan_hash = sha256_hex(orphan_data);
    storage.put(&orphan_hash, orphan_data).expect("put orphan");
    let orphan_at = Instant::now();

    // Make sure the orphan is older than the grace period we will use.
    wait_for_grace_since(orphan_at, Duration::from_secs(1)).await;

    let gc = RemoteGc::new(
        storage.clone(),
        ref_store,
        GcConfig {
            grace_period: Duration::from_secs(1),
            dry_run: false,
            ..Default::default()
        },
    );
    // First pass tombstones the orphan in the ledger; it is never deleted on the
    // pass that first sees it unreferenced.
    let first = gc.run().await.expect("remote gc first run");
    let tombstoned_at = Instant::now();
    assert_eq!(
        first.objects_deleted, 0,
        "first pass must only tombstone, got {first:?}"
    );
    assert!(
        storage.size(&orphan_hash).is_ok(),
        "orphan must survive the tombstoning pass"
    );

    // After the (1s) grace elapses, a second pass collects it.
    wait_for_grace_since(tombstoned_at, Duration::from_secs(1)).await;
    let report = gc.run().await.expect("remote gc second run");

    // The orphan plus every reachable CAS object were scanned.
    assert!(
        report.objects_scanned >= 2,
        "expected at least reachable + orphan, got {report:?}"
    );
    assert!(
        report.objects_deleted >= 1,
        "expected at least one orphan deleted, got {report:?}"
    );

    // Orphan is gone.
    assert!(
        storage.size(&orphan_hash).is_err(),
        "orphan should have been deleted"
    );

    assert!(
        storage.size(&reachable_hash).is_ok(),
        "reachable object should survive GC"
    );

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn remote_gc_dry_run_does_not_delete_on_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcdryrun-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "dry run\n")], "c1");
    origin.publish();

    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let storage = make_s3_storage(&env, &prefix).expect("storage");
    let orphan_data = b"dry-run-orphan";
    let orphan_hash = sha256_hex(orphan_data);
    storage.put(&orphan_hash, orphan_data).expect("put orphan");
    let orphan_at = Instant::now();

    // Make sure the orphan is older than the grace period we will use.
    wait_for_grace_since(orphan_at, Duration::from_secs(1)).await;

    let ref_store = make_s3_ref_store(storage.clone());
    let gc = RemoteGc::new(
        storage.clone(),
        ref_store,
        GcConfig {
            grace_period: Duration::from_secs(1),
            dry_run: true,
            ..Default::default()
        },
    );
    // First dry-run pass tombstones (would_delete=0); after grace a second pass
    // reports it as a would-delete candidate without removing it.
    let _ = gc.run().await.expect("remote gc dry run first");
    let tombstoned_at = Instant::now();
    wait_for_grace_since(tombstoned_at, Duration::from_secs(1)).await;
    let report = gc.run().await.expect("remote gc dry run second");
    assert!(
        report.objects_deleted >= 1,
        "dry-run should report at least one deletion, got {report:?}"
    );

    // The orphan must still be present.
    assert!(
        storage.size(&orphan_hash).is_ok(),
        "dry-run must not delete objects"
    );

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

/// Race: RemoteGc with grace=0 must not corrupt a clone that is stalled
/// mid-chunk by the fault hook. The clone either completes with a correct tree
/// or fails cleanly without leaving a partial target directory.
#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn remote_gc_during_faulting_clone_is_safe() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcrace-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    // Fail the first 2 artifact GETs; the clone retries within its default
    // budget and should recover. GC runs while the clone is stalled on those
    // retries.
    let server = start_s3_server_faulting(&env, &prefix, 2).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "gc race\n"), ("b.txt", "x\n")], "c1");
    origin.publish();

    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    // Start the clone on a faulting server and let it begin resolving/downloading.
    let client = server.client();
    let repo_path = format!("acme/{repo}");
    let clone_task = tokio::spawn(async move {
        let out = tempfile::tempdir().expect("clone temp dir");
        let target = out.path().join("clone");
        let result = client
            .install_repo_with_mode_at(
                &repo_path,
                "HEAD",
                None,
                &target,
                CloneMode::Files,
                Some("full"),
                None,
            )
            .await;
        (result, out, target)
    });

    // Yield briefly so the clone task is scheduled and begins hitting faults,
    // then run GC with the most aggressive grace possible while the clone is
    // mid-flight.
    sleep(Duration::from_millis(200)).await;

    let storage = make_s3_storage(&env, &prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage.clone());
    let gc = RemoteGc::new(
        storage.clone(),
        ref_store,
        GcConfig {
            grace_period: Duration::ZERO,
            dry_run: false,
        },
    );
    let report = gc.run().await.expect("remote gc run during clone");
    eprintln!("GC during clone: {report:?}");

    let (result, _out, target) = clone_task.await.expect("clone task joined");
    match result {
        Ok(_) => {
            assert!(target.exists(), "successful clone must materialize target");
            assert_eq!(
                std::fs::read_to_string(target.join("a.txt")).unwrap_or_default(),
                "gc race\n",
                "clone content must be intact"
            );
            assert_eq!(
                std::fs::read_to_string(target.join("b.txt")).unwrap_or_default(),
                "x\n",
                "clone content must be intact"
            );
        }
        Err(_) => {
            assert!(
                !target.exists(),
                "failed clone must not leave a partial tree at target"
            );
        }
    }

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn status_reports_bytes_from_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("billings3-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "bill me\n")], "c1");
    origin.publish();

    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let status = get_status(&server, "acme", &repo, None).await;
    assert_eq!(status["owner"], "acme");
    assert_eq!(status["repo"], repo);
    assert!(status["refs"][0]["bytes"].as_u64().unwrap() > 0);
    assert_eq!(
        status["refs"][0]["bytes"],
        status["refs"][0]["unique_bytes"]
    );
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    assert_eq!(status["total_bytes"], status["total_unique_bytes"]);
    assert!(!status["regions"].as_array().unwrap().is_empty());
    assert!(status["regions"][0]["unique_bytes"].as_u64().unwrap() > 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

async fn load_head_ref(env: &S3Env, prefix: &str, owner: &str, repo: &str) -> ripclone::RefInfo {
    let storage = make_s3_storage(env, prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage);
    ref_store
        .load_branch(&RepoId::github(format!("{owner}/{repo}")), "HEAD")
        .await
        .expect("load ref")
        .expect("ref exists")
}

async fn save_head_ref(
    env: &S3Env,
    prefix: &str,
    owner: &str,
    repo: &str,
    info: &ripclone::RefInfo,
) {
    let storage = make_s3_storage(env, prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage);
    ref_store
        .save_branch(&RepoId::github(format!("{owner}/{repo}")), "HEAD", info)
        .await
        .expect("save ref");
}

async fn run_gc(
    env: &S3Env,
    prefix: &str,
    warm_ttl: Duration,
    dry_run: bool,
) -> ripclone::remote_gc::GcReport {
    let storage = make_s3_storage(env, prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage.clone());
    let gc = RemoteGc::new(
        storage,
        ref_store,
        GcConfig {
            grace_period: Duration::from_secs(0),
            warm_ttl,
            dry_run,
        },
    );
    gc.run().await.expect("gc run")
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn warm_ttl_evicts_idle_ref_and_status_reports_cold() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcwarm-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "warm me\n")], "c1");
    origin.publish();
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(status["refs"][0]["warm"].as_bool().unwrap());

    let mut info = load_head_ref(&env, &prefix, "acme", &repo).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    info.last_accessed_at = Some(now.saturating_sub(86400));
    info.synced_at = Some(now.saturating_sub(86400));
    save_head_ref(&env, &prefix, "acme", &repo, &info).await;

    let report = run_gc(&env, &prefix, Duration::from_secs(1), false).await;
    assert!(
        report.objects_deleted > 0,
        "GC should delete idle artifacts"
    );

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(!status["refs"][0]["warm"].as_bool().unwrap());
    assert_eq!(status["refs"][0]["bytes"], 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn warm_ttl_keeps_pinned_ref() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcpin-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "pin me\n")], "c1");
    origin.publish();
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let mut info = load_head_ref(&env, &prefix, "acme", &repo).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    info.last_accessed_at = Some(now.saturating_sub(86400));
    info.synced_at = Some(now.saturating_sub(86400));
    info.warm_pinned = true;
    save_head_ref(&env, &prefix, "acme", &repo, &info).await;

    let report = run_gc(&env, &prefix, Duration::from_secs(1), false).await;
    assert_eq!(
        report.objects_deleted, 0,
        "pinned ref must not lose artifacts"
    );

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(status["refs"][0]["warm"].as_bool().unwrap());
    assert!(status["refs"][0]["pinned"].as_bool().unwrap());
    assert!(status["refs"][0]["bytes"].as_u64().unwrap() > 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn clone_after_eviction_rebuilds_cleanly() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcrebuild-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "rebuild me\n")], "c1");
    origin.publish();
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let mut info = load_head_ref(&env, &prefix, "acme", &repo).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    info.last_accessed_at = Some(now.saturating_sub(86400));
    info.synced_at = Some(now.saturating_sub(86400));
    save_head_ref(&env, &prefix, "acme", &repo, &info).await;

    run_gc(&env, &prefix, Duration::from_secs(1), false).await;

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(!status["refs"][0]["warm"].as_bool().unwrap());

    // Plain clone after eviction: no pre-sync. The first ref resolve returns
    // 202, enqueues a rebuild, and the client polls until the rebuild is warm.
    let (_dir, target) = clone_only(
        &server,
        "acme",
        &repo,
        0,
        ripclone::mode::CloneMode::Editable,
    )
    .await
    .expect("clone after eviction");
    assert_eq!(read(&target, "a.txt"), "rebuild me\n");

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(status["refs"][0]["warm"].as_bool().unwrap());

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn public_fork_status_is_free_on_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("forks3-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "fork me\n")], "c1");
    origin.publish();

    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let status = get_status(
        &server,
        "acme",
        &repo,
        Some("public=true&fork_of=upstream/repo"),
    )
    .await;
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    assert_eq!(status["total_unique_bytes"], 0);
    assert_eq!(status["refs"][0]["unique_bytes"], 0);
    assert_eq!(status["regions"][0]["unique_bytes"], 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}
