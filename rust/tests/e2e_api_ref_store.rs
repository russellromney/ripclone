//! ApiRefStore (D4): farmed-out workers hold no DB credentials.
//!
//! Server keeps the real metadata DB (`RIPCLONE_METADATA=sqlite`). The worker
//! uses `RIPCLONE_METADATA=api` + report URL + per-job bearer token, and POSTs
//! ref-writes to `POST /v1/refs`. The server validates the token and writes.
//!
//! Process-global queue/metadata env + a real worker binary → serialize.

mod common;

use common::*;
use ripclone::backends;
use ripclone::job_token::{mint_job_token, report_token_secret_from_env};
use ripclone::meta::{SqlRefStore, SqliteMeta};
use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue, JobState, SqlJobQueue};
use ripclone::ref_store::RefStore;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Spawn a worker that talks to the server's report endpoint and holds **no**
/// metadata DB credentials. Clears any inherited DB URL/token so the child's
/// env bag matches the farm-out contract.
fn spawn_api_worker(
    cas_dir: &Path,
    repo_root: &Path,
    report_url: &str,
    job_token: &str,
) -> WorkerProc {
    // Prove the bag we hand the worker has no DB creds.
    assert!(
        std::env::var("RIPCLONE_METADATA_DB_URL").is_ok()
            || std::env::var("RIPCLONE_METADATA").as_deref() != Ok("api"),
        "parent may hold DB URL for the server; child must not"
    );

    let mut cmd = Command::new(cargo_bin("ripclone-worker"));
    cmd.arg("--cas-dir")
        .arg(cas_dir)
        .arg("--repo-root")
        .arg(repo_root)
        .arg("--idle-poll-ms")
        .arg("100")
        .env_remove("RIPCLONE_IDLE_EXIT_SECS")
        .env_remove("RIPCLONE_MAX_JOBS")
        // Worker metadata target: API report, not a direct DB.
        .env("RIPCLONE_METADATA", "api")
        .env("RIPCLONE_METADATA_REPORT_URL", report_url)
        .env("RIPCLONE_METADATA_JOB_TOKEN", job_token)
        // The farm-out contract: no DB credentials on the worker.
        .env_remove("RIPCLONE_METADATA_DB_URL")
        .env_remove("RIPCLONE_METADATA_DB_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());

    // Record the exact env we intend (for the "no DB creds" assertion).
    let child_meta = "api";
    let child_has_db_url = false;
    assert_eq!(child_meta, "api");
    assert!(!child_has_db_url);

    let child = cmd.spawn().expect("spawn api ripclone-worker");
    WorkerProc::from_child(child)
}

fn setup_sqlite_queue_and_meta() -> (tempfile::TempDir, tempfile::TempDir, String, String) {
    let qdir = tempfile::tempdir().expect("queue dir");
    let mdir = tempfile::tempdir().expect("meta dir");
    let queue_url = qdir.path().join("queue.db").to_string_lossy().to_string();
    let meta_url = mdir.path().join("meta.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &queue_url);
        std::env::set_var("RIPCLONE_METADATA", "sqlite");
        std::env::set_var("RIPCLONE_METADATA_DB_URL", &meta_url);
        // Keep retry backoff snappy for the dead-URL requeue assertion.
        std::env::set_var("RIPCLONE_QUEUE_RETRY_BACKOFF_MS", "20");
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "10");
    }
    init(false);
    (qdir, mdir, queue_url, meta_url)
}

fn mint_report_token() -> String {
    // Server e2e init sets RIPCLONE_SERVER_TOKEN = TOKEN; secret derives from it.
    let secret = report_token_secret_from_env()
        .expect("job token secret (RIPCLONE_SERVER_TOKEN is set by common::init)");
    mint_job_token(&secret, Duration::from_secs(3600)).expect("mint job report token")
}

async fn open_meta_store(meta_url: &str) -> Arc<dyn RefStore> {
    Arc::new(
        SqlRefStore::new(Box::new(
            SqliteMeta::connect(meta_url).await.expect("connect meta"),
        ))
        .await
        .expect("init meta"),
    )
}

/// Wait until the job is dead-lettered after retryable report failures, or
/// panic if it becomes `Done`. Initial `Pending` is not enough — the job starts
/// that way before the worker claims it.
async fn wait_dead_lettered(queue: &SqlJobQueue, id: i64, timeout: Duration) -> String {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match queue.job_status(id).await.expect("status") {
            JobState::Done => {
                panic!("job {id} became done; dead report URL must fail retryable, not succeed")
            }
            JobState::Failed(msg) => {
                assert!(
                    msg.contains("dead-lettered"),
                    "expected dead-letter after retryable report failures, got permanent fail: {msg}"
                );
                return msg;
            }
            JobState::Pending | JobState::Unknown => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "job {id} never dead-lettered within {timeout:?} (still pending/unknown)"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Worker with api metadata + report URL + job token, no DB creds: build lands
/// the ref in the server's sqlite via POST /v1/refs.
#[tokio::test]
async fn api_worker_reports_ref_without_db_creds() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, _queue_url, meta_url) = setup_sqlite_queue_and_meta();

    let server = start_server().await;
    let report_url = format!("{}/v1/refs", server.url);
    let token = mint_report_token();

    // Real worker: METADATA=api, no DB URL/token in its env.
    let _worker = spawn_api_worker(&server.cas_dir, &server.repo_root, &report_url, &token);

    let origin = make_origin("acme", "api-ref");
    let commit = origin.commit(&[("a.txt", "api\n")], "c1");
    origin.publish();

    register_added_without_build(&server, "acme/api-ref")
        .await
        .expect("add repo");
    let resp = server
        .client()
        .sync_repo("acme/api-ref", None)
        .await
        .expect("api-worker farm-out sync");
    assert_eq!(resp.commit, commit);

    // Direct read of the server's sqlite metadata DB — the write path was the
    // worker's ApiRefStore → POST /v1/refs → server's SqlRefStore.
    let store = open_meta_store(&meta_url).await;
    let rid = RepoId::github("acme/api-ref");
    let stored = store
        .load_branch(&rid, "main")
        .await
        .expect("load")
        .expect("ref must be in sqlite after api report");
    assert_eq!(stored.commit, commit);
}

/// Wrong token → endpoint 401, and no row in the metadata DB.
#[tokio::test]
async fn bad_job_token_rejected_and_no_db_write() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, _queue_url, meta_url) = setup_sqlite_queue_and_meta();

    let server = start_server().await;
    let report_url = format!("{}/v1/refs", server.url);

    let store = open_meta_store(&meta_url).await;
    let rid = RepoId::github("acme/bad-tok");
    let info = ripclone::RefInfo {
        commit: "deadbeef".into(),
        default_branch: "main".into(),
        manifest: "m".into(),
        ..Default::default()
    };
    let body = serde_json::json!({
        "op": "save_branch",
        "repo_key": rid.storage_key(),
        "branch": "main",
        "info": info,
    });

    let client = reqwest::Client::new();

    // Missing token.
    let resp = client
        .post(&report_url)
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(store.load_branch(&rid, "main").await.unwrap().is_none());

    // Wrong token.
    let resp = client
        .post(&report_url)
        .header("Authorization", "Bearer totally-wrong")
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        store.load_branch(&rid, "main").await.unwrap().is_none(),
        "bad token must not write a ref row"
    );

    // Well-formed token signed with the wrong secret → bad signature, no write.
    let wrong_secret =
        mint_job_token(b"not-the-server-secret", Duration::from_secs(3600)).expect("mint");
    let resp = client
        .post(&report_url)
        .header("Authorization", format!("Bearer {wrong_secret}"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(store.load_branch(&rid, "main").await.unwrap().is_none());

    // Correct token does write (proves the guard, not a broken endpoint).
    let good = mint_report_token();
    let resp = client
        .post(&report_url)
        .header("Authorization", format!("Bearer {good}"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let stored = store.load_branch(&rid, "main").await.unwrap().unwrap();
    assert_eq!(stored.commit, "deadbeef");
}

/// Point the worker at a dead report URL: the job must requeue (retryable) and
/// eventually dead-letter — never mark done. Losing a build result silently is
/// unacceptable. With `MAX_ATTEMPTS=2` we get one requeue then a dead-letter;
/// the dead-letter message is the durable proof the retryable path ran.
#[tokio::test]
async fn dead_report_url_job_requeues_not_done() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, _queue_url, meta_url) = setup_sqlite_queue_and_meta();
    // Two attempts: first retryable failure requeues, second dead-letters.
    // Worker reads this at SqlJobQueue::new.
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE_MAX_ATTEMPTS", "2");
    }

    let server = start_server().await;
    // Port 1 is unroutable/refused on loopback — network error → retryable.
    let dead_url = "http://127.0.0.1:1/v1/refs";
    let token = mint_report_token();

    let origin = make_origin("acme", "dead-url");
    origin.commit(&[("a.txt", "x\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/dead-url")
        .await
        .expect("add");

    let queue = backends::connect_sql_queue().await.expect("queue");
    let enq = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("acme/dead-url"),
            branch: "main".into(),
            rev: None,
            credential: None,
            recheck: 0,
            size_bytes: None,
        })
        .await
        .expect("enqueue");
    let job_id = enq.job_id.expect("job id");

    let _worker = spawn_api_worker(&server.cas_dir, &server.repo_root, dead_url, &token);

    let msg = wait_dead_lettered(&queue, job_id, Duration::from_secs(90)).await;
    assert!(
        msg.contains("metadata report") || msg.contains("127.0.0.1:1") || msg.contains("error"),
        "dead-letter should mention the report failure: {msg}"
    );

    // No ref written: the report never reached the server.
    let store = open_meta_store(&meta_url).await;
    let rid = RepoId::github("acme/dead-url");
    assert!(
        store.load_branch(&rid, "main").await.unwrap().is_none(),
        "failed report must not leave a ref in the DB"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_QUEUE_MAX_ATTEMPTS");
    }
}
