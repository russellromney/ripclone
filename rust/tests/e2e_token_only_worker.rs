//! Token-only farm-out worker: claim + ack + heartbeat + refs entirely over the
//! server's HTTP API, with **zero** database credentials on the worker.
//!
//! The server holds the one queue + metadata DB (`RIPCLONE_QUEUE=sqlite`,
//! `RIPCLONE_METADATA=sqlite`) and serves `/v1/jobs/*` + `/v1/refs`. The worker
//! runs with `RIPCLONE_QUEUE=api` + `RIPCLONE_METADATA=api` + a bearer token and
//! no DB creds. This is the security seam: workers on untrusted infra never hold
//! DB credentials.
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
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The four DB-cred keys a farm-out worker must never receive.
const DB_CRED_KEYS: &[&str] = &[
    "RIPCLONE_QUEUE_DB_URL",
    "RIPCLONE_QUEUE_DB_TOKEN",
    "RIPCLONE_METADATA_DB_URL",
    "RIPCLONE_METADATA_DB_TOKEN",
];

/// Spawn a worker that reaches the queue AND metadata entirely over HTTP with a
/// single bearer token and **no** DB credentials. Asserts the child bag we hand
/// it carries none of the four DB-cred keys.
fn spawn_token_only_worker(
    cas_dir: &Path,
    repo_root: &Path,
    server_url: &str,
    job_token: &str,
) -> WorkerProc {
    spawn_token_only_worker_with(cas_dir, repo_root, server_url, job_token, &[], &[])
}

fn spawn_token_only_worker_with(
    cas_dir: &Path,
    repo_root: &Path,
    server_url: &str,
    job_token: &str,
    extra_args: &[&str],
    extra_env: &[(&str, &str)],
) -> WorkerProc {
    let report_url = format!("{server_url}/v1/refs");
    let mut cmd = Command::new(cargo_bin("ripclone-worker"));
    cmd.arg("--cas-dir")
        .arg(cas_dir)
        .arg("--repo-root")
        .arg(repo_root)
        .arg("--idle-poll-ms")
        .arg("100")
        .args(extra_args)
        .env_remove("RIPCLONE_IDLE_EXIT_SECS")
        .env_remove("RIPCLONE_MAX_JOBS")
        // Queue over HTTP.
        .env("RIPCLONE_QUEUE", "api")
        .env("RIPCLONE_QUEUE_API_URL", server_url)
        // Metadata over HTTP.
        .env("RIPCLONE_METADATA", "api")
        .env("RIPCLONE_METADATA_REPORT_URL", &report_url)
        // One token for all four endpoints.
        .env("RIPCLONE_METADATA_JOB_TOKEN", job_token)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    // The farm-out contract: not one DB credential on the worker.
    for k in DB_CRED_KEYS {
        cmd.env_remove(k);
    }
    // Prove the exact env we hand the child carries none of the four DB keys.
    // `env_remove` records the key with a `None` value, so it is never inherited.
    let planned: std::collections::HashMap<std::ffi::OsString, Option<std::ffi::OsString>> = cmd
        .get_envs()
        .map(|(k, v)| (k.to_owned(), v.map(|s| s.to_owned())))
        .collect();
    for k in DB_CRED_KEYS {
        assert_eq!(
            planned.get(std::ffi::OsStr::new(k)),
            Some(&None),
            "farm-out worker env must not carry DB cred {k}"
        );
    }
    let child = cmd.spawn().expect("spawn token-only ripclone-worker");
    WorkerProc::from_child(child)
}

async fn wait_for_exact_full(
    store: &Arc<dyn RefStore>,
    repo_id: &RepoId,
    commit: &str,
) -> ripclone::RefInfo {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(info) = store
            .load_build(repo_id, commit)
            .await
            .expect("load exact API-worker build")
            && info.commit == commit
            && matches!(info.build_status.as_deref(), None | Some("done"))
            && !info.full_clonepack.manifest.is_empty()
        {
            return info;
        }
        assert!(
            Instant::now() < deadline,
            "exact Full({commit}) did not settle through the API worker"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
        std::env::set_var("RIPCLONE_QUEUE_RETRY_BACKOFF_MS", "20");
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "20");
    }
    init(false);
    (qdir, mdir, queue_url, meta_url)
}

/// Mint a report/queue token with an explicit TTL. `init` sets
/// `RIPCLONE_SERVER_TOKEN`, from which the secret derives.
fn mint_token(ttl: Duration) -> String {
    let secret = report_token_secret_from_env().expect("job token secret (RIPCLONE_SERVER_TOKEN)");
    mint_job_token(&secret, ttl).expect("mint token")
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

async fn enqueue_job(path: &str) -> (SqlJobQueue, i64) {
    let queue = backends::connect_sql_queue().await.expect("queue");
    let enq = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github(path),
            branch: "main".into(),
            rev: None,
            admitted_commit: Some("1111111111111111111111111111111111111111".into()),
            admitted_default_branch: None,
            credential: None,
            recheck: 0,
            size_bytes: None,
        })
        .await
        .expect("enqueue");
    (queue, enq.job_id.expect("job id"))
}

/// POSITIVE: a real token-only worker (no DB creds in its env) claims an
/// enqueued job over HTTP, builds it, acks over HTTP, and the ref lands in the
/// server's sqlite — end to end through the API.
#[tokio::test]
async fn token_only_worker_claims_builds_acks_over_api() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, _queue_url, meta_url) = setup_sqlite_queue_and_meta();

    let server = start_server().await;
    let token = mint_token(Duration::from_secs(3600));

    let mut worker =
        spawn_token_only_worker(&server.cas_dir, &server.repo_root, &server.url, &token);

    let origin = make_origin("acme", "tok-only");
    let commit = origin.commit(&[("a.txt", "hi\n")], "c1");
    origin.publish();

    register_added_without_build(&server, "acme/tok-only")
        .await
        .expect("add repo");
    let resp = server
        .client()
        .sync_repo("acme/tok-only", None)
        .await
        .expect("token-only farm-out sync");
    assert_eq!(resp.commit, commit);

    // The ref reached the server's sqlite metadata: worker → POST /v1/refs.
    let store = open_meta_store(&meta_url).await;
    let rid = RepoId::github("acme/tok-only");
    let stored = store
        .load_branch(&rid, "main")
        .await
        .expect("load")
        .expect("ref must be in sqlite after api build");
    assert_eq!(stored.commit, commit);
    // (The spawn helper already asserted the child's env carries no DB creds.)
    worker.kill_now();
}

/// One complete immutable API-worker journey: public admissions race credentials
/// and a moving branch, while the real worker owns no database/provider secret
/// and can obtain the exact source identity only from its authenticated claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_worker_publishes_exact_result_without_db_secret() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, queue_url, meta_url) = setup_sqlite_queue_and_meta();

    const T1: &str = "credential-one";
    const T2: &str = "credential-two-decoy";
    let origin = make_http_origin_with_alternate_discovery_auth(
        "acme/api-immutable",
        &format!("token {T1}"),
        &format!("token {T2}"),
    );
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();

    let isolated = tempfile::tempdir().expect("isolated API-worker config");
    let server_config = isolated.path().join("server-config-missing.toml");
    let providers = serde_json::json!({
        "providers": [{
            "id": "credential-http",
            "kind": "generic",
            "host": origin.url,
            "auth_template": "token {token}",
        }]
    })
    .to_string();
    let server = start_server_env(&[
        ("RIPCLONE_CONFIG", server_config.to_str().unwrap()),
        ("RIPCLONE_PROVIDERS", &providers),
    ])
    .await;
    register_added_without_build_for_provider(&server, "credential-http", "acme/api-immutable")
        .await
        .expect("register private API-worker fixture");

    let first_client = server.client_with_provider("credential-http", Some(T1));
    let started = Instant::now();
    let first = first_client
        .admit_sync_repo("acme/api-immutable", None)
        .await
        .expect("admit B with T1");
    let first_elapsed = started.elapsed();
    assert!(first.accepted);
    assert_eq!(first.commit, b);
    assert!(
        first_elapsed < Duration::from_secs(5),
        "public B admission waited without a worker: {:?}",
        first_elapsed
    );

    let duplicate = server
        .client_with_provider("credential-http", Some(T2))
        .admit_sync_repo("acme/api-immutable", None)
        .await
        .expect("duplicate B with discovery-only T2");
    assert_eq!(duplicate.commit, b);
    assert_eq!(duplicate.status, "coalesced");

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::from_str(&queue_url)
                .expect("parse queue database path"),
        )
        .await
        .expect("open queue database");
    let active_b: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE admitted_commit = ? AND status IN ('queued', 'claimed')",
    )
    .bind(&b)
    .fetch_one(&pool)
    .await
    .expect("count active B rows");
    assert_eq!(active_b, 1, "duplicate B must share one active SQL row");

    // C diverges from A at the same history depth as B and becomes the moving
    // row before the API worker can publish B.
    git(&origin.work, &["reset", "--hard", &a]);
    let c = origin.commit(&[("value.txt", "C\n")], "C-divergent");
    origin.publish();
    let store = open_meta_store(&meta_url).await;
    let repo_id = RepoId {
        provider: ripclone::provider::ProviderInstanceId::new("credential-http"),
        path: "acme/api-immutable".to_string(),
    };
    let moving_c = ripclone::RefInfo {
        commit: c.clone(),
        default_branch: "main".to_string(),
        generation: Some(2),
        synced_at: Some(2),
        ..Default::default()
    };
    store
        .save_branch(&repo_id, "main", &moving_c)
        .await
        .expect("install equal-depth moving C before B worker");
    let moving_c_json = serde_json::to_value(&moving_c).unwrap();

    let decoy_queue = isolated.path().join("worker-must-not-open-queue.db");
    let decoy_meta = isolated.path().join("worker-must-not-open-meta.db");
    let worker_config = isolated.path().join("worker-config-missing.toml");
    // Put realistic decoy DB coordinates in the inherited process environment.
    // `spawn_token_only_worker_with` must remove all four keys from the child;
    // if that security boundary regresses, these files become observable.
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &decoy_queue);
        std::env::set_var("RIPCLONE_QUEUE_DB_TOKEN", "decoy-queue-token");
        std::env::set_var("RIPCLONE_METADATA_DB_URL", &decoy_meta);
        std::env::set_var("RIPCLONE_METADATA_DB_TOKEN", "decoy-meta-token");
    }
    let worker_repo = tempfile::tempdir().expect("isolated worker scratch");
    let token = mint_token(Duration::from_secs(3600));
    let mut worker = spawn_token_only_worker_with(
        &server.cas_dir,
        worker_repo.path(),
        &server.url,
        &token,
        &[],
        &[
            ("RIPCLONE_CONFIG", worker_config.to_str().unwrap()),
            ("RIPCLONE_PROVIDERS", &providers),
        ],
    );
    unsafe {
        std::env::remove_var("RIPCLONE_QUEUE_DB_URL");
        std::env::remove_var("RIPCLONE_QUEUE_DB_TOKEN");
        std::env::remove_var("RIPCLONE_METADATA_DB_URL");
        std::env::remove_var("RIPCLONE_METADATA_DB_TOKEN");
    }

    let full_b = wait_for_exact_full(&store, &repo_id, &b).await;
    assert_eq!(full_b.commit, b);
    let full_b_manifest = full_b.full_clonepack.manifest.clone();
    assert!(server.cas_path(&full_b_manifest).exists());
    assert_eq!(
        serde_json::to_value(store.load_branch(&repo_id, "main").await.unwrap().unwrap()).unwrap(),
        moving_c_json,
        "late equal-depth B publication through the API cannot mutate moving C"
    );

    let log_after_b = origin.auth_log_text();
    let mut t2_discovery = 0;
    let mut t2_object_requests = 0;
    let mut t1_object_requests = 0;
    for line in log_after_b.lines() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() < 5 {
            continue;
        }
        let path = fields[2].split('?').next().unwrap_or(fields[2]);
        let discovery = path.ends_with("/info/refs") || path.ends_with("/HEAD");
        match fields[3] {
            "token credential-two-decoy" if discovery => t2_discovery += 1,
            "token credential-two-decoy" => t2_object_requests += 1,
            "token credential-one" if !discovery => t1_object_requests += 1,
            _ => {}
        }
    }
    assert!(
        t2_discovery > 0,
        "T2 did not perform its real admission probe"
    );
    assert_eq!(
        t2_object_requests, 0,
        "coalescing replaced T1 with T2 for source acquisition:\n{log_after_b}"
    );
    assert!(
        t1_object_requests > 0,
        "B worker did not fetch protected objects with T1:\n{log_after_b}"
    );
    assert!(!decoy_queue.exists(), "API worker opened decoy queue DB");
    assert!(!decoy_meta.exists(), "API worker opened decoy metadata DB");

    let admitted_c = first_client
        .admit_sync_repo("acme/api-immutable", None)
        .await
        .expect("admit C separately");
    assert!(admitted_c.accepted);
    assert_eq!(admitted_c.commit, c);
    let full_c = wait_for_exact_full(&store, &repo_id, &c).await;
    assert_eq!(full_c.commit, c);
    let ordinary = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load ordinary branch")
        .expect("ordinary branch exists");
    assert_eq!(ordinary.commit, c, "ordinary branch must settle at C");

    let branches = store
        .list_branches(&repo_id)
        .await
        .expect("list API-worker publication refs");
    assert!(
        branches.iter().any(|branch| branch == &format!("main#{b}")),
        "API worker keeps exact B addressable: {branches:?}"
    );
    assert!(
        branches.iter().any(|branch| branch == &format!("main#{c}")),
        "API worker keeps exact C addressable: {branches:?}"
    );
    assert!(
        branches.iter().all(|branch| !branch.starts_with("HEAD#")),
        "ordinary API-worker jobs do not create HEAD exact aliases: {branches:?}"
    );
    assert!(
        server.cas_path(&full_b_manifest).exists(),
        "superseded B bytes may remain in CAS until ordinary retention reclaims them"
    );

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE admitted_commit IN (?, ?)")
        .bind(&b)
        .bind(&c)
        .fetch_one(&pool)
        .await
        .expect("count immutable API-worker rows");
    assert_eq!(rows, 2, "B and C require exactly two durable jobs");
    let unfinished: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE admitted_commit IN (?, ?) AND status != 'done'",
    )
    .bind(&b)
    .bind(&c)
    .fetch_one(&pool)
    .await
    .expect("count unfinished immutable API-worker rows");
    assert_eq!(unfinished, 0);
    assert!(!decoy_queue.exists());
    assert!(!decoy_meta.exists());
    println!(
        "API_WORKER_IMMUTABLE_EVIDENCE B={b} C={c} admission_ms={} active_B_after_duplicate=1 durable_jobs={rows} T1_object_requests={t1_object_requests} T2_discovery_requests={t2_discovery} T2_object_requests={t2_object_requests} synthetic_aliases=0 decoy_databases_touched=false ordinary_tip={}",
        first_elapsed.as_millis(),
        ordinary.commit
    );
    worker.kill_now();
    pool.close().await;
}

/// NEGATIVE: claim / ack / heartbeat with a missing / garbage / wrong-secret
/// token → 401 with **no** state change. Wrong-token-then-good-token proves the
/// guard, not a broken endpoint.
#[tokio::test]
async fn worker_endpoints_reject_bad_tokens_no_state_change() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, _queue_url, _meta_url) = setup_sqlite_queue_and_meta();

    let server = start_server().await;
    let client = reqwest::Client::new();

    // A queued job to try to claim.
    let (_queue, job_id) = enqueue_job("acme/negtok").await;

    let claim_url = format!("{}/v1/jobs/claim", server.url);
    let claim_body = serde_json::json!({ "worker_id": "w-neg" });

    let wrong_secret = mint_job_token(b"not-the-server-secret", Duration::from_secs(3600))
        .expect("mint wrong-secret token");
    let bad_auths: [Option<String>; 3] = [
        None,
        Some("Bearer totally-garbage".to_string()),
        Some(format!("Bearer {wrong_secret}")),
    ];

    for auth in &bad_auths {
        let mut req = client.post(&claim_url).json(&claim_body);
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        let resp = req.send().await.expect("claim post");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "bad-token claim must 401 (auth={auth:?})"
        );
    }
    // No claim happened: a GOOD token still claims the job (proves it was never
    // moved out of `queued` by the rejected attempts).
    let good = mint_token(Duration::from_secs(3600));
    let resp = client
        .post(&claim_url)
        .header("Authorization", format!("Bearer {good}"))
        .json(&claim_body)
        .send()
        .await
        .expect("good claim");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let claimed: serde_json::Value = resp.json().await.expect("claim json");
    let job = claimed.get("job").expect("job field");
    assert!(!job.is_null(), "good token must claim the still-queued job");
    assert_eq!(
        job["id"].as_i64(),
        Some(job_id),
        "claim returns exactly our job"
    );

    // ack with bad tokens → 401; job not settled (stays non-terminal).
    let ack_url = format!("{}/v1/jobs/{job_id}/ack", server.url);
    let ack_body = serde_json::json!({ "worker_id": "w-neg", "result": { "ok": true } });
    for auth in &bad_auths {
        let mut req = client.post(&ack_url).json(&ack_body);
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        let resp = req.send().await.expect("ack post");
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    // Job still not done (a rejected ack changed nothing).
    let queue = backends::connect_sql_queue().await.expect("queue");
    assert!(
        matches!(
            queue.job_status(job_id).await.expect("status"),
            JobState::Pending
        ),
        "rejected ack must not settle the job"
    );

    // heartbeat with bad tokens → 401; registry stays empty.
    let hb_url = format!("{}/v1/jobs/heartbeat", server.url);
    let hb_body = serde_json::json!({ "worker_id": "w-neg", "current_job": job_id });
    for auth in &bad_auths {
        let mut req = client.post(&hb_url).json(&hb_body);
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        let resp = req.send().await.expect("hb post");
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    let live = queue.live_worker_count().await.expect("live count");
    assert_eq!(live, 0, "rejected heartbeats must not register a worker");

    // Good token: heartbeat registers, ack settles — proves the endpoints work.
    let resp = client
        .post(&hb_url)
        .header("Authorization", format!("Bearer {good}"))
        .json(&hb_body)
        .send()
        .await
        .expect("good hb");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(queue.live_worker_count().await.expect("live"), 1);

    let resp = client
        .post(&ack_url)
        .header("Authorization", format!("Bearer {good}"))
        .json(&ack_body)
        .send()
        .await
        .expect("good ack");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        matches!(
            queue.job_status(job_id).await.expect("status"),
            JobState::Done
        ),
        "good-token ack settles the job"
    );
}

/// NEGATIVE: a worker whose token has expired → its next claim 401s → it exits
/// cleanly (code 0, not a crash, not a spin), and the job stays claimable by a
/// fresh worker.
// Multi-threaded runtime: this test blocks on `wait_for_exit` while the
// in-process server must keep answering the worker's `/v1/jobs/claim` (so the
// worker sees the 401 and exits). On a single-threaded runtime the blocking
// wait starves the server task and the claim never gets a response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_token_worker_exits_clean_job_survives() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, _queue_url, meta_url) = setup_sqlite_queue_and_meta();

    let server = start_server().await;

    let origin = make_origin("acme", "expiry");
    let commit = origin.commit(&[("a.txt", "later\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/expiry")
        .await
        .expect("add");
    let (_queue, job_id) = enqueue_job("acme/expiry").await;

    // Mint a 1s token and let it expire before the worker's first claim.
    let expired = mint_token(Duration::from_secs(1));
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut worker =
        spawn_token_only_worker(&server.cas_dir, &server.repo_root, &server.url, &expired);
    let status = worker
        .wait_for_exit(Duration::from_secs(20))
        .expect("expired-token worker must exit, not spin");
    assert!(
        status.success(),
        "worker must exit cleanly (code 0) on token expiry, got {status:?}"
    );

    // The job was never settled — still claimable by a fresh worker.
    let queue = backends::connect_sql_queue().await.expect("queue");
    assert!(
        matches!(
            queue.job_status(job_id).await.expect("status"),
            JobState::Pending
        ),
        "expired-token worker must not have settled the job"
    );

    // A fresh worker with a valid token drains it end to end.
    let good = mint_token(Duration::from_secs(3600));
    let mut fresh = spawn_token_only_worker(&server.cas_dir, &server.repo_root, &server.url, &good);
    let resp = server
        .client()
        .sync_repo("acme/expiry", None)
        .await
        .expect("sync after fresh worker");
    assert_eq!(resp.commit, commit);

    let store = open_meta_store(&meta_url).await;
    let stored = store
        .load_branch(&RepoId::github("acme/expiry"), "main")
        .await
        .expect("load")
        .expect("ref lands after fresh worker");
    assert_eq!(stored.commit, commit);
    fresh.kill_now();
}

/// NEGATIVE: the claim endpoint returns at most the one claimed job and never
/// another job's `credential`. Enqueue two jobs, one carrying a per-job upstream
/// credential; a single claim returns exactly one job — and if it is the plain
/// one, its credential is absent (never leaks the other job's secret).
#[tokio::test]
async fn claim_returns_one_job_no_foreign_credential() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, _queue_url, _meta_url) = setup_sqlite_queue_and_meta();
    let server = start_server().await;

    let queue = backends::connect_sql_queue().await.expect("queue");
    // Job A: no credential. Job B: a per-job upstream credential.
    let a = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("acme/plain"),
            branch: "main".into(),
            rev: None,
            admitted_commit: Some("2222222222222222222222222222222222222222".into()),
            admitted_default_branch: None,
            credential: None,
            recheck: 0,
            size_bytes: None,
        })
        .await
        .expect("enqueue a")
        .job_id
        .unwrap();
    let _b = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("acme/secret"),
            branch: "main".into(),
            rev: None,
            admitted_commit: Some("3333333333333333333333333333333333333333".into()),
            admitted_default_branch: None,
            credential: Some(secrecy::SecretString::new(
                "SUPER-SECRET-UPSTREAM".to_string().into(),
            )),
            recheck: 0,
            size_bytes: None,
        })
        .await
        .expect("enqueue b")
        .job_id
        .unwrap();

    let client = reqwest::Client::new();
    let good = mint_token(Duration::from_secs(3600));
    let resp = client
        .post(format!("{}/v1/jobs/claim", server.url))
        .header("Authorization", format!("Bearer {good}"))
        .json(&serde_json::json!({ "worker_id": "w-one" }))
        .send()
        .await
        .expect("claim");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json");

    // Exactly one job object (never a list).
    let job = body.get("job").expect("job field");
    assert!(!job.is_null());
    assert!(
        body.as_object().unwrap().len() == 1,
        "response is just {{job}}"
    );

    // FIFO: job A (the plain one) is claimed first. Its credential must be null —
    // the other job's secret is never attached.
    assert_eq!(job["id"].as_i64(), Some(a));
    assert_eq!(
        job["admitted_commit"].as_str(),
        Some("2222222222222222222222222222222222222222"),
        "API worker claim must transport the exact admitted commit"
    );
    assert!(
        job.get("credential").map(|c| c.is_null()).unwrap_or(true),
        "the plain job must not carry another job's credential: {job}"
    );
    // The secret string must not appear anywhere in the claim response.
    assert!(
        !body.to_string().contains("SUPER-SECRET-UPSTREAM"),
        "claim must never leak another job's credential"
    );
}

/// NEGATIVE: an active legacy row reaches a real API worker, is rejected before
/// credential/provider/source work, and is permanently settled rather than
/// guessed from the branch's current tip.
#[tokio::test]
async fn api_worker_rejects_legacy_job_before_source_work() {
    let _guard = SERIAL.lock().await;
    let (_q, _m, queue_url, _meta_url) = setup_sqlite_queue_and_meta();
    let server = start_server().await;

    // Simulate an active row written by the pre-admission schema. Use an
    // unknown provider as a decoy: if the worker resolves provider/source state
    // before checking admitted_commit, the observed failure will be different.
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::from_str(&queue_url)
                .expect("parse queue database path"),
        )
        .await
        .expect("open queue database");
    sqlx::query(
        "INSERT INTO jobs (key, provider, path, branch, status, created_at, attempts, size_class)
         VALUES (?, ?, ?, ?, 'queued', ?, 0, 0)",
    )
    .bind("legacy-api-row")
    .bind("provider-that-is-not-configured")
    .bind("acme/legacy-api")
    .bind("main")
    .bind(1_i64)
    .execute(&pool)
    .await
    .expect("insert legacy active row");
    pool.close().await;

    let token = mint_token(Duration::from_secs(3600));
    let mut worker =
        spawn_token_only_worker(&server.cas_dir, &server.repo_root, &server.url, &token);

    let deadline = Instant::now() + Duration::from_secs(15);
    let (status, error) = loop {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::from_str(&queue_url)
                    .expect("parse queue database path"),
            )
            .await
            .expect("reopen queue database");
        let row: (String, Option<String>) =
            sqlx::query_as("SELECT status, error FROM jobs WHERE key = 'legacy-api-row'")
                .fetch_one(&pool)
                .await
                .expect("read legacy row");
        pool.close().await;
        if row.0 == "failed" {
            break row;
        }
        assert!(
            Instant::now() < deadline,
            "API worker did not settle the legacy row before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(status, "failed");
    assert_eq!(
        error.as_deref(),
        Some("legacy queued job has no admitted commit; resubmit sync")
    );
    assert!(
        !server
            .repo_root
            .join("provider-that-is-not-configured_acme%2Flegacy-api.git")
            .exists(),
        "legacy rejection must happen before mirror/source mutation"
    );
    worker.kill_now();
}
