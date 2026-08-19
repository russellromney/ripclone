//! A standalone worker claims and acknowledges through the authenticated API
//! while the server alone owns the SQLite control database.

mod common;

use common::*;
use ripclone::job_token::{mint_job_token, report_token_secret_from_env};
use ripclone::queue::{BuildJob, JobQueue, JobState};
use ripclone::server::{RateLimiter, ServerState, build_app};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn token_only_worker_never_opens_control_credentials() {
    init(false);
    let dir = tempfile::tempdir().unwrap();
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    let control_path = dir.path().join("control.db");
    let control = Arc::new(
        ripclone::control::ControlDb::open(
            &control_path,
            None,
            ripclone::queue::default_size_classes(),
        )
        .await
        .unwrap(),
    );
    let metrics = ripclone::metrics::Metrics::new();
    let backends = ripclone::backends::Backends::from_env_with_ref_store(
        &cas_dir,
        &repo_root,
        &metrics,
        control.ref_store(),
    )
    .await
    .unwrap();
    let queue = control.queue();
    let provider_registry = ripclone::provider::ProviderRegistry::new();
    let state = ServerState {
        cas: backends.cas,
        repo_config: Arc::new(ripclone::repo_config::RepoConfigStore::new(
            backends.storage.clone(),
        )),
        storage: backends.storage,
        repo_root: repo_root.clone(),
        ref_store: backends.ref_store,
        provider_registry: provider_registry.clone(),
        broker: Arc::new(ripclone::auth::broker::StaticBroker::new(provider_registry)),
        token_hash: Some(token_hash()),
        jwt: None,
        metrics,
        rate_limiter: RateLimiter::new(1_000_000, 1_000_000.0),
        retention: backends.retention,
        build_queue: queue.clone(),
        control_db: Some(control.clone()),
        worker_queue: Some(queue.clone()),
        build_queue_depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        oidc_verifier: None,
        webhook_config: Arc::new(ripclone::webhook::WebhookConfig::empty()),
        sync_locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        mirror_freshness: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        mirror_fresh_ttl: Duration::from_secs(60),
        ref_response_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        artifact_fetch_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        fail_first_fetches: 0,
        artifact_barrier: None,
        readyz_cache: Arc::new(std::sync::Mutex::new(None)),
        access_verifier: Arc::new(ripclone::auth::access::HttpAccessVerifier::new()),
        require_repo_auth: false,
    };
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(
            listener,
            build_app(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let ready = format!("http://127.0.0.1:{port}/readyz");
    for attempt in 0..200 {
        if reqwest::get(&ready)
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        assert!(
            attempt < 199,
            "API-only worker test server did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let enqueued = queue
        .enqueue(BuildJob {
            repo_id: ripclone::provider::RepoId {
                provider: ripclone::provider::ProviderInstanceId::new("missing-provider"),
                path: "acme/api-only".to_string(),
            },
            branch: "main".to_string(),
            admitted_commit: "1111111111111111111111111111111111111111".to_string(),
            admitted_default_branch: Some("main".to_string()),
            credential: None,
            size_bytes: None,
        })
        .await
        .unwrap();
    let job_id = enqueued.job_id.unwrap();
    let secret = report_token_secret_from_env().unwrap();
    let token = mint_job_token(&secret, Duration::from_secs(300)).unwrap();
    let decoy = dir.path().join("worker-must-not-open.db");
    let worker_cas = dir.path().join("worker-cas");
    let worker_repos = dir.path().join("worker-repos");
    let server_url = format!("http://127.0.0.1:{port}");
    let mut command = Command::new(cargo_bin("ripclone-worker"));
    command
        .arg("--cas-dir")
        .arg(&worker_cas)
        .arg("--repo-root")
        .arg(&worker_repos)
        .arg("--idle-poll-ms")
        .arg("20")
        .arg("--max-jobs")
        .arg("1")
        .env("RIPCLONE_QUEUE_API_URL", &server_url)
        .env(
            "RIPCLONE_CONFIG",
            dir.path().join("worker-config-missing.toml"),
        )
        .env(
            "RIPCLONE_METADATA_REPORT_URL",
            format!("{server_url}/v1/refs"),
        )
        .env("RIPCLONE_METADATA_JOB_TOKEN", token)
        .env_remove("RIPCLONE_CONTROL_DB_PATH")
        .env_remove("RIPCLONE_TURSO_DATABASE_URL")
        .env_remove("RIPCLONE_TURSO_AUTH_TOKEN")
        .env_remove("RIPCLONE_METADATA_DB_URL")
        .env_remove("RIPCLONE_METADATA_DB_TOKEN")
        .env_remove("RIPCLONE_QUEUE_DB_URL")
        .env_remove("RIPCLONE_QUEUE_DB_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    for key in [
        "RIPCLONE_CONTROL_DB_PATH",
        "RIPCLONE_TURSO_DATABASE_URL",
        "RIPCLONE_TURSO_AUTH_TOKEN",
        "RIPCLONE_METADATA_DB_URL",
        "RIPCLONE_METADATA_DB_TOKEN",
        "RIPCLONE_QUEUE_DB_URL",
        "RIPCLONE_QUEUE_DB_TOKEN",
    ] {
        assert_eq!(
            command
                .get_envs()
                .find(|(candidate, _)| *candidate == std::ffi::OsStr::new(key))
                .and_then(|(_, value)| value),
            None,
            "worker command must remove {key}"
        );
    }
    // The parent can carry a realistic decoy; the child command explicitly
    // removes the selector and therefore cannot create it.
    unsafe { std::env::set_var("RIPCLONE_CONTROL_DB_PATH", &decoy) };
    let mut child = command.spawn().unwrap();
    unsafe { std::env::remove_var("RIPCLONE_CONTROL_DB_PATH") };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "worker exited {status}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not finish one API job"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(matches!(
        queue.job_status(job_id).await.unwrap(),
        JobState::Failed(_)
    ));
    assert!(!decoy.exists(), "worker opened the decoy control database");
    assert!(
        control_path.exists(),
        "server retained its control database"
    );
}
