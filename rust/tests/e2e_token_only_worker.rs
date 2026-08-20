//! A standalone worker claims and acknowledges through the authenticated API
//! while the server alone owns the SQLite control database.

mod common;

use common::*;
use ripclone::job_token::{mint_job_token, report_token_secret_from_env};
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue};
use ripclone::ref_store::{AddedRepo, AddedRepoSource};
use ripclone::server::{RateLimiter, ServerState, build_app};
use ripclone::storage::StorageBackend;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::SystemTime;
use std::time::{Duration, Instant};

// Hosted embedded replicas bootstrap through libsql's blocking bridge, matching
// the server binary's multi-thread Tokio runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_only_worker_builds_and_clones_without_control_credentials() {
    init(false);
    let require_turso = std::env::var("RIPCLONE_REQUIRE_TURSO").as_deref() == Ok("1");
    let turso = require_turso.then(|| ripclone::control::TursoReplicaConfig {
        url: std::env::var("RIPCLONE_TURSO_DATABASE_URL")
            .expect("RIPCLONE_TURSO_DATABASE_URL for required Turso proof"),
        token: std::env::var("RIPCLONE_TURSO_AUTH_TOKEN")
            .expect("RIPCLONE_TURSO_AUTH_TOKEN for required Turso proof"),
    });
    if require_turso {
        for key in [
            "RIPCLONE_S3_ENDPOINT",
            "RIPCLONE_S3_BUCKET",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ] {
            assert!(
                std::env::var_os(key).is_some(),
                "{key} is required for Turso plus S3 proof"
            );
        }
    }
    let origin = make_origin("acme", "api-only");
    let commit = origin.commit(&[("value.txt", "built by API worker\n")], "api worker");
    origin.publish();
    let dir = tempfile::tempdir().unwrap();
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    let control_path = dir.path().join("control.db");
    let control = Arc::new(
        ripclone::control::ControlDb::open(
            &control_path,
            turso,
            ripclone::queue::default_size_classes(),
        )
        .await
        .unwrap(),
    );
    control
        .ref_store()
        .add_repo(&AddedRepo {
            repo_id: RepoId::github("acme/api-only"),
            added_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            history_enabled: true,
            source: AddedRepoSource::Api,
            repo_size_bytes: None,
        })
        .await
        .unwrap();
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

    let secret = report_token_secret_from_env().unwrap();
    let token = mint_job_token(&secret, Duration::from_secs(300)).unwrap();
    let decoy = dir.path().join("worker-must-not-open.db");
    // Local artifact storage is intentionally shared for this composition;
    // the worker still receives no path or credential for the control database.
    let worker_cas = cas_dir.clone();
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
    let client = ripclone::client::Client::new_with_token(server_url, Some(token_hash()));
    let ready = tokio::time::timeout(
        Duration::from_secs(30),
        client.sync_repo("acme/api-only", None),
    )
    .await
    .expect("API worker published Head within the bound")
    .expect("API worker sync succeeded");
    assert_eq!(ready.commit, commit);

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

    let clone = dir.path().join("clone");
    client
        .install_repo_with_mode_at(
            "acme/api-only",
            "main",
            Some(&commit),
            &clone,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("clone API-worker artifacts");
    assert_eq!(
        std::fs::read_to_string(clone.join("value.txt")).unwrap(),
        "built by API worker\n"
    );
    assert_eq!(git(&clone, &["rev-parse", "HEAD"]), commit);
    assert_eq!(control.is_turso_replica(), require_turso);
    if require_turso {
        let s3 = ripclone::storage::S3Storage::from_env()
            .expect("configure required S3 fixture")
            .expect("required S3 fixture is enabled");
        assert!(
            !s3.list_hashes()
                .expect("list required S3 artifacts")
                .is_empty(),
            "API worker uploaded artifacts to S3"
        );
    }
    assert!(!decoy.exists(), "worker opened the decoy control database");
    assert!(
        control_path.exists(),
        "server retained its control database"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turso_primary_loss_rejects_ref_and_job_writes() {
    if std::env::var("RIPCLONE_REQUIRE_TURSO_FAILURE").as_deref() != Ok("1") {
        return;
    }
    init(false);
    let turso = ripclone::control::TursoReplicaConfig {
        url: std::env::var("RIPCLONE_TURSO_DATABASE_URL")
            .expect("RIPCLONE_TURSO_DATABASE_URL for required failure proof"),
        token: std::env::var("RIPCLONE_TURSO_AUTH_TOKEN")
            .expect("RIPCLONE_TURSO_AUTH_TOKEN for required failure proof"),
    };
    let barrier = std::path::PathBuf::from(
        std::env::var_os("RIPCLONE_TURSO_FAILURE_BARRIER")
            .expect("RIPCLONE_TURSO_FAILURE_BARRIER for required failure proof"),
    );
    std::fs::create_dir_all(&barrier).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let control = ripclone::control::ControlDb::open(
        &dir.path().join("control.db"),
        Some(turso),
        ripclone::queue::default_size_classes(),
    )
    .await
    .expect("bootstrap embedded replica while primary is available");
    let ref_store = control.ref_store();
    let queue = control.queue();
    let baseline = AddedRepo {
        repo_id: RepoId::github("acme/before-primary-loss"),
        added_at: 1,
        history_enabled: true,
        source: AddedRepoSource::Api,
        repo_size_bytes: None,
    };
    ref_store
        .add_repo(&baseline)
        .await
        .expect("primary acknowledges baseline durable write");

    std::fs::write(barrier.join("ready"), b"ready\n").unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !barrier.join("proceed").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("proof driver stopped the primary within the bound");

    let after_loss = AddedRepo {
        repo_id: RepoId::github("acme/after-primary-loss"),
        added_at: 2,
        history_enabled: true,
        source: AddedRepoSource::Api,
        repo_size_bytes: None,
    };
    let ref_error = tokio::time::timeout(Duration::from_secs(30), ref_store.add_repo(&after_loss))
        .await
        .expect("failed ref write returned within the bound")
        .expect_err("remote-primary loss must reject a new ref write");
    assert!(!ref_error.to_string().is_empty());
    assert!(
        ref_store
            .load_added_repo(&after_loss.repo_id)
            .await
            .unwrap()
            .is_none(),
        "failed remote ref write became visible in the local replica"
    );

    let job = BuildJob {
        repo_id: RepoId::github("acme/job-after-primary-loss"),
        branch: "main".to_string(),
        admitted_commit: "1111111111111111111111111111111111111111".to_string(),
        admitted_default_branch: Some("main".to_string()),
        credential: None,
        size_bytes: None,
    };
    let job_error = tokio::time::timeout(Duration::from_secs(30), queue.enqueue(job))
        .await
        .expect("failed job write returned within the bound")
        .expect_err("remote-primary loss must reject a new job write");
    assert!(!job_error.to_string().is_empty());
    assert_eq!(
        queue.depth().await,
        0,
        "failed remote job write became visible in the local replica"
    );
}
