//! A standalone worker claims and acknowledges through the authenticated API
//! while the server alone owns the SQLite control database.

mod common;

use common::*;
use ripclone::job_token::{mint_job_token, report_token_secret_from_env};
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue, JobState};
use ripclone::ref_store::{AddedRepo, AddedRepoSource};
use ripclone::server::{RateLimiter, ServerState, build_app};
use ripclone::{ClonepackArtifacts, RefInfo};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::SystemTime;
use std::time::{Duration, Instant};

struct ScopedEnvVar {
    key: &'static str,
    previous: Option<OsString>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

fn record_clonepack_hashes(hashes: &mut BTreeSet<String>, clonepack: &ClonepackArtifacts) {
    for hash in [
        &clonepack.manifest,
        &clonepack.metadata_chunk,
        &clonepack.skeleton_pack,
        &clonepack.skeleton_idx,
        &clonepack.prebuilt_index,
        &clonepack.midx,
        &clonepack.idx_bundle,
    ] {
        if !hash.is_empty() {
            hashes.insert(hash.clone());
        }
    }
}

fn published_hashes(result: &RefInfo) -> BTreeSet<String> {
    let mut hashes = BTreeSet::new();
    if let Some(head) = &result.head {
        record_clonepack_hashes(&mut hashes, &head.clonepack);
        for pack in &head.packs {
            hashes.insert(pack.pack.clone());
            hashes.insert(pack.idx.clone());
        }
        for pack in &head.base_packs {
            hashes.insert(pack.pack.clone());
            hashes.insert(pack.idx.clone());
        }
    }
    if let Some(full) = &result.full {
        record_clonepack_hashes(&mut hashes, &full.clonepack);
        for pack in &full.packs {
            hashes.insert(pack.pack.clone());
            hashes.insert(pack.idx.clone());
        }
        for level in &full.history_levels {
            for pack in &level.packs {
                hashes.insert(pack.pack.clone());
                hashes.insert(pack.idx.clone());
            }
        }
    }
    if let Some(files) = &result.files {
        record_clonepack_hashes(&mut hashes, &files.clonepack);
        hashes.extend(files.archive_chunks.iter().cloned());
        hashes.extend(
            files
                .archive_frames
                .iter()
                .map(|frame| frame.chunk_hash.clone()),
        );
    }
    hashes.retain(|hash| !hash.is_empty());
    hashes
}

async fn job_count(control_path: &std::path::Path, repo: &str) -> i64 {
    let database = libsql::Builder::new_local(control_path)
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    let mut rows = connection
        .query("SELECT COUNT(*) FROM jobs WHERE path = ?1", [repo])
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
}

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
    let _cache_max_age =
        require_turso.then(|| ScopedEnvVar::set("RIPCLONE_RETENTION_MAX_AGE_DAYS", "1"));
    // A short lease makes the real child prove renewal repeatedly while Full
    // is held. The worker command intentionally does not enable optional idle
    // fleet heartbeats.
    unsafe { std::env::set_var("RIPCLONE_QUEUE_STALE_SECS", "2") };
    let control = Arc::new(
        ripclone::control::ControlDb::open(
            &control_path,
            turso,
            ripclone::queue::default_size_classes(),
        )
        .await
        .unwrap(),
    );
    unsafe { std::env::remove_var("RIPCLONE_QUEUE_STALE_SECS") };
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
    let admitted_config = ripclone::repo_config::RepoConfig {
        compression_level: Some(3),
        ..Default::default()
    };
    control
        .put_repository_config(&RepoId::github("acme/api-only"), &admitted_config)
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
    let artifact_storage = backends.storage.clone();
    let cache_cleanup_fixture = if require_turso {
        let cache_only_hash = backends
            .cas
            .put(b"cache-only object without a durable remote copy")
            .unwrap();
        let cache_only_path = backends.cas.path(&cache_only_hash);
        let durable_bytes = b"old local object with a durable remote copy";
        let durable_hash = backends.cas.put(durable_bytes).unwrap();
        let durable_path = backends.cas.path(&durable_hash);
        filetime::set_file_mtime(&cache_only_path, filetime::FileTime::from_unix_time(1, 0))
            .unwrap();
        filetime::set_file_mtime(&durable_path, filetime::FileTime::from_unix_time(1, 0)).unwrap();

        assert!(
            artifact_storage
                .verify_durable_copy(&cache_only_hash)
                .is_err(),
            "cache-only bytes must not count as a durable remote copy"
        );
        artifact_storage.put(&durable_hash, durable_bytes).unwrap();
        artifact_storage
            .verify_durable_copy(&durable_hash)
            .expect("uploaded cache fixture has a durable remote copy");
        Some((cache_only_path, durable_path, durable_hash))
    } else {
        None
    };
    let provider_registry = ripclone::provider::ProviderRegistry::new();
    let state = ServerState {
        cas: backends.cas,
        storage: backends.storage,
        repo_root: repo_root.clone(),
        ref_store: backends.ref_store,
        provider_registry: provider_registry.clone(),
        broker: Arc::new(ripclone::auth::broker::StaticBroker::new(provider_registry)),
        token_hash: Some(token_hash()),
        jwt: None,
        metrics,
        rate_limiter: RateLimiter::new(1_000_000, 1_000_000.0),
        build_queue: queue.clone(),
        control_db: Some(control.clone()),
        worker_queue: Some(queue.clone()),
        build_queue_depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        oidc_verifier: None,
        webhook_config: Arc::new(ripclone::webhook::WebhookConfig::empty()),
        sync_locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
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
    let full_barrier = dir.path().join("api-worker-full-barrier");
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
        .env("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS", "3")
        .env("RIPCLONE_TESTING", "1")
        .env("RIPCLONE_TEST_AFTER_HEAD_BARRIER_DIR", &full_barrier)
        .env("RIPCLONE_TEST_AFTER_HEAD_BARRIER_COMMIT", &commit)
        .env_remove("RIPCLONE_WORKER_HEARTBEAT")
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
    if let Some((cache_only_path, durable_path, durable_hash)) = &cache_cleanup_fixture {
        tokio::time::timeout(Duration::from_secs(30), async {
            while durable_path.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("API worker cleaned its old remote-backed CAS object");
        assert!(
            cache_only_path.exists(),
            "API worker cleanup must retain bytes without a durable remote copy"
        );
        artifact_storage
            .verify_durable_copy(durable_hash)
            .expect("API worker cleanup retained the durable MinIO object");
    }
    let client = ripclone::client::Client::new_with_token(server_url.clone(), Some(token_hash()));
    let admission = tokio::time::timeout(
        Duration::from_secs(30),
        client.admit_sync_repo("acme/api-only", None),
    )
    .await
    .expect("API worker job admitted within the bound")
    .expect("API worker admission succeeded");
    assert_eq!(admission.commit, commit);

    tokio::time::timeout(Duration::from_secs(30), async {
        while !full_barrier.join("entered").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("API worker entered the held Full phase");
    let exact = control
        .ref_store()
        .load_result(&RepoId::github("acme/api-only"), &commit)
        .await
        .unwrap()
        .expect("Head publication is durable before held Full");
    assert_eq!(exact.commit, commit);
    assert!(
        exact.head.is_some(),
        "Head is published before Full is released"
    );
    assert!(
        exact.full.is_none(),
        "Full remains unavailable at its barrier"
    );

    // The live repository setting may change while Full is in flight, but the
    // durable job and token-only claim keep the admitted snapshot.
    control
        .put_repository_config(
            &RepoId::github("acme/api-only"),
            &ripclone::repo_config::RepoConfig {
                compression_level: Some(19),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Exercise the server's real reclaim path repeatedly for longer than the
    // configured two-second stale bound. Mandatory active heartbeats keep the
    // claim unavailable even though idle fleet heartbeats were not enabled.
    let lease_proof_deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < lease_proof_deadline {
        assert!(
            queue.claim("reclaim-decoy").await.unwrap().is_none(),
            "healthy API worker Full claim was reclaimed without idle heartbeats enabled"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    std::fs::write(full_barrier.join("proceed"), b"proceed\n").unwrap();

    let job_key = format!(
        "{}\x1f{commit}",
        RepoId::github("acme/api-only").storage_key()
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if matches!(
                queue.job_state_for_key(&job_key).await.unwrap(),
                JobState::Done
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("first API worker job settled");

    let refs = control.ref_store();
    let first = refs
        .load_result(&RepoId::github("acme/api-only"), &commit)
        .await
        .unwrap()
        .expect("first API worker job published the exact result");
    assert!(first.head.is_some() && first.full.is_some() && first.files.is_some());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "worker exited {status}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "API worker did not finish its job"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        queue.live_worker_count().await.unwrap(),
        0,
        "settled API work must not leave a fresh worker-registry row"
    );

    let first_json = serde_json::to_string(&first).unwrap();
    let b_hashes = published_hashes(&first);
    assert!(!b_hashes.is_empty());

    if require_turso {
        for hash in &b_hashes {
            artifact_storage
                .verify_durable_copy(hash)
                .unwrap_or_else(|error| panic!("published B object {hash} is missing: {error:#}"));
        }

        std::fs::remove_dir_all(&cas_dir).expect("remove worker local CAS");
        std::fs::remove_dir_all(&worker_repos).expect("remove worker local mirror");
        assert!(!cas_dir.exists());
        assert!(!worker_repos.exists());

        let offline_bare = origin.bare.with_extension("git.offline");
        std::fs::rename(&origin.bare, &offline_bare).expect("make upstream unavailable");

        let second_token = mint_job_token(&secret, Duration::from_secs(300)).unwrap();
        let mut second_command = Command::new(cargo_bin("ripclone-worker"));
        second_command
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
            .env("RIPCLONE_METADATA_JOB_TOKEN", second_token)
            .env("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS", "3")
            .env("RIPCLONE_TESTING", "1")
            .env_remove("RIPCLONE_WORKER_HEARTBEAT")
            .env_remove("RIPCLONE_CONTROL_DB_PATH")
            .env_remove("RIPCLONE_TURSO_DATABASE_URL")
            .env_remove("RIPCLONE_TURSO_AUTH_TOKEN")
            .env_remove("RIPCLONE_METADATA_DB_URL")
            .env_remove("RIPCLONE_METADATA_DB_TOKEN")
            .env_remove("RIPCLONE_QUEUE_DB_URL")
            .env_remove("RIPCLONE_QUEUE_DB_TOKEN")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut second_worker = second_command.spawn().unwrap();

        let jobs_before_clones = job_count(&control_path, "acme/api-only").await;
        assert_eq!(jobs_before_clones, 1);
        for (name, mode, clonepack) in [
            ("head", CloneMode::Editable, "shallow"),
            ("full", CloneMode::Editable, "full"),
            ("files", CloneMode::Files, "full"),
        ] {
            let target = dir.path().join(format!("empty-cache-b-{name}"));
            let outcome = client
                .install_repo_with_mode_at(
                    "acme/api-only",
                    "main",
                    Some(&commit),
                    &target,
                    mode,
                    Some(clonepack),
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("clone exact B {name}: {error:#}"));
            assert_eq!(outcome.commit, commit);
            assert_eq!(
                std::fs::read_to_string(target.join("value.txt")).unwrap(),
                "built by API worker\n"
            );
            if mode == CloneMode::Editable {
                assert_eq!(git(&target, &["rev-parse", "HEAD"]), commit);
            }
        }
        assert_eq!(
            job_count(&control_path, "acme/api-only").await,
            jobs_before_clones,
            "ready exact B clones must enqueue no job"
        );
        let unchanged = refs
            .load_result(&RepoId::github("acme/api-only"), &commit)
            .await
            .unwrap()
            .expect("B remains published after empty-cache clones");
        assert_eq!(serde_json::to_string(&unchanged).unwrap(), first_json);
        for hash in &b_hashes {
            artifact_storage
                .verify_durable_copy(hash)
                .unwrap_or_else(|error| panic!("B object {hash} disappeared: {error:#}"));
        }

        std::fs::rename(&offline_bare, &origin.bare).expect("restore upstream for C");
        let c = origin.commit(&[("value.txt", "built C from empty caches\n")], "C");
        origin.publish();
        let c_admission = tokio::time::timeout(
            Duration::from_secs(30),
            client.admit_sync_repo("acme/api-only", None),
        )
        .await
        .expect("C admitted within the bound")
        .expect("C admission succeeds from empty caches");
        assert_eq!(c_admission.commit, c);

        let c_key = format!("{}\x1f{c}", RepoId::github("acme/api-only").storage_key());
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if matches!(
                    queue.job_state_for_key(&c_key).await.unwrap(),
                    JobState::Done
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("C API worker job settled");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = second_worker.try_wait().unwrap() {
                assert!(status.success(), "restarted worker exited {status}");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "restarted worker did not build C"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let c_result = refs
            .load_result(&RepoId::github("acme/api-only"), &c)
            .await
            .unwrap()
            .expect("C exact result exists");
        assert!(
            c_result.head.is_some() && c_result.full.is_some() && c_result.files.is_some(),
            "C builds every output from empty local caches"
        );
        assert_eq!(job_count(&control_path, "acme/api-only").await, 2);
        let unchanged = refs
            .load_result(&RepoId::github("acme/api-only"), &commit)
            .await
            .unwrap()
            .expect("B remains published after C");
        assert_eq!(serde_json::to_string(&unchanged).unwrap(), first_json);
        for hash in &b_hashes {
            artifact_storage
                .verify_durable_copy(hash)
                .unwrap_or_else(|error| panic!("B object {hash} missing after C: {error:#}"));
        }
    }

    let snapshot_database = libsql::Builder::new_local(&control_path)
        .build()
        .await
        .unwrap();
    let snapshot_connection = snapshot_database.connect().unwrap();
    let mut snapshot_rows = snapshot_connection
        .query(
            "SELECT repo_config FROM jobs WHERE path = ?1 ORDER BY id ASC",
            ["acme/api-only"],
        )
        .await
        .unwrap();
    let mut snapshots = Vec::new();
    while let Some(row) = snapshot_rows.next().await.unwrap() {
        snapshots.push(
            serde_json::from_str::<ripclone::repo_config::RepoConfig>(
                &row.get::<String>(0).unwrap(),
            )
            .unwrap(),
        );
    }
    assert_eq!(
        snapshots.len(),
        if require_turso { 2 } else { 1 },
        "each API-worker build must have exactly one durable job"
    );
    assert_eq!(snapshots[0], admitted_config);
    if require_turso {
        assert_eq!(snapshots[1].compression_level, Some(19));
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
        admitted_commit: "1111111111111111111111111111111111111111".to_string(),
        repo_config: ripclone::repo_config::RepoConfig::default(),
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
