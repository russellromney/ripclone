//! Exact-commit Head, Full, and Files publication and restart proofs.

use crate::common::*;
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use ripclone::queue::{JobQueue, JobState};
use ripclone::server::{AdmissionTestBarrier, AdmissionTestProbe};
use ripclone::{ExactResultKind, RefInfo};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct ScopedEnvVar {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
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

async fn wait_barrier(barrier: &AdmissionTestBarrier, count: usize) {
    tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_entered(count))
        .await
        .expect("result barrier entered");
}

async fn wait_count(counter: &AtomicUsize, count: usize) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while counter.load(Ordering::SeqCst) < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("result counter reached expected value");
}

async fn wait_for_failed_job(server: &Server, repo: &str, commit: &str) {
    let queue = ripclone::queue::SqlJobQueue::new(Box::new(
        ripclone::queue::LibsqlDb::connect(&server.control_db.to_string_lossy())
            .await
            .expect("connect exact job queue"),
    ))
    .await
    .expect("initialize exact job queue");
    let key = format!("{}\x1f{commit}", RepoId::github(repo).storage_key());
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match queue
                .job_state_for_key(&key)
                .await
                .expect("read exact job state")
            {
                JobState::Failed(_) => return,
                JobState::Pending | JobState::Unknown => tokio::task::yield_now().await,
                JobState::Done => panic!("injected build failure was recorded as successful"),
            }
        }
    })
    .await
    .expect("exact job reached durable Failed state");
}

async fn exact_result(server: &Server, repo: &str, commit: &str) -> RefInfo {
    server_ref_store(server)
        .await
        .load_result(&RepoId::github(repo), commit)
        .await
        .expect("load exact result")
        .expect("exact result exists")
}

async fn admit(server: &Server, repo: &str) -> String {
    let admission = server
        .client()
        .admit_sync_repo(repo, None)
        .await
        .expect("admit exact job");
    assert!(admission.accepted);
    admission.commit
}

async fn pinned_status(
    server: &Server,
    repo: &str,
    commit: &str,
    result: ExactResultKind,
) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/{repo}/refs/HEAD?result={result}&pinned={commit}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("pinned result request")
        .status()
}

async fn stop_current_job(server: &Server, repo: &str, commit: &str) {
    let database = libsql::Builder::new_local(&server.control_db)
        .build()
        .await
        .expect("open exact job database");
    let connection = database.connect().expect("connect to exact job database");
    let key = format!("{}\x1f{commit}", RepoId::github(repo).storage_key());
    let mut rows = connection
        .query(
            "SELECT id, worker_id FROM jobs WHERE key = ?1 AND status = 'claimed'",
            [key.as_str()],
        )
        .await
        .expect("read exact claimed job");
    let row = rows
        .next()
        .await
        .expect("read exact claimed job row")
        .expect("exact job is claimed at the result barrier");
    let job_id = row.get::<i64>(0).expect("claimed job id");
    let worker_id = row
        .get::<Option<String>>(1)
        .expect("claimed worker id column")
        .expect("claimed job has an owner");
    assert!(
        rows.next()
            .await
            .expect("read duplicate exact claimed job")
            .is_none(),
        "one exact commit must have at most one claimed job"
    );
    drop(rows);

    let queue = ripclone::queue::SqlJobQueue::new(Box::new(
        ripclone::queue::LibsqlDb::connect(&server.control_db.to_string_lossy())
            .await
            .expect("connect exact job queue"),
    ))
    .await
    .expect("open exact job queue");
    assert!(
        queue
            .ack(
                job_id,
                &worker_id,
                Err(ripclone::queue::BuildError::permanent(
                    "stopped by deterministic result test",
                )),
            )
            .await
            .expect("stop current job")
    );
}

async fn fixture(repo: &str, files: &[(&str, &str)]) -> (Server, Origin, String) {
    let server = start_server().await;
    let origin = make_origin("acme", repo);
    let commit = origin.commit(files, "exact commit");
    origin.publish();
    register_added_without_build(&server, &format!("acme/{repo}"))
        .await
        .expect("register fixture");
    (server, origin, commit)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn head_is_ready_while_full_and_files_are_running_and_both_publications_survive() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.before_full_publish.arm();
    probe.before_files_publish.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("head-first", &[("value.txt", "B\n")]).await;

    assert_eq!(admit(&server, "acme/head-first").await, commit);
    let ((), ()) = tokio::join!(
        wait_barrier(&probe.before_full_publish, 1),
        wait_barrier(&probe.before_files_publish, 1),
    );
    let held = exact_result(&server, "acme/head-first", &commit).await;
    assert!(held.head.is_some());
    assert!(held.full.is_none());
    assert!(held.files.is_none());
    assert_eq!(
        pinned_status(&server, "acme/head-first", &commit, ExactResultKind::Head).await,
        reqwest::StatusCode::OK
    );

    probe.before_full_publish.release();
    probe.before_files_publish.release();
    wait_count(&probe.full_publishes, 1).await;
    wait_count(&probe.files_publishes, 1).await;
    assert_eq!(
        probe.bitmap_writes.load(Ordering::SeqCst),
        1,
        "the missing Full result writes one reachability bitmap"
    );
    let complete = exact_result(&server, "acme/head-first", &commit).await;
    assert!(complete.head.is_some() && complete.full.is_some() && complete.files.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn files_can_publish_while_full_is_running() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.before_full_publish.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("files-first", &[("value.txt", "B\n")]).await;

    admit(&server, "acme/files-first").await;
    wait_barrier(&probe.before_full_publish, 1).await;
    wait_count(&probe.files_publishes, 1).await;
    let held = exact_result(&server, "acme/files-first", &commit).await;
    assert!(held.head.is_some() && held.files.is_some());
    assert!(held.full.is_none());
    assert_eq!(
        pinned_status(&server, "acme/files-first", &commit, ExactResultKind::Files).await,
        reqwest::StatusCode::OK
    );
    probe.before_full_publish.release();
    wait_count(&probe.full_publishes, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_can_publish_while_files_is_running() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.before_files_publish.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("full-first", &[("value.txt", "B\n")]).await;

    admit(&server, "acme/full-first").await;
    wait_barrier(&probe.before_files_publish, 1).await;
    wait_count(&probe.full_publishes, 1).await;
    let held = exact_result(&server, "acme/full-first", &commit).await;
    assert!(held.head.is_some() && held.full.is_some());
    assert!(held.files.is_none());
    assert_eq!(
        pinned_status(&server, "acme/full-first", &commit, ExactResultKind::Full).await,
        reqwest::StatusCode::OK
    );
    probe.before_files_publish.release();
    wait_count(&probe.files_publishes, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_result_read_failure_starts_no_full_or_files_work() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.builder_entry.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let fail_next_read = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server =
        start_server_split_storage_failing_next_ref_read(Arc::clone(&fail_next_read)).await;
    let origin = make_origin("acme", "current-read-failure");
    let commit = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    register_added_without_build(&server, "acme/current-read-failure")
        .await
        .expect("register read-failure fixture");

    admit(&server, "acme/current-read-failure").await;
    wait_barrier(&probe.builder_entry, 1).await;
    fail_next_read.store(true, Ordering::SeqCst);
    probe.builder_entry.release();
    probe.builder_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_failure(1))
        .await
        .expect("current-result read failure settles");

    assert_eq!(probe.full_builds.load(Ordering::SeqCst), 0);
    assert_eq!(probe.files_builds.load(Ordering::SeqCst), 0);
    assert_eq!(probe.artifact_uploads.load(Ordering::SeqCst), 0);
    let result = exact_result(&server, "acme/current-read-failure", &commit).await;
    assert!(result.head.is_none() && result.full.is_none() && result.files.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_head_is_reused_while_parent_full_and_files_are_running() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.before_full_publish.arm();
    probe.before_files_publish.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, origin, parent) = fixture("parent-head", &[("value.txt", "B\n")]).await;

    admit(&server, "acme/parent-head").await;
    let ((), ()) = tokio::join!(
        wait_barrier(&probe.before_full_publish, 1),
        wait_barrier(&probe.before_files_publish, 1),
    );
    let child = origin.commit(&[("value.txt", "C\n")], "C");
    origin.publish();
    assert_eq!(admit(&server, "acme/parent-head").await, child);
    wait_count(&probe.head_publishes, 2).await;

    let parent_result = exact_result(&server, "acme/parent-head", &parent).await;
    assert!(parent_result.head.is_some());
    assert!(parent_result.full.is_none() && parent_result.files.is_none());
    let child_head = exact_result(&server, "acme/parent-head", &child)
        .await
        .head
        .expect("child Head result");
    assert_eq!(child_head.parent_commit.as_deref(), Some(parent.as_str()));
    assert_eq!(
        child_head.base_commit, parent,
        "the child Head reuses the ready parent Head base"
    );

    probe.before_full_publish.release();
    probe.before_files_publish.release();
    wait_count(&probe.full_publishes, 2).await;
    wait_count(&probe.files_publishes, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_failure_keeps_head_and_files_and_pinned_checks_never_enqueue() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("full-failure", &[("value.txt", "B\n")]).await;
    probe.fail_full_for(&commit);

    admit(&server, "acme/full-failure").await;
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_failure(1))
        .await
        .expect("Full failure settles");
    wait_for_failed_job(&server, "acme/full-failure", &commit).await;
    let failed = exact_result(&server, "acme/full-failure", &commit).await;
    assert!(failed.head.is_some() && failed.files.is_some());
    assert!(failed.full.is_none());
    assert_eq!(
        pinned_status(&server, "acme/full-failure", &commit, ExactResultKind::Head).await,
        reqwest::StatusCode::OK
    );
    assert_eq!(
        pinned_status(
            &server,
            "acme/full-failure",
            &commit,
            ExactResultKind::Files
        )
        .await,
        reqwest::StatusCode::OK
    );

    let jobs = probe.queue_inserts.load(Ordering::SeqCst);
    let fetches = probe.exact_fetches.load(Ordering::SeqCst);
    for _ in 0..5 {
        assert_eq!(
            pinned_status(&server, "acme/full-failure", &commit, ExactResultKind::Full).await,
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), jobs);
    assert_eq!(probe.exact_fetches.load(Ordering::SeqCst), fetches);

    probe.allow_full_for(&commit);
    let head_builds = probe.head_builds.load(Ordering::SeqCst);
    let files_builds = probe.files_builds.load(Ordering::SeqCst);
    admit(&server, "acme/full-failure").await;
    wait_count(&probe.full_publishes, 1).await;
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), jobs + 1);
    assert_eq!(probe.head_builds.load(Ordering::SeqCst), head_builds);
    assert_eq!(probe.files_builds.load(Ordering::SeqCst), files_builds);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn files_failure_keeps_head_and_full_and_retry_builds_only_files() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("files-failure", &[("value.txt", "B\n")]).await;
    probe.fail_files_for(&commit);

    admit(&server, "acme/files-failure").await;
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_failure(1))
        .await
        .expect("Files failure settles");
    wait_for_failed_job(&server, "acme/files-failure", &commit).await;
    let failed = exact_result(&server, "acme/files-failure", &commit).await;
    assert!(failed.head.is_some() && failed.full.is_some());
    assert!(failed.files.is_none());
    assert_eq!(
        pinned_status(
            &server,
            "acme/files-failure",
            &commit,
            ExactResultKind::Full
        )
        .await,
        reqwest::StatusCode::OK
    );

    probe.allow_files_for(&commit);
    let jobs = probe.queue_inserts.load(Ordering::SeqCst);
    let head_builds = probe.head_builds.load(Ordering::SeqCst);
    let full_builds = probe.full_builds.load(Ordering::SeqCst);
    let files_builds = probe.files_builds.load(Ordering::SeqCst);
    admit(&server, "acme/files-failure").await;
    wait_count(&probe.files_publishes, 1).await;
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), jobs + 1);
    assert_eq!(probe.head_builds.load(Ordering::SeqCst), head_builds);
    assert_eq!(probe.full_builds.load(Ordering::SeqCst), full_builds);
    assert_eq!(probe.files_builds.load(Ordering::SeqCst), files_builds + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_after_head_restarts_without_rebuilding_head() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.after_head_entry.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("stopped-head", &[("value.txt", "B\n")]).await;

    admit(&server, "acme/stopped-head").await;
    wait_barrier(&probe.after_head_entry, 1).await;
    let held = exact_result(&server, "acme/stopped-head", &commit).await;
    assert!(held.head.is_some() && held.full.is_none() && held.files.is_none());
    stop_current_job(&server, "acme/stopped-head", &commit).await;
    probe.after_head_entry.release();
    probe.after_head_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_failure(1))
        .await
        .expect("stale Head owner exits");

    let head_builds = probe.head_builds.load(Ordering::SeqCst);
    let jobs = probe.queue_inserts.load(Ordering::SeqCst);
    admit(&server, "acme/stopped-head").await;
    wait_count(&probe.full_publishes, 1).await;
    wait_count(&probe.files_publishes, 1).await;
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), jobs + 1);
    assert_eq!(probe.head_builds.load(Ordering::SeqCst), head_builds);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_after_full_restarts_without_rebuilding_head_or_full() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.after_full_publish.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("stopped-full", &[("value.txt", "B\n")]).await;
    probe.fail_files_for(&commit);

    admit(&server, "acme/stopped-full").await;
    wait_barrier(&probe.after_full_publish, 1).await;
    let held = exact_result(&server, "acme/stopped-full", &commit).await;
    assert!(held.head.is_some() && held.full.is_some() && held.files.is_none());
    stop_current_job(&server, "acme/stopped-full", &commit).await;
    probe.allow_files_for(&commit);
    probe.after_full_publish.release();
    probe.after_full_publish.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_failure(1))
        .await
        .expect("stale Full owner exits");

    let jobs = probe.queue_inserts.load(Ordering::SeqCst);
    let head_builds = probe.head_builds.load(Ordering::SeqCst);
    let full_builds = probe.full_builds.load(Ordering::SeqCst);
    admit(&server, "acme/stopped-full").await;
    wait_count(&probe.files_publishes, 1).await;
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), jobs + 1);
    assert_eq!(probe.head_builds.load(Ordering::SeqCst), head_builds);
    assert_eq!(probe.full_builds.load(Ordering::SeqCst), full_builds);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_after_files_restarts_without_rebuilding_head_or_files() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.after_files_publish.arm();
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("stopped-files", &[("value.txt", "B\n")]).await;
    probe.fail_full_for(&commit);

    admit(&server, "acme/stopped-files").await;
    wait_barrier(&probe.after_files_publish, 1).await;
    let held = exact_result(&server, "acme/stopped-files", &commit).await;
    assert!(held.head.is_some() && held.files.is_some() && held.full.is_none());
    stop_current_job(&server, "acme/stopped-files", &commit).await;
    probe.allow_full_for(&commit);
    probe.after_files_publish.release();
    probe.after_files_publish.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_failure(1))
        .await
        .expect("stale Files owner exits");

    let jobs = probe.queue_inserts.load(Ordering::SeqCst);
    let head_builds = probe.head_builds.load(Ordering::SeqCst);
    let files_builds = probe.files_builds.load(Ordering::SeqCst);
    admit(&server, "acme/stopped-files").await;
    wait_count(&probe.full_publishes, 1).await;
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), jobs + 1);
    assert_eq!(probe.head_builds.load(Ordering::SeqCst), head_builds);
    assert_eq!(probe.files_builds.load(Ordering::SeqCst), files_builds);
}

#[tokio::test]
async fn committed_empty_tree_has_ready_files_with_zero_archive_chunks() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "empty-tree");
    let commit = origin.empty_commit("committed empty tree");
    origin.publish();
    register_added_without_build(&server, "acme/empty-tree")
        .await
        .expect("register empty tree");

    admit(&server, "acme/empty-tree").await;
    wait_count(&probe.files_publishes, 1).await;
    let exact = exact_result(&server, "acme/empty-tree", &commit).await;
    let files = exact.files.expect("Files result");
    assert!(files.archive_chunks.is_empty());
    assert_eq!(
        pinned_status(&server, "acme/empty-tree", &commit, ExactResultKind::Files).await,
        reqwest::StatusCode::OK
    );
    let (_guard, checkout) = clone_only(&server, "acme", "empty-tree", 0, CloneMode::Files)
        .await
        .expect("clone committed empty tree as Files");
    assert!(std::fs::read_dir(checkout).unwrap().next().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_head_preserves_ready_full_and_files() {
    let _lock = env_lock().lock().await;
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let (server, _origin, commit) = fixture("missing-head", &[("value.txt", "B\n")]).await;

    admit(&server, "acme/missing-head").await;
    wait_count(&probe.full_publishes, 1).await;
    wait_count(&probe.files_publishes, 1).await;
    let repo_id = RepoId::github("acme/missing-head");
    let store = server_ref_store(&server).await;
    let mut malformed = store.load_result(&repo_id, &commit).await.unwrap().unwrap();
    malformed.head = None;
    store.save_result(&repo_id, &malformed).await.unwrap();

    let head_builds = probe.head_builds.load(Ordering::SeqCst);
    let full_builds = probe.full_builds.load(Ordering::SeqCst);
    let files_builds = probe.files_builds.load(Ordering::SeqCst);
    admit(&server, "acme/missing-head").await;
    wait_count(&probe.head_builds, head_builds + 1).await;
    wait_count(&probe.head_publishes, 2).await;
    let rebuilt = exact_result(&server, "acme/missing-head", &commit).await;
    assert!(rebuilt.head.is_some() && rebuilt.full.is_some() && rebuilt.files.is_some());
    assert_eq!(probe.full_builds.load(Ordering::SeqCst), full_builds);
    assert_eq!(probe.files_builds.load(Ordering::SeqCst), files_builds);
}
