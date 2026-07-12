use std::sync::Arc;

use ripclone::meta::{SqlRefStore, SqliteMeta};
use ripclone::provider::RepoId;
use ripclone::ref_store::{
    AddedRepo, AddedRepoSource, CachingRefStore, FileRefStore, RefStore, RepoLifecycleState,
};

fn attempt(repo_id: RepoId, id: &str, branch: &str) -> AddedRepo {
    AddedRepo {
        repo_id,
        added_at: 1,
        history_enabled: true,
        source: AddedRepoSource::Api,
        repo_size_bytes: None,
        state: RepoLifecycleState::Initializing,
        initialization_branch: Some(branch.into()),
        initialization_target: None,
        activated_at: None,
        failure: None,
        initialization_attempt_id: Some(id.into()),
    }
}

async fn exercise(store: Arc<dyn RefStore>, suffix: &str) {
    let repo = RepoId::github(format!("admission/{suffix}"));

    // Two delayed /add preflights may race. Exactly one insert wins and the
    // loser cannot overwrite its immutable attempt token.
    let a = attempt(repo.clone(), "attempt-a", "HEAD");
    let b = attempt(repo.clone(), "attempt-b", "HEAD");
    let (ra, rb) = tokio::join!(
        store.begin_repo_initialization(&a),
        store.begin_repo_initialization(&b)
    );
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    assert_ne!(ra, rb, "exactly one concurrent admission must win");
    let winner = if ra { "attempt-a" } else { "attempt-b" };
    let loser = if ra { "attempt-b" } else { "attempt-a" };
    assert_eq!(
        store
            .load_added_repo(&repo)
            .await
            .unwrap()
            .unwrap()
            .initialization_attempt_id
            .as_deref(),
        Some(winner)
    );

    // HEAD canonicalization must not make a delayed terminal report miss.
    assert!(
        store
            .pin_repo_initialization(&repo, "main", "same-sha", Some(winner))
            .await
            .unwrap()
    );
    assert!(
        store
            .fail_repo_initialization(
                &repo,
                "HEAD",
                Some("same-sha"),
                "first failed",
                Some(winner)
            )
            .await
            .unwrap()
    );

    // Retry at the same SHA receives a new attempt. The old completion is stale
    // even though branch and commit are identical.
    let retry = attempt(repo.clone(), "attempt-retry", "HEAD");
    assert!(store.begin_repo_initialization(&retry).await.unwrap());
    assert!(
        store
            .pin_repo_initialization(&repo, "main", "same-sha", Some("attempt-retry"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .fail_repo_initialization(
                &repo,
                "main",
                Some("same-sha"),
                "stale delayed failure",
                Some(winner),
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .activate_repo(&repo, "main", "same-sha", Some(loser))
            .await
            .unwrap()
    );

    // Deterministically force failure to land before activation. Verified
    // readiness for the same immutable attempt upgrades Failed -> Active.
    let release_activation = Arc::new(tokio::sync::Notify::new());
    let s1 = store.clone();
    let r1 = repo.clone();
    let release = release_activation.clone();
    let activate = tokio::spawn(async move {
        release.notified().await;
        s1.activate_repo(&r1, "main", "same-sha", Some("attempt-retry"))
            .await
            .unwrap()
    });
    assert!(
        store
            .fail_repo_initialization(
                &repo,
                "HEAD",
                Some("same-sha"),
                "racing failure",
                Some("attempt-retry"),
            )
            .await
            .unwrap()
    );
    release_activation.notify_one();
    assert!(activate.await.unwrap());
    let terminal = store.load_added_repo(&repo).await.unwrap().unwrap().state;
    assert_eq!(terminal, RepoLifecycleState::Active);
    assert!(
        !store
            .fail_repo_initialization(
                &repo,
                "HEAD",
                Some("same-sha"),
                "late after ready",
                Some("attempt-retry"),
            )
            .await
            .unwrap()
    );

    // Force the opposite ordering on a separate attempt: activation first,
    // then a released failure. Active is monotonic and cannot be demoted.
    let ready_first = RepoId::github(format!("admission/{suffix}-ready-first"));
    let ready_attempt = attempt(ready_first.clone(), "ready-first", "HEAD");
    assert!(
        store
            .begin_repo_initialization(&ready_attempt)
            .await
            .unwrap()
    );
    assert!(
        store
            .pin_repo_initialization(&ready_first, "main", "ready-sha", Some("ready-first"))
            .await
            .unwrap()
    );
    let release_failure = Arc::new(tokio::sync::Notify::new());
    let failing_store = store.clone();
    let failing_repo = ready_first.clone();
    let release = release_failure.clone();
    let delayed_failure = tokio::spawn(async move {
        release.notified().await;
        failing_store
            .fail_repo_initialization(
                &failing_repo,
                "HEAD",
                Some("ready-sha"),
                "late",
                Some("ready-first"),
            )
            .await
            .unwrap()
    });
    assert!(
        store
            .activate_repo(&ready_first, "main", "ready-sha", Some("ready-first"))
            .await
            .unwrap()
    );
    release_failure.notify_one();
    assert!(!delayed_failure.await.unwrap());
    assert_eq!(
        store
            .load_added_repo(&ready_first)
            .await
            .unwrap()
            .unwrap()
            .state,
        RepoLifecycleState::Active
    );

    // A delayed repeated /add never demotes an active row and never resets a
    // failed row unless it carries a distinct explicit retry attempt.
    assert!(!store.begin_repo_initialization(&retry).await.unwrap());
    assert!(
        !store
            .begin_repo_initialization(&attempt(repo.clone(), "late-add", "HEAD"))
            .await
            .unwrap()
    );
    assert_eq!(
        store.load_added_repo(&repo).await.unwrap().unwrap().state,
        RepoLifecycleState::Active
    );
}

#[tokio::test]
async fn caching_file_admission_is_atomic_and_attempt_guarded() {
    let tmp = tempfile::tempdir().unwrap();
    let left: Arc<dyn RefStore> = Arc::new(CachingRefStore::new(FileRefStore::new(tmp.path())));
    let right: Arc<dyn RefStore> = Arc::new(CachingRefStore::new(FileRefStore::new(tmp.path())));
    let repo = RepoId::github("admission/file-cross-instance");
    let left_attempt = attempt(repo.clone(), "left", "HEAD");
    let right_attempt = attempt(repo, "right", "HEAD");
    let (a, b) = tokio::join!(
        left.begin_repo_initialization(&left_attempt),
        right.begin_repo_initialization(&right_attempt),
    );
    assert_ne!(a.unwrap(), b.unwrap());
    exercise(left, "file").await;
}

#[tokio::test]
async fn sqlite_admission_is_atomic_and_attempt_guarded_across_store_instances() {
    let tmp = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", tmp.path().join("meta.db").display());
    let left: Arc<dyn RefStore> = Arc::new(
        SqlRefStore::new(Box::new(SqliteMeta::connect(&url).await.unwrap()))
            .await
            .unwrap(),
    );
    // Exercise a second independent pool against the same DB through the same
    // test by racing it explicitly before the full lifecycle.
    let right: Arc<dyn RefStore> = Arc::new(
        SqlRefStore::new(Box::new(SqliteMeta::connect(&url).await.unwrap()))
            .await
            .unwrap(),
    );
    let repo = RepoId::github("admission/sql-cross-process");
    let left_attempt = attempt(repo.clone(), "left", "HEAD");
    let right_attempt = attempt(repo.clone(), "right", "HEAD");
    let (a, b) = tokio::join!(
        left.begin_repo_initialization(&left_attempt),
        right.begin_repo_initialization(&right_attempt),
    );
    assert_ne!(a.unwrap(), b.unwrap());

    exercise(left, "sqlite").await;
}
