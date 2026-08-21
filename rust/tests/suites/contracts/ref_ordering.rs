//! Concurrent exact-result writes against the server control database.

use ripclone::control::ControlDb;
use ripclone::provider::RepoId;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_control_keeps_every_exact_commit_independent() {
    let directory = tempfile::tempdir().unwrap();
    let control = Arc::new(
        ControlDb::open(
            &directory.path().join("control.db"),
            None,
            ripclone::queue::default_size_classes(),
        )
        .await
        .unwrap(),
    );
    let repo = RepoId::github("acme/ref-ordering");
    let mut writes = Vec::new();
    for ordinal in (1..=64).rev() {
        let store = control.ref_store();
        let repo = repo.clone();
        writes.push(tokio::spawn(async move {
            store
                .save_result(
                    &repo,
                    &ripclone::RefInfo {
                        commit: format!("{ordinal:040x}"),
                        synced_at: Some(1000 - ordinal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }));
    }
    for write in writes {
        write.await.unwrap();
    }
    let commits = control.ref_store().list_commits(&repo).await.unwrap();
    assert_eq!(commits.len(), 64);
    for ordinal in 1..=64 {
        let commit = format!("{ordinal:040x}");
        assert_eq!(
            control
                .ref_store()
                .load_result(&repo, &commit)
                .await
                .unwrap()
                .unwrap()
                .commit,
            commit
        );
    }
}
