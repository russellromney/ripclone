//! Behavior every [`ControlStore`] must have. A backend passes by running
//! [`run_all`] with a factory for empty backends.

use super::{Clock, ControlStore, Out, Output, Pool};
use crate::provider::{ProviderInstanceId, RepoId};
use crate::repo_config::RepoConfig;
use crate::{ClonepackArtifacts, ExactResultKind, FilesResult, FullResult, HeadResult};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

pub const MAX_ATTEMPTS: i64 = 3;
const LEASE: Duration = Duration::from_secs(60);
const START: i64 = 1_000;
const MAIN: &str = "refs/heads/main";

pub struct ManualClock(AtomicI64);

impl ManualClock {
    pub fn advance(&self, secs: i64) {
        self.0.fetch_add(secs, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// One empty backing store. `open` may be called again after the previous
/// store is dropped and must then see the same data.
#[async_trait]
pub trait Backend: Send + Sync {
    async fn open(
        &self,
        clock: Arc<ManualClock>,
        max_attempts: i64,
    ) -> Result<Arc<dyn ControlStore>>;
}

pub async fn run_all<B: Backend>(new_backend: impl Fn() -> B) {
    macro_rules! run {
        ($($test:ident),* $(,)?) => {$(
            eprintln!("control store conformance: {}", stringify!($test));
            $test(&new_backend()).await;
        )*};
    }
    run!(
        admits_coalesce_to_one_row_per_commit,
        ref_name_is_filled_never_replaced,
        pools_offer_head_then_background,
        claims_take_the_oldest_row,
        expired_lease_is_reclaimed_from_its_old_owner,
        renew_extends_the_lease,
        retry_after_gates_claims,
        failed_head_fails_every_output,
        retryable_failure_on_last_attempt_fails,
        background_outputs_fail_independently,
        retry_touches_only_failed_outputs,
        exhausted_attempts_are_given_up,
        stale_claim_cannot_overwrite_any_ready_result,
        reset_leases_frees_every_row,
        latest_ready_full_is_newest_ready_on_the_ref,
        concurrent_claims_never_share_a_row,
        state_survives_restart,
        repos_are_added_listed_and_removed,
        config_defaults_without_writing,
    );
}

async fn open(backend: &dyn Backend) -> (Arc<ManualClock>, Arc<dyn ControlStore>) {
    let clock = Arc::new(ManualClock(AtomicI64::new(START)));
    let store = backend.open(clock.clone(), MAX_ATTEMPTS).await.unwrap();
    (clock, store)
}

fn repo() -> RepoId {
    RepoId::github("acme/widget")
}

fn sha(n: u32) -> String {
    format!("{n:040x}")
}

fn artifacts(commit: &str, label: &str) -> ClonepackArtifacts {
    let hash = |suffix: &str| crate::cas::hash(format!("{commit}-{label}-{suffix}").as_bytes());
    ClonepackArtifacts {
        manifest: hash("manifest"),
        metadata_chunk: hash("metadata"),
        skeleton_pack: hash("skeleton-pack"),
        skeleton_idx: hash("skeleton-idx"),
        prebuilt_index: hash("index"),
        idx_bundle: hash("idx-bundle"),
        commit: commit.to_string(),
        ..Default::default()
    }
}

fn head(commit: &str, label: &str) -> Output {
    Output::Head {
        result: HeadResult {
            clonepack: artifacts(commit, label),
            ..Default::default()
        },
        parent: Some(sha(0xfeed)),
    }
}

fn files(commit: &str, label: &str) -> Output {
    Output::Files(FilesResult {
        clonepack: artifacts(commit, label),
        ..Default::default()
    })
}

fn full(commit: &str, label: &str) -> Output {
    Output::Full(FullResult {
        clonepack: artifacts(commit, label),
        ..Default::default()
    })
}

fn manifest(commit: &str, label: &str) -> String {
    artifacts(commit, label).manifest
}

trait Clonepack {
    fn clonepack(&self) -> &ClonepackArtifacts;
}

impl Clonepack for HeadResult {
    fn clonepack(&self) -> &ClonepackArtifacts {
        &self.clonepack
    }
}

impl Clonepack for FilesResult {
    fn clonepack(&self) -> &ClonepackArtifacts {
        &self.clonepack
    }
}

impl Clonepack for FullResult {
    fn clonepack(&self) -> &ClonepackArtifacts {
        &self.clonepack
    }
}

fn ready_manifest<T: Clonepack>(out: &Out<T>) -> Option<String> {
    out.ready()
        .map(|output| output.clonepack().manifest.clone())
}

fn pending<T>(error: Option<&str>) -> Out<T> {
    Out::Pending {
        error: error.map(str::to_string),
    }
}

fn failed<T>(error: &str) -> Out<T> {
    Out::Failed(error.to_string())
}

fn is<T>(out: &Out<T>, expected: &Out<T>) -> bool {
    match (out, expected) {
        (Out::Pending { error: a }, Out::Pending { error: b }) => a == b,
        (Out::Failed(a), Out::Failed(b)) => a == b,
        (Out::Ready(_), Out::Ready(_)) => true,
        _ => false,
    }
}

macro_rules! assert_out {
    ($out:expr, $expected:expr) => {
        assert!(
            is(&$out, &$expected),
            "{} is {:?}",
            stringify!($out),
            match &$out {
                Out::Pending { error } => format!("pending {error:?}"),
                Out::Ready(_) => "ready".to_string(),
                Out::Failed(error) => format!("failed {error:?}"),
            }
        )
    };
}

/// Admit `commit` and publish all three outputs through claims.
async fn complete(store: &dyn ControlStore, repo: &RepoId, commit: &str, ref_name: Option<&str>) {
    store.admit(repo, commit, ref_name).await.unwrap();
    let claim = store
        .claim("builder", Pool::Head, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.build.commit, commit);
    assert!(
        store
            .publish(&claim, head(commit, "head"))
            .await
            .unwrap()
            .applied
    );
    store.release(&claim).await.unwrap();
    let claim = store
        .claim("builder", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.build.commit, commit);
    assert!(
        store
            .publish(&claim, files(commit, "files"))
            .await
            .unwrap()
            .applied
    );
    assert!(
        store
            .publish(&claim, full(commit, "full"))
            .await
            .unwrap()
            .applied
    );
    store.release(&claim).await.unwrap();
}

async fn admits_coalesce_to_one_row_per_commit(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    let refs = [Some(MAIN), Some("refs/heads/release"), None];
    let mut admits = Vec::new();
    for index in 0..32 {
        let store = store.clone();
        let commit = commit.clone();
        admits.push(tokio::spawn(async move {
            store
                .admit(&repo(), &commit, refs[index % refs.len()])
                .await
                .unwrap()
        }));
    }
    let mut inserted = 0;
    for admit in admits {
        let (row, was_inserted) = admit.await.unwrap();
        assert_eq!(row.commit, commit);
        inserted += usize::from(was_inserted);
    }
    assert_eq!(inserted, 1);
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert!(row.ref_name.is_some());
    assert_eq!(row.created_at, START);
    assert_eq!(row.attempts, 0);
    assert_out!(row.head, pending(None));
    assert_out!(row.files, pending(None));
    assert_out!(row.full, pending(None));

    assert!(store.admit(&repo(), &sha(2), None).await.unwrap().1);
    let other_repo = RepoId::github("acme/other");
    assert!(store.admit(&other_repo, &commit, None).await.unwrap().1);
    assert!(store.status(&repo(), &sha(3)).await.unwrap().is_none());
    assert!(store.admit(&repo(), "HEAD~1", None).await.is_err());
    assert!(store.admit(&repo(), &sha(4), Some("main")).await.is_err());
}

async fn ref_name_is_filled_never_replaced(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    let (row, inserted) = store.admit(&repo(), &commit, None).await.unwrap();
    assert!(inserted);
    assert_eq!(row.ref_name, None);
    let (row, inserted) = store.admit(&repo(), &commit, Some(MAIN)).await.unwrap();
    assert!(!inserted);
    assert_eq!(row.ref_name.as_deref(), Some(MAIN));
    let (row, _) = store
        .admit(&repo(), &commit, Some("refs/heads/release"))
        .await
        .unwrap();
    assert_eq!(row.ref_name.as_deref(), Some(MAIN));
    let (row, _) = store.admit(&repo(), &commit, None).await.unwrap();
    assert_eq!(row.ref_name.as_deref(), Some(MAIN));
}

async fn pools_offer_head_then_background(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, Some(MAIN)).await.unwrap();
    assert!(
        store
            .claim("b", Pool::Background, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.repo, repo());
    assert_eq!(claim.owner, "h");
    assert_eq!(claim.build.commit, commit);
    assert_eq!(claim.build.ref_name.as_deref(), Some(MAIN));
    assert_eq!(claim.build.attempts, 1);
    assert_eq!(claim.build.lease_until, Some(START + 60));
    assert!(
        store
            .claim("h2", Pool::Head, LEASE)
            .await
            .unwrap()
            .is_none()
    );

    assert!(
        store
            .publish(&claim, head(&sha(9), "wrong-commit"))
            .await
            .is_err()
    );
    let published = store.publish(&claim, head(&commit, "head")).await.unwrap();
    assert!(published.applied);
    assert!(!published.row_complete);
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_eq!(row.parent, Some(sha(0xfeed)));
    assert_eq!(
        row.attempts, 0,
        "publishing Head starts the Background phase"
    );
    assert!(
        store
            .claim("b", Pool::Background, LEASE)
            .await
            .unwrap()
            .is_none(),
        "the Head lease still holds the row"
    );
    store.release(&claim).await.unwrap();
    assert_eq!(
        store
            .status(&repo(), &commit)
            .await
            .unwrap()
            .unwrap()
            .lease_until,
        None
    );
    assert!(store.claim("h", Pool::Head, LEASE).await.unwrap().is_none());

    let claim = store
        .claim("b", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.build.attempts, 1);
    assert!(claim.build.head.ready().is_some());
    let published = store
        .publish(&claim, files(&commit, "files"))
        .await
        .unwrap();
    assert!(published.applied && !published.row_complete);
    let published = store.publish(&claim, full(&commit, "full")).await.unwrap();
    assert!(published.applied && published.row_complete);
    store.release(&claim).await.unwrap();
    assert!(store.claim("h", Pool::Head, LEASE).await.unwrap().is_none());
    assert!(
        store
            .claim("b", Pool::Background, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.files), Some(manifest(&commit, "files")));
    assert_eq!(ready_manifest(&row.full), Some(manifest(&commit, "full")));
}

async fn claims_take_the_oldest_row(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    store.admit(&repo(), &sha(2), None).await.unwrap();
    clock.advance(1);
    store.admit(&repo(), &sha(1), None).await.unwrap();
    for expected in [sha(2), sha(1)] {
        let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.commit, expected);
    }
    assert!(store.claim("h", Pool::Head, LEASE).await.unwrap().is_none());
}

async fn expired_lease_is_reclaimed_from_its_old_owner(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let old = store
        .claim("old", Pool::Head, LEASE)
        .await
        .unwrap()
        .unwrap();
    clock.advance(59);
    assert!(
        store
            .claim("new", Pool::Head, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    clock.advance(1);
    let new = store
        .claim("new", Pool::Head, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new.build.commit, commit);
    assert_eq!(new.build.attempts, 2);

    assert!(!store.renew(&old, LEASE).await.unwrap());
    assert!(
        !store
            .publish(&old, head(&commit, "old"))
            .await
            .unwrap()
            .applied
    );
    store
        .fail(&old, ExactResultKind::Head, "old failed", None)
        .await
        .unwrap();
    store.release(&old).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, pending(None));
    assert_eq!(row.lease_until, Some(START + 120));
    assert!(
        store
            .claim("third", Pool::Head, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .publish(&new, head(&commit, "new"))
            .await
            .unwrap()
            .applied
    );
}

async fn renew_extends_the_lease(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    store.admit(&repo(), &sha(1), None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    clock.advance(50);
    assert!(store.renew(&claim, LEASE).await.unwrap());
    clock.advance(50);
    assert!(store.claim("b", Pool::Head, LEASE).await.unwrap().is_none());
    clock.advance(10);
    assert!(store.claim("b", Pool::Head, LEASE).await.unwrap().is_some());
}

async fn retry_after_gates_claims(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store
        .fail(
            &claim,
            ExactResultKind::Head,
            "flaky",
            Some(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, pending(Some("flaky")));
    assert_out!(row.files, pending(None));
    clock.advance(29);
    assert!(store.claim("a", Pool::Head, LEASE).await.unwrap().is_none());
    clock.advance(1);
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.attempts, 2);
    assert!(
        store
            .publish(&claim, head(&commit, "head"))
            .await
            .unwrap()
            .applied
    );
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, Out::Ready(HeadResult::default()));
}

async fn failed_head_fails_every_output(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store
        .fail(&claim, ExactResultKind::Head, "bad object", None)
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, failed("bad object"));
    assert_out!(row.files, failed("head failed: bad object"));
    assert_out!(row.full, failed("head failed: bad object"));
    assert!(store.claim("a", Pool::Head, LEASE).await.unwrap().is_none());
    assert!(
        store
            .claim("a", Pool::Background, LEASE)
            .await
            .unwrap()
            .is_none()
    );
}

async fn retryable_failure_on_last_attempt_fails(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    for attempt in 1..=MAX_ATTEMPTS {
        let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.attempts, attempt);
        store
            .fail(
                &claim,
                ExactResultKind::Head,
                &format!("flaky {attempt}"),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap();
        store.release(&claim).await.unwrap();
        clock.advance(1);
    }
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, failed("flaky 3"));
    assert_out!(row.full, failed("head failed: flaky 3"));
    assert!(store.claim("a", Pool::Head, LEASE).await.unwrap().is_none());
}

async fn background_outputs_fail_independently(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    store.publish(&claim, head(&commit, "head")).await.unwrap();
    store.release(&claim).await.unwrap();
    let claim = store
        .claim("b", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    store
        .fail(&claim, ExactResultKind::Full, "out of memory", None)
        .await
        .unwrap();
    store
        .fail(
            &claim,
            ExactResultKind::Head,
            "ignored: Head is ready",
            None,
        )
        .await
        .unwrap();
    let published = store
        .publish(&claim, files(&commit, "files"))
        .await
        .unwrap();
    assert!(published.applied && !published.row_complete);
    store.release(&claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_eq!(ready_manifest(&row.files), Some(manifest(&commit, "files")));
    assert_out!(row.full, failed("out of memory"));
    assert!(
        store
            .claim("b", Pool::Background, LEASE)
            .await
            .unwrap()
            .is_none()
    );
}

async fn retry_touches_only_failed_outputs(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    assert!(store.retry(&repo(), &sha(9)).await.unwrap().is_none());

    // Head and Files ready, Full failed.
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    store.publish(&claim, head(&commit, "head")).await.unwrap();
    store.release(&claim).await.unwrap();
    let claim = store
        .claim("b", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    store
        .publish(&claim, files(&commit, "files"))
        .await
        .unwrap();
    store
        .fail(&claim, ExactResultKind::Full, "out of memory", None)
        .await
        .unwrap();
    store.release(&claim).await.unwrap();

    let row = store.retry(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_eq!(ready_manifest(&row.files), Some(manifest(&commit, "files")));
    assert_out!(row.full, pending(None));
    assert_eq!(row.attempts, 0);
    let claim = store
        .claim("b", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !store
            .publish(&claim, files(&commit, "again"))
            .await
            .unwrap()
            .applied
    );
    assert!(
        store
            .publish(&claim, full(&commit, "full"))
            .await
            .unwrap()
            .row_complete
    );

    // Nothing failed: the row, including attempts and backoff, is unchanged.
    let other = sha(2);
    store.admit(&repo(), &other, None).await.unwrap();
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    store
        .fail(
            &claim,
            ExactResultKind::Head,
            "flaky",
            Some(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    let row = store.retry(&repo(), &other).await.unwrap().unwrap();
    assert_eq!(row.attempts, 1);
    assert_out!(row.head, pending(Some("flaky")));
    assert!(store.claim("h", Pool::Head, LEASE).await.unwrap().is_none());

    // A failed Head comes back with everything it took down.
    let third = sha(3);
    store.admit(&repo(), &third, None).await.unwrap();
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, third);
    store
        .fail(&claim, ExactResultKind::Head, "bad", None)
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    let row = store.retry(&repo(), &third).await.unwrap().unwrap();
    assert_out!(row.head, pending(None));
    assert_out!(row.files, pending(None));
    assert_out!(row.full, pending(None));
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, third);
}

async fn exhausted_attempts_are_given_up(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    // First attempt fails retryably; the next two die holding the lease.
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store
        .fail(
            &claim,
            ExactResultKind::Head,
            "flaky",
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    clock.advance(1);
    let mut last = None;
    for attempt in 2..=MAX_ATTEMPTS {
        let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.attempts, attempt);
        last = Some(claim);
        clock.advance(60);
    }
    assert!(store.claim("a", Pool::Head, LEASE).await.unwrap().is_none());
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, failed("gave up after 3 attempts: flaky"));
    assert_out!(
        row.files,
        failed("head failed: gave up after 3 attempts: flaky")
    );
    assert_out!(
        row.full,
        failed("head failed: gave up after 3 attempts: flaky")
    );
    assert_eq!(row.lease_until, None);
    let last = last.unwrap();
    assert!(
        !store
            .publish(&last, head(&commit, "late"))
            .await
            .unwrap()
            .applied
    );

    // Background phase: Head stays ready, only the pending outputs fail.
    let other = sha(2);
    store.admit(&repo(), &other, None).await.unwrap();
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    store.publish(&claim, head(&other, "head")).await.unwrap();
    store.release(&claim).await.unwrap();
    let claim = store
        .claim("b", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    store.publish(&claim, files(&other, "files")).await.unwrap();
    clock.advance(60);
    for _ in 2..=MAX_ATTEMPTS {
        store
            .claim("b", Pool::Background, LEASE)
            .await
            .unwrap()
            .unwrap();
        clock.advance(60);
    }
    assert!(
        store
            .claim("b", Pool::Background, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    let row = store.status(&repo(), &other).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&other, "head")));
    assert_eq!(ready_manifest(&row.files), Some(manifest(&other, "files")));
    assert_out!(row.full, failed("gave up after 3 attempts"));
}

async fn stale_claim_cannot_overwrite_any_ready_result(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let first = store
        .claim("first", Pool::Head, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .publish(&first, head(&commit, "head"))
            .await
            .unwrap()
            .applied
    );
    store.release(&first).await.unwrap();
    let second = store
        .claim("second", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .publish(&second, files(&commit, "files"))
            .await
            .unwrap()
            .applied
    );
    assert!(
        store
            .publish(&second, full(&commit, "full"))
            .await
            .unwrap()
            .applied
    );

    for output in [
        head(&commit, "stale"),
        files(&commit, "stale"),
        full(&commit, "stale"),
    ] {
        assert!(!store.publish(&first, output).await.unwrap().applied);
    }
    for output in [
        head(&commit, "again"),
        files(&commit, "again"),
        full(&commit, "again"),
    ] {
        assert!(!store.publish(&second, output).await.unwrap().applied);
    }
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_eq!(ready_manifest(&row.files), Some(manifest(&commit, "files")));
    assert_eq!(ready_manifest(&row.full), Some(manifest(&commit, "full")));
}

async fn reset_leases_frees_every_row(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    store.admit(&repo(), &sha(1), None).await.unwrap();
    store.admit(&repo(), &sha(2), None).await.unwrap();
    store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    assert!(store.claim("b", Pool::Head, LEASE).await.unwrap().is_none());
    assert_eq!(store.reset_leases().await.unwrap(), 2);
    assert_eq!(store.reset_leases().await.unwrap(), 0);
    for _ in 0..2 {
        let claim = store.claim("b", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.attempts, 2);
    }
}

async fn latest_ready_full_is_newest_ready_on_the_ref(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let other_repo = RepoId::github("acme/other");
    assert!(
        store
            .latest_ready_full(&repo(), MAIN)
            .await
            .unwrap()
            .is_none()
    );
    complete(store.as_ref(), &repo(), &sha(1), Some(MAIN)).await;
    clock.advance(100);
    complete(store.as_ref(), &repo(), &sha(2), Some(MAIN)).await;
    clock.advance(100);
    complete(store.as_ref(), &repo(), &sha(3), Some("refs/heads/dev")).await;
    complete(store.as_ref(), &other_repo, &sha(4), Some(MAIN)).await;
    complete(store.as_ref(), &repo(), &sha(5), None).await;
    clock.advance(100);
    store.admit(&repo(), &sha(6), Some(MAIN)).await.unwrap();

    let (commit, result) = store
        .latest_ready_full(&repo(), MAIN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(commit, sha(2));
    assert_eq!(result.clonepack.manifest, manifest(&sha(2), "full"));
    assert_eq!(
        store
            .latest_ready_full(&repo(), "refs/heads/dev")
            .await
            .unwrap()
            .unwrap()
            .0,
        sha(3)
    );
    assert_eq!(
        store
            .latest_ready_full(&other_repo, MAIN)
            .await
            .unwrap()
            .unwrap()
            .0,
        sha(4)
    );
    assert!(
        store
            .latest_ready_full(&repo(), "refs/heads/none")
            .await
            .unwrap()
            .is_none()
    );
}

async fn concurrent_claims_never_share_a_row(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let rows = 16;
    for n in 0..rows {
        store.admit(&repo(), &sha(n), None).await.unwrap();
    }
    let mut claims = Vec::new();
    for worker in 0..(rows * 2) {
        let store = store.clone();
        claims.push(tokio::spawn(async move {
            store
                .claim(&format!("worker-{worker}"), Pool::Head, LEASE)
                .await
                .unwrap()
        }));
    }
    let mut claimed = HashSet::new();
    for claim in claims {
        if let Some(claim) = claim.await.unwrap() {
            assert_eq!(claim.build.attempts, 1);
            assert!(
                claimed.insert(claim.build.commit.clone()),
                "{} claimed twice",
                claim.build.commit
            );
        }
    }
    assert_eq!(claimed.len(), rows as usize);
}

async fn state_survives_restart(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    let config = RepoConfig {
        compression_level: Some(3),
        ..Default::default()
    };
    store.add_repo(&repo()).await.unwrap();
    store.set_config(&repo(), &config).await.unwrap();
    store.admit(&repo(), &commit, Some(MAIN)).await.unwrap();
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    store.publish(&claim, head(&commit, "head")).await.unwrap();
    store.release(&claim).await.unwrap();
    let claim = store
        .claim("b", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    store.publish(&claim, full(&commit, "full")).await.unwrap();
    store
        .fail(
            &claim,
            ExactResultKind::Files,
            "flaky",
            Some(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    drop(claim);
    drop(store);

    let store = backend.open(clock.clone(), MAX_ATTEMPTS).await.unwrap();
    store.health().await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(row.ref_name.as_deref(), Some(MAIN));
    assert_eq!(row.parent, Some(sha(0xfeed)));
    assert_eq!(row.created_at, START);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.lease_until, Some(START + 60));
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_eq!(ready_manifest(&row.full), Some(manifest(&commit, "full")));
    assert_out!(row.files, pending(Some("flaky")));
    assert_eq!(
        store.get_repo(&repo()).await.unwrap().unwrap().added_at,
        START as u64
    );
    assert_eq!(store.get_config(&repo()).await.unwrap(), config);
    assert_eq!(
        store
            .latest_ready_full(&repo(), MAIN)
            .await
            .unwrap()
            .unwrap()
            .0,
        commit
    );
    // The dead process's lease survives until reset at startup.
    assert_eq!(store.reset_leases().await.unwrap(), 1);
    clock.advance(5);
    let claim = store
        .claim("b2", Pool::Background, LEASE)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .publish(&claim, files(&commit, "files"))
            .await
            .unwrap()
            .row_complete
    );
}

async fn repos_are_added_listed_and_removed(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let nested = RepoId {
        provider: ProviderInstanceId::new("gitlab"),
        path: "group/sub/project".to_string(),
    };
    assert!(store.get_repo(&repo()).await.unwrap().is_none());
    assert!(store.list_repos().await.unwrap().is_empty());
    assert!(store.add_repo(&repo()).await.unwrap());
    clock.advance(10);
    assert!(!store.add_repo(&repo()).await.unwrap());
    assert!(store.add_repo(&nested).await.unwrap());

    let added = store.get_repo(&repo()).await.unwrap().unwrap();
    assert_eq!(added.repo_id, repo());
    assert_eq!(
        added.added_at, START as u64,
        "re-adding keeps the first time"
    );
    let listed: Vec<RepoId> = store
        .list_repos()
        .await
        .unwrap()
        .into_iter()
        .map(|added| added.repo_id)
        .collect();
    assert_eq!(listed, vec![repo(), nested.clone()]);

    assert!(store.remove_repo(&repo()).await.unwrap());
    assert!(!store.remove_repo(&repo()).await.unwrap());
    assert!(store.get_repo(&repo()).await.unwrap().is_none());
    assert_eq!(store.list_repos().await.unwrap().len(), 1);
    assert_eq!(
        store.get_repo(&nested).await.unwrap().unwrap().added_at,
        (START + 10) as u64
    );
}

async fn config_defaults_without_writing(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let missing = RepoId::github("acme/missing");
    assert_eq!(
        store.get_config(&missing).await.unwrap(),
        RepoConfig::default()
    );
    let config = RepoConfig {
        compression_level: Some(3),
        archive_chunk_size: Some(1024),
        ..Default::default()
    };
    store.set_config(&repo(), &config).await.unwrap();
    assert_eq!(store.get_config(&repo()).await.unwrap(), config);
    let invalid = RepoConfig {
        compression_level: Some(99),
        ..Default::default()
    };
    assert!(store.set_config(&repo(), &invalid).await.is_err());
    assert_eq!(store.get_config(&repo()).await.unwrap(), config);
    let updated = RepoConfig {
        compression_level: Some(19),
        ..Default::default()
    };
    store.set_config(&repo(), &updated).await.unwrap();
    assert_eq!(store.get_config(&repo()).await.unwrap(), updated);
    assert_eq!(
        store.get_config(&missing).await.unwrap(),
        RepoConfig::default()
    );
}
