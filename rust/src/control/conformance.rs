//! Behavior every [`ControlStore`] must have. A backend passes by running
//! [`run_all`] with a factory for empty backends.

use super::{Clock, ControlStore, Lease, Out, Output, Pool};
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
        pools_offer_head_and_files_then_full,
        claims_take_the_oldest_row,
        a_repo_holds_one_lease_per_pool,
        files_does_not_wait_for_another_rows_full,
        full_claims_follow_admission_order,
        expired_lease_is_reclaimed_from_its_old_owner,
        expired_lease_cannot_publish_fail_or_renew,
        renew_extends_the_lease,
        retry_after_gates_claims,
        failed_head_fails_full_not_files,
        retryable_failure_on_last_attempt_fails,
        failed_files_does_not_stop_full,
        retryable_files_failure_does_not_hold_full,
        a_claim_touches_only_its_own_kind,
        retry_touches_only_failed_outputs,
        exhausted_attempts_are_given_up,
        stale_claim_cannot_overwrite_any_ready_result,
        reset_leases_frees_every_row,
        latest_ready_full_is_newest_ready_on_the_ref,
        list_builds_returns_one_repo_oldest_first,
        concurrent_claims_never_share_a_row,
        state_survives_restart,
        repos_are_added_listed_and_removed,
        config_is_absent_until_written,
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
    for (pool, output) in [
        (Pool::Head, head(commit, "head")),
        (Pool::Files, files(commit, "files")),
        (Pool::Full, full(commit, "full")),
    ] {
        let claim = store.claim("builder", pool, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.commit, commit);
        assert!(store.publish(&claim, output).await.unwrap().applied);
        store.release(&claim).await.unwrap();
    }
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
    for kind in [
        ExactResultKind::Head,
        ExactResultKind::Files,
        ExactResultKind::Full,
    ] {
        assert_eq!(row.lease(kind), Lease::default());
    }
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

/// Head and Files are offered at once. Full is offered once Head is ready,
/// while Files still builds. Each has its own lease and attempts.
async fn pools_offer_head_and_files_then_full(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, Some(MAIN)).await.unwrap();
    assert!(store.claim("b", Pool::Full, LEASE).await.unwrap().is_none());
    let files_claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(files_claim.pool, Pool::Files);
    assert_eq!(files_claim.attempts(), 1);
    assert_out!(files_claim.build.head, pending(None));
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.repo, repo());
    assert_eq!(claim.owner, "h");
    assert_eq!(claim.pool, Pool::Head);
    assert_eq!(claim.kind(), ExactResultKind::Head);
    assert_eq!(claim.build.commit, commit);
    assert_eq!(claim.build.ref_name.as_deref(), Some(MAIN));
    assert_eq!(claim.attempts(), 1);
    assert_eq!(
        claim.build.head_lease,
        Lease {
            attempts: 1,
            until: Some(START + 60)
        }
    );
    assert_eq!(claim.build.files_lease.until, Some(START + 60));
    for pool in [Pool::Head, Pool::Files] {
        assert!(store.claim("x", pool, LEASE).await.unwrap().is_none());
    }
    assert!(
        store.claim("b", Pool::Full, LEASE).await.unwrap().is_none(),
        "Full waits for Head"
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
    assert_eq!(row.head_lease.until, Some(START + 60));

    // Full is claimable as soon as Head is ready, even before the Head
    // lease is released.
    let full_claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
    assert!(full_claim.build.head.ready().is_some());
    store.release(&claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(row.head_lease.until, None);
    assert_eq!(row.files_lease.until, Some(START + 60));
    assert!(store.claim("h", Pool::Head, LEASE).await.unwrap().is_none());

    assert_eq!(full_claim.build.commit, commit);
    assert_eq!(full_claim.pool, Pool::Full);
    assert_eq!(full_claim.attempts(), 1);
    assert_out!(full_claim.build.files, pending(None));
    assert_eq!(full_claim.build.files_lease.until, Some(START + 60));
    for pool in [Pool::Files, Pool::Full] {
        assert!(store.claim("x", pool, LEASE).await.unwrap().is_none());
    }

    let published = store
        .publish(&full_claim, full(&commit, "full"))
        .await
        .unwrap();
    assert!(published.applied && !published.row_complete);
    store.release(&full_claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.full), Some(manifest(&commit, "full")));
    assert_out!(row.files, pending(None));
    assert_eq!(row.full_lease.until, None);
    assert_eq!(
        row.files_lease.until,
        Some(START + 60),
        "releasing Full leaves the Files lease"
    );

    let published = store
        .publish(&files_claim, files(&commit, "files"))
        .await
        .unwrap();
    assert!(published.applied && published.row_complete);
    store.release(&files_claim).await.unwrap();
    for pool in [Pool::Head, Pool::Files, Pool::Full] {
        assert!(store.claim("x", pool, LEASE).await.unwrap().is_none());
    }
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.files), Some(manifest(&commit, "files")));
    assert_eq!(ready_manifest(&row.full), Some(manifest(&commit, "full")));
}

/// Publish Head of `commit` through a claim.
async fn publish_head(store: &dyn ControlStore, commit: &str) {
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, commit);
    assert!(
        store
            .publish(&claim, head(commit, "head"))
            .await
            .unwrap()
            .applied
    );
    store.release(&claim).await.unwrap();
}

/// Publish Files of `commit` through a claim.
async fn publish_files(store: &dyn ControlStore, commit: &str) {
    let claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, commit);
    assert!(
        store
            .publish(&claim, files(commit, "files"))
            .await
            .unwrap()
            .applied
    );
    store.release(&claim).await.unwrap();
}

/// A new push's Files is claimable while an older row's Full is building.
async fn files_does_not_wait_for_another_rows_full(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let (b, c) = (sha(1), sha(2));
    store.admit(&repo(), &b, Some(MAIN)).await.unwrap();
    publish_head(store.as_ref(), &b).await;
    publish_files(store.as_ref(), &b).await;
    let full_b = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
    assert_eq!(full_b.build.commit, b);

    store.admit(&repo(), &c, Some(MAIN)).await.unwrap();
    publish_head(store.as_ref(), &c).await;
    let files_c = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(files_c.build.commit, c);
    assert!(
        store
            .publish(&files_c, files(&c, "files"))
            .await
            .unwrap()
            .applied
    );
    store.release(&files_c).await.unwrap();
    let row = store.status(&repo(), &b).await.unwrap().unwrap();
    assert_out!(row.full, pending(None));
    assert_eq!(
        row.full_lease.until,
        Some(START + 60),
        "Full(B) still holds B"
    );
}

/// Full claims go oldest admission first, so with one Full slot Full(C)
/// builds after Full(B) and on its history levels. B and C are admitted in
/// the same second and C sorts first by commit id: only admission order
/// decides. Full(C) waits while Full(B) is pending even with no Full lease
/// held, and neither waits for Files.
async fn full_claims_follow_admission_order(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let (b, c) = (sha(2), sha(1));
    store.admit(&repo(), &b, Some(MAIN)).await.unwrap();
    store.admit(&repo(), &c, Some(MAIN)).await.unwrap();
    for commit in [&b, &c] {
        publish_head(store.as_ref(), commit).await;
    }
    let claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, b);
    store
        .fail(&claim, "flaky", Some(Duration::from_secs(30)))
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    assert!(
        store.claim("u", Pool::Full, LEASE).await.unwrap().is_none(),
        "Full(C) waits for the pending Full(B)"
    );
    clock.advance(30);
    for commit in [&b, &c] {
        let claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
        assert_eq!(&claim.build.commit, commit);
        assert!(
            store
                .publish(&claim, full(commit, "full"))
                .await
                .unwrap()
                .applied
        );
        store.release(&claim).await.unwrap();
    }
    for commit in [&b, &c] {
        let row = store.status(&repo(), commit).await.unwrap().unwrap();
        assert_out!(row.files, pending(None));
    }
}

/// Claims follow admission order, not the clock or commit ids: all rows
/// here are admitted in the same second, in an order their ids do not sort
/// in.
async fn claims_take_the_oldest_row(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let other_repo = RepoId::github("acme/other");
    store.admit(&repo(), &sha(3), None).await.unwrap();
    store.admit(&other_repo, &sha(2), None).await.unwrap();
    store.admit(&repo(), &sha(1), None).await.unwrap();
    for expected in [sha(3), sha(2), sha(1)] {
        publish_head(store.as_ref(), &expected).await;
    }
    assert!(store.claim("h", Pool::Head, LEASE).await.unwrap().is_none());
}

/// A repo with a live lease in a pool is skipped by that pool's claims, so
/// one busy repo cannot hold every slot. Other pools still claim its rows.
async fn a_repo_holds_one_lease_per_pool(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let other_repo = RepoId::github("acme/other");
    for n in 1..=3 {
        store.admit(&repo(), &sha(n), None).await.unwrap();
    }
    store.admit(&other_repo, &sha(9), None).await.unwrap();

    let head_1 = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(head_1.build.commit, sha(1));
    let other = store.claim("b", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(other.repo, other_repo, "the busy repo's sha(2) is skipped");
    assert!(store.claim("c", Pool::Head, LEASE).await.unwrap().is_none());

    // Files of one row and Head of the next run together in one repo.
    assert!(
        store
            .publish(&head_1, head(&sha(1), "head"))
            .await
            .unwrap()
            .applied
    );
    store.release(&head_1).await.unwrap();
    let files_1 = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(files_1.build.commit, sha(1));
    let other_files = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(
        other_files.repo, other_repo,
        "Files skips the busy repo too"
    );
    let head_2 = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(head_2.build.commit, sha(2));
    assert!(store.claim("a", Pool::Head, LEASE).await.unwrap().is_none());

    // Releasing or expiring the lease frees the repo.
    assert!(
        store
            .publish(&head_2, head(&sha(2), "head"))
            .await
            .unwrap()
            .applied
    );
    store.release(&head_2).await.unwrap();
    assert!(
        store
            .claim("f", Pool::Files, LEASE)
            .await
            .unwrap()
            .is_none(),
        "Files(sha 1) still holds the repo's Files lease"
    );
    let head_3 = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(head_3.build.commit, sha(3));
    clock.advance(60);
    let files_2 = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(
        files_2.build.commit,
        sha(1),
        "the expired Files lease is retaken"
    );
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
    assert_eq!(new.attempts(), 2);

    assert!(!store.renew(&old, LEASE).await.unwrap());
    assert!(
        !store
            .publish(&old, head(&commit, "old"))
            .await
            .unwrap()
            .applied
    );
    store.fail(&old, "old failed", None).await.unwrap();
    store.release(&old).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, pending(None));
    assert_eq!(row.head_lease.until, Some(START + 120));
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

/// A lease past its expiry is lost even before anyone claims the row: the
/// late owner cannot publish, record a failure, or revive it by renewing.
async fn expired_lease_cannot_publish_fail_or_renew(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    clock.advance(60);
    assert!(!store.renew(&claim, LEASE).await.unwrap());
    assert!(
        !store
            .publish(&claim, head(&commit, "late"))
            .await
            .unwrap()
            .applied
    );
    store.fail(&claim, "late failure", None).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, pending(None));
    let claim = store.claim("b", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.attempts(), 2);
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
        .fail(&claim, "flaky", Some(Duration::from_secs(30)))
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
    assert_eq!(claim.attempts(), 2);
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

/// Full is built from Head, so a failed Head fails it. Files does not need
/// Head and still builds.
async fn failed_head_fails_full_not_files(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store.fail(&claim, "bad object", None).await.unwrap();
    store.release(&claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, failed("bad object"));
    assert_out!(row.files, pending(None));
    assert_out!(row.full, failed("head failed: bad object"));
    for pool in [Pool::Head, Pool::Full] {
        assert!(store.claim("a", pool, LEASE).await.unwrap().is_none());
    }
    publish_files(store.as_ref(), &commit).await;
}

async fn retryable_failure_on_last_attempt_fails(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    for attempt in 1..=MAX_ATTEMPTS {
        let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.attempts(), attempt);
        store
            .fail(
                &claim,
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

/// A failed Files fails alone: the row's Full, building at the same time,
/// keeps its lease and publishes.
async fn failed_files_does_not_stop_full(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    publish_head(store.as_ref(), &commit).await;
    let files_claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    let full_claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
    store
        .fail(&files_claim, "out of memory", None)
        .await
        .unwrap();
    store.release(&files_claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_out!(row.files, failed("out of memory"));
    assert_out!(row.full, pending(None));
    assert_eq!(
        row.full_lease,
        Lease {
            attempts: 1,
            until: Some(START + 60)
        }
    );
    assert!(
        store
            .claim("f", Pool::Files, LEASE)
            .await
            .unwrap()
            .is_none()
    );

    let published = store
        .publish(&full_claim, full(&commit, "full"))
        .await
        .unwrap();
    assert!(published.applied && !published.row_complete);
    store.release(&full_claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.files, failed("out of memory"));
    assert_eq!(ready_manifest(&row.full), Some(manifest(&commit, "full")));
    for pool in [Pool::Files, Pool::Full] {
        assert!(store.claim("x", pool, LEASE).await.unwrap().is_none());
    }
}

/// A Files failure with attempts left keeps Files pending behind its
/// backoff. Full does not wait for it.
async fn retryable_files_failure_does_not_hold_full(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    publish_head(store.as_ref(), &commit).await;
    let claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    store
        .fail(&claim, "flaky", Some(Duration::from_secs(30)))
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.files, pending(Some("flaky")));
    assert_eq!(row.files_lease.attempts, 1);
    assert_eq!(row.full_lease.attempts, 0);
    let full_claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
    assert_eq!(full_claim.attempts(), 1);
    assert!(
        store
            .publish(&full_claim, full(&commit, "full"))
            .await
            .unwrap()
            .applied
    );
    store.release(&full_claim).await.unwrap();
    clock.advance(29);
    assert!(
        store
            .claim("f", Pool::Files, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    clock.advance(1);
    let claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.attempts(), 2);
    assert!(
        store
            .publish(&claim, files(&commit, "files"))
            .await
            .unwrap()
            .row_complete
    );
}

/// A claim acts on its own kind's lease only, even when the owner names
/// match: a stale Files claim cannot publish, fail, renew or release
/// anything of the row's Full, and a Full claim cannot touch Files.
async fn a_claim_touches_only_its_own_kind(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    publish_head(store.as_ref(), &commit).await;
    let files_claim = store.claim("w", Pool::Files, LEASE).await.unwrap().unwrap();
    clock.advance(30);
    let full_claim = store.claim("w", Pool::Full, LEASE).await.unwrap().unwrap();
    clock.advance(30);

    // Files' lease has expired; Full's has 30 s left.
    assert!(!store.renew(&files_claim, LEASE).await.unwrap());
    for output in [files(&commit, "stale"), full(&commit, "stale")] {
        assert!(!store.publish(&files_claim, output).await.unwrap().applied);
    }
    store
        .fail(&files_claim, "stale failure", None)
        .await
        .unwrap();
    store.release(&files_claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.files, pending(None));
    assert_out!(row.full, pending(None));
    assert_eq!(row.files_lease.until, None);
    assert_eq!(row.full_lease.until, Some(START + 90));
    assert!(store.renew(&full_claim, LEASE).await.unwrap());

    // A new Files claim. Full fails for good and leaves Files alone.
    let files_claim = store.claim("w", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(files_claim.attempts(), 2);
    assert!(
        !store
            .publish(&full_claim, files(&commit, "wrong kind"))
            .await
            .unwrap()
            .applied
    );
    store
        .fail(&full_claim, "out of memory", None)
        .await
        .unwrap();
    store.release(&full_claim).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.full, failed("out of memory"));
    assert_out!(row.files, pending(None));
    assert_eq!(row.full_lease.until, None);
    assert_eq!(row.files_lease.until, Some(START + 120));
    assert!(store.renew(&files_claim, LEASE).await.unwrap());
    let published = store
        .publish(&files_claim, files(&commit, "files"))
        .await
        .unwrap();
    assert!(published.applied && !published.row_complete);
}

async fn retry_touches_only_failed_outputs(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    assert!(store.retry(&repo(), &sha(9)).await.unwrap().is_none());

    // Head and Files ready, Full failed.
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    publish_head(store.as_ref(), &commit).await;
    publish_files(store.as_ref(), &commit).await;
    let claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
    store.fail(&claim, "out of memory", None).await.unwrap();
    store.release(&claim).await.unwrap();

    let row = store.retry(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_eq!(ready_manifest(&row.files), Some(manifest(&commit, "files")));
    assert_out!(row.full, pending(None));
    assert_eq!(row.full_lease.attempts, 0, "the failed kind starts over");
    assert_eq!(row.files_lease.attempts, 1, "a ready kind is untouched");
    assert!(
        store
            .claim("f", Pool::Files, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    let claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
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
        .fail(&claim, "flaky", Some(Duration::from_secs(30)))
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    let row = store.retry(&repo(), &other).await.unwrap().unwrap();
    assert_eq!(row.head_lease.attempts, 1);
    assert_out!(row.head, pending(Some("flaky")));
    assert!(store.claim("h", Pool::Head, LEASE).await.unwrap().is_none());

    // A failed Head comes back with everything it took down.
    let third = sha(3);
    store.admit(&repo(), &third, None).await.unwrap();
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, third);
    store.fail(&claim, "bad", None).await.unwrap();
    store.release(&claim).await.unwrap();
    let row = store.retry(&repo(), &third).await.unwrap().unwrap();
    assert_out!(row.head, pending(None));
    assert_out!(row.files, pending(None));
    assert_out!(row.full, pending(None));
    assert_eq!(row.head_lease.attempts, 0);
    let claim = store.claim("h", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, third);
    assert_eq!(claim.attempts(), 1);
}

async fn exhausted_attempts_are_given_up(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    // First attempt fails retryably; the next two die holding the lease.
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store
        .fail(&claim, "flaky", Some(Duration::from_secs(1)))
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    clock.advance(1);
    let mut last = None;
    for attempt in 2..=MAX_ATTEMPTS {
        let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.attempts(), attempt);
        last = Some(claim);
        clock.advance(60);
    }
    assert!(store.claim("a", Pool::Head, LEASE).await.unwrap().is_none());
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.head, failed("gave up after 3 attempts: flaky"));
    assert_out!(row.files, pending(None));
    assert_out!(
        row.full,
        failed("head failed: gave up after 3 attempts: flaky")
    );
    assert_eq!(row.head_lease.until, None);
    let last = last.unwrap();
    assert!(
        !store
            .publish(&last, head(&commit, "late"))
            .await
            .unwrap()
            .applied
    );
    // Files does not need Head, so it still builds.
    publish_files(store.as_ref(), &commit).await;

    // Files and Full count their own attempts. Files runs out while Full is
    // never claimed: only Files fails, and Full keeps all its attempts.
    let other = sha(2);
    store.admit(&repo(), &other, None).await.unwrap();
    publish_head(store.as_ref(), &other).await;
    for attempt in 1..=MAX_ATTEMPTS {
        let claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.commit, other);
        assert_eq!(claim.attempts(), attempt);
        clock.advance(60);
    }
    assert!(
        store
            .claim("f", Pool::Files, LEASE)
            .await
            .unwrap()
            .is_none()
    );
    let row = store.status(&repo(), &other).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&other, "head")));
    assert_out!(row.files, failed("gave up after 3 attempts"));
    assert_out!(row.full, pending(None));
    assert_eq!(row.files_lease.until, None);
    assert_eq!(row.full_lease, Lease::default());

    // Full gets its own attempts; giving it up leaves Files as it was.
    for attempt in 1..=MAX_ATTEMPTS {
        let claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.attempts(), attempt);
        clock.advance(60);
    }
    assert!(store.claim("u", Pool::Full, LEASE).await.unwrap().is_none());
    let row = store.status(&repo(), &other).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&other, "head")));
    assert_out!(row.files, failed("gave up after 3 attempts"));
    assert_out!(row.full, failed("gave up after 3 attempts"));
    assert_eq!(row.full_lease.until, None);

    // The other way round: Full runs out first, and Files still builds.
    let third = sha(3);
    store.admit(&repo(), &third, None).await.unwrap();
    publish_head(store.as_ref(), &third).await;
    for _ in 1..=MAX_ATTEMPTS {
        store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
        clock.advance(60);
    }
    assert!(store.claim("u", Pool::Full, LEASE).await.unwrap().is_none());
    let row = store.status(&repo(), &third).await.unwrap().unwrap();
    assert_out!(row.full, failed("gave up after 3 attempts"));
    assert_out!(row.files, pending(None));
    assert_eq!(row.files_lease, Lease::default());
    let claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, third);
    assert_eq!(claim.attempts(), 1);
    let published = store.publish(&claim, files(&third, "files")).await.unwrap();
    assert!(published.applied && !published.row_complete);
}

async fn stale_claim_cannot_overwrite_any_ready_result(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let mut claims = Vec::new();
    for (pool, output) in [
        (Pool::Head, head(&commit, "head")),
        (Pool::Files, files(&commit, "files")),
        (Pool::Full, full(&commit, "full")),
    ] {
        let claim = store.claim("c", pool, LEASE).await.unwrap().unwrap();
        assert!(store.publish(&claim, output).await.unwrap().applied);
        store.release(&claim).await.unwrap();
        claims.push(claim);
    }

    for claim in &claims {
        for output in [
            head(&commit, "stale"),
            files(&commit, "stale"),
            full(&commit, "stale"),
        ] {
            assert!(!store.publish(claim, output).await.unwrap().applied);
        }
    }
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_eq!(ready_manifest(&row.files), Some(manifest(&commit, "files")));
    assert_eq!(ready_manifest(&row.full), Some(manifest(&commit, "full")));
}

async fn reset_leases_frees_every_row(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    store.admit(&repo(), &sha(1), None).await.unwrap();
    store
        .admit(&RepoId::github("acme/other"), &sha(2), None)
        .await
        .unwrap();
    store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    assert!(store.claim("b", Pool::Head, LEASE).await.unwrap().is_none());
    assert_eq!(store.reset_leases().await.unwrap(), 2);
    assert_eq!(store.reset_leases().await.unwrap(), 0);
    for _ in 0..2 {
        let claim = store.claim("b", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.attempts(), 2);
        assert!(
            store
                .publish(&claim, head(&claim.build.commit, "head"))
                .await
                .unwrap()
                .applied
        );
    }

    // One row holding Files and Full leases counts once; both are freed.
    for pool in [Pool::Files, Pool::Full] {
        let claim = store.claim("c", pool, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.commit, sha(1));
    }
    assert_eq!(store.reset_leases().await.unwrap(), 2);
    let row = store.status(&repo(), &sha(1)).await.unwrap().unwrap();
    for kind in [
        ExactResultKind::Head,
        ExactResultKind::Files,
        ExactResultKind::Full,
    ] {
        assert_eq!(row.lease(kind).until, None, "{kind}");
    }
    for pool in [Pool::Files, Pool::Full] {
        let claim = store.claim("d", pool, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.build.commit, sha(1));
        assert_eq!(claim.attempts(), 2);
    }
}

/// Newest by admission order; every row here is admitted in one second.
async fn latest_ready_full_is_newest_ready_on_the_ref(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let other_repo = RepoId::github("acme/other");
    assert!(
        store
            .latest_ready_full(&repo(), MAIN)
            .await
            .unwrap()
            .is_none()
    );
    complete(store.as_ref(), &repo(), &sha(2), Some(MAIN)).await;
    complete(store.as_ref(), &repo(), &sha(1), Some(MAIN)).await;
    complete(store.as_ref(), &repo(), &sha(3), Some("refs/heads/dev")).await;
    complete(store.as_ref(), &other_repo, &sha(4), Some(MAIN)).await;
    complete(store.as_ref(), &repo(), &sha(5), None).await;
    store.admit(&repo(), &sha(6), Some(MAIN)).await.unwrap();

    let (commit, result) = store
        .latest_ready_full(&repo(), MAIN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(commit, sha(1));
    assert_eq!(result.clonepack.manifest, manifest(&sha(1), "full"));
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

async fn list_builds_returns_one_repo_oldest_first(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let other_repo = RepoId::github("acme/other");
    assert!(store.list_builds(&repo()).await.unwrap().is_empty());
    complete(store.as_ref(), &repo(), &sha(2), Some(MAIN)).await;
    store.admit(&repo(), &sha(1), None).await.unwrap();
    store.admit(&other_repo, &sha(3), None).await.unwrap();

    let rows = store.list_builds(&repo()).await.unwrap();
    let commits: Vec<&str> = rows.iter().map(|row| row.commit.as_str()).collect();
    assert_eq!(commits, [sha(2), sha(1)]);
    assert_eq!(
        ready_manifest(&rows[0].full),
        Some(manifest(&sha(2), "full"))
    );
    assert!(is(&rows[1].head, &pending(None)));
    assert_eq!(store.list_builds(&other_repo).await.unwrap().len(), 1);
}

/// One row in each of 16 repos, so the one-lease-per-repo rule does not
/// limit how many claims succeed.
async fn concurrent_claims_never_share_a_row(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let rows = 16;
    for n in 0..rows {
        store
            .admit(&RepoId::github(format!("acme/repo-{n}")), &sha(n), None)
            .await
            .unwrap();
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
            assert_eq!(claim.attempts(), 1);
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
    publish_head(store.as_ref(), &commit).await;
    let claim = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    store
        .fail(&claim, "flaky", Some(Duration::from_secs(5)))
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
    assert_eq!(
        row.files_lease,
        Lease {
            attempts: 1,
            until: Some(START + 60)
        }
    );
    assert_eq!(ready_manifest(&row.head), Some(manifest(&commit, "head")));
    assert_out!(row.files, pending(Some("flaky")));
    assert_out!(row.full, pending(None));
    assert_eq!(
        store.get_repo(&repo()).await.unwrap().unwrap().added_at,
        START as u64
    );
    assert_eq!(
        store.get_config(&repo()).await.unwrap(),
        Some(config.clone())
    );
    // The dead process's lease survives until reset at startup.
    assert_eq!(store.reset_leases().await.unwrap(), 1);
    clock.advance(5);
    publish_files(store.as_ref(), &commit).await;
    let claim = store.claim("u", Pool::Full, LEASE).await.unwrap().unwrap();
    assert!(
        store
            .publish(&claim, full(&commit, "full"))
            .await
            .unwrap()
            .row_complete
    );
    drop(store);

    let store = backend.open(clock.clone(), MAX_ATTEMPTS).await.unwrap();
    assert_eq!(
        store
            .latest_ready_full(&repo(), MAIN)
            .await
            .unwrap()
            .unwrap()
            .0,
        commit
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

async fn config_is_absent_until_written(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let missing = RepoId::github("acme/missing");
    assert_eq!(store.get_config(&missing).await.unwrap(), None);
    let config = RepoConfig {
        compression_level: Some(3),
        ..Default::default()
    };
    store.set_config(&repo(), &config).await.unwrap();
    assert_eq!(
        store.get_config(&repo()).await.unwrap(),
        Some(config.clone())
    );
    let invalid = RepoConfig {
        compression_level: Some(99),
        ..Default::default()
    };
    assert!(store.set_config(&repo(), &invalid).await.is_err());
    assert_eq!(
        store.get_config(&repo()).await.unwrap(),
        Some(config.clone())
    );
    let updated = RepoConfig {
        compression_level: Some(19),
        ..Default::default()
    };
    store.set_config(&repo(), &updated).await.unwrap();
    assert_eq!(store.get_config(&repo()).await.unwrap(), Some(updated));
    assert_eq!(store.get_config(&missing).await.unwrap(), None);
}
