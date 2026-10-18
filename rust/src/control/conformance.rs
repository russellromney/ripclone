//! Behavior every [`ControlStore`] must have. A backend passes by running
//! [`run_all`] with a factory for empty backends.

use super::{
    Clock, ControlStore, Lease, Out, Output, Pool, PublishedTags, RepoBranches, RepoRefs, SidePack,
};
use crate::provider::{ProviderInstanceId, RepoId};
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
        an_expired_lease_nobody_took_is_kept,
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
        purge_queues_only_artifacts_no_other_repo_references,
        purge_of_an_unknown_repo_does_nothing,
        a_queued_artifact_keeps_its_later_due_time,
        a_referenced_artifact_is_never_deletable,
        an_artifact_queued_again_during_a_sweep_stays_queued,
        refs_name_their_artifacts_until_purged,
        an_unapplied_publish_references_nothing,
        refs_are_absent_until_written_and_replaced,
    );
}

async fn open(backend: &dyn Backend) -> (Arc<ManualClock>, Arc<dyn ControlStore>) {
    let clock = Arc::new(ManualClock(AtomicI64::new(START)));
    let store = backend.open(clock.clone(), MAX_ATTEMPTS).await.unwrap();
    (clock, store)
}

/// Drop `store` and open its backend again, as a restarted server does. The
/// new store is another instance: it may take the old one's expired leases.
async fn reopen(
    backend: &dyn Backend,
    clock: &Arc<ManualClock>,
    store: Arc<dyn ControlStore>,
) -> Arc<dyn ControlStore> {
    drop(store);
    backend.open(clock.clone(), MAX_ATTEMPTS).await.unwrap()
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
    assert_eq!(
        claim.owner.strip_suffix('h'),
        files_claim.owner.strip_suffix('f'),
        "the owner is the store's instance and the name given"
    );
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

    // Releasing the lease frees the repo. So does its expiry, for another
    // store only.
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
    assert!(
        store
            .claim("f", Pool::Files, LEASE)
            .await
            .unwrap()
            .is_none(),
        "this store's expired Files lease still holds the repo"
    );
    let store = reopen(backend, &clock, store).await;
    let files_2 = store.claim("f", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(
        files_2.build.commit,
        sha(1),
        "another store retakes the expired Files lease"
    );
}

/// Another store, as after a restart, takes an expired lease. The old
/// claim is then fenced out: it cannot renew, publish, fail or release.
async fn expired_lease_is_reclaimed_from_its_old_owner(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let old = store
        .claim("old", Pool::Head, LEASE)
        .await
        .unwrap()
        .unwrap();
    let store = reopen(backend, &clock, store).await;
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

/// A process that stalls past its leases, as a sleeping laptop or a long
/// pause does, still runs those builds: its store never claims them again,
/// and they renew and publish. An expired lease nobody took renews from
/// another store too.
async fn an_expired_lease_nobody_took_is_kept(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let (b, c) = (sha(1), sha(2));
    store.admit(&repo(), &b, None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    clock.advance(600);
    assert!(store.claim("b", Pool::Head, LEASE).await.unwrap().is_none());
    assert!(store.renew(&claim, LEASE).await.unwrap());
    let row = store.status(&repo(), &b).await.unwrap().unwrap();
    assert_eq!(row.head_lease.until, Some(START + 660));
    assert_eq!(row.head_lease.attempts, 1);
    clock.advance(600);
    assert!(
        store
            .publish(&claim, head(&b, "late"))
            .await
            .unwrap()
            .applied
    );
    store.release(&claim).await.unwrap();

    store.admit(&repo(), &c, None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    assert_eq!(claim.build.commit, c);
    let store = reopen(backend, &clock, store).await;
    clock.advance(60);
    assert!(store.renew(&claim, LEASE).await.unwrap());
    assert!(
        store.claim("b", Pool::Head, LEASE).await.unwrap().is_none(),
        "the renewed lease holds against the new store"
    );
    store.fail(&claim, "bad object", None).await.unwrap();
    let row = store.status(&repo(), &c).await.unwrap().unwrap();
    assert_out!(row.head, failed("bad object"));
}

async fn renew_extends_the_lease(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    store.admit(&repo(), &sha(1), None).await.unwrap();
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    clock.advance(50);
    assert!(store.renew(&claim, LEASE).await.unwrap());
    let store = reopen(backend, &clock, store).await;
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
    let stale = store.claim("w", Pool::Files, LEASE).await.unwrap().unwrap();
    let store = reopen(backend, &clock, store).await;
    clock.advance(60);
    let files_claim = store.claim("w", Pool::Files, LEASE).await.unwrap().unwrap();
    assert_eq!(files_claim.attempts(), 2);
    let full_claim = store.claim("w", Pool::Full, LEASE).await.unwrap().unwrap();

    // The first Files claim is stale; the new Files and the Full hold
    // leases with the same owner.
    assert!(!store.renew(&stale, LEASE).await.unwrap());
    for output in [files(&commit, "stale"), full(&commit, "stale")] {
        assert!(!store.publish(&stale, output).await.unwrap().applied);
    }
    store.fail(&stale, "stale failure", None).await.unwrap();
    store.release(&stale).await.unwrap();
    let row = store.status(&repo(), &commit).await.unwrap().unwrap();
    assert_out!(row.files, pending(None));
    assert_out!(row.full, pending(None));
    assert_eq!(row.files_lease.until, Some(START + 120));
    assert_eq!(row.full_lease.until, Some(START + 120));
    assert!(store.renew(&full_claim, LEASE).await.unwrap());

    // Full fails for good and leaves Files alone.
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
    // First attempt fails retryably; the next two die holding the lease,
    // and a restarted server finds it expired.
    let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
    store
        .fail(&claim, "flaky", Some(Duration::from_secs(1)))
        .await
        .unwrap();
    store.release(&claim).await.unwrap();
    clock.advance(1);
    let mut store = store;
    let mut last = None;
    for attempt in 2..=MAX_ATTEMPTS {
        let claim = store.claim("a", Pool::Head, LEASE).await.unwrap().unwrap();
        assert_eq!(claim.attempts(), attempt);
        last = Some(claim);
        store = reopen(backend, &clock, store).await;
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
        store = reopen(backend, &clock, store).await;
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
        store = reopen(backend, &clock, store).await;
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
        store = reopen(backend, &clock, store).await;
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
    store.add_repo(&repo()).await.unwrap();
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

/// Every artifact the three outputs of `commit` built under `complete`.
fn completed_artifacts(commit: &str) -> Vec<String> {
    let mut hashes: Vec<String> = [
        head(commit, "head"),
        files(commit, "files"),
        full(commit, "full"),
    ]
    .iter()
    .flat_map(|output| {
        output
            .artifacts()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    })
    .collect();
    hashes.sort();
    hashes
}

async fn purge_queues_only_artifacts_no_other_repo_references(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let other = RepoId::github("acme/fork");
    store.add_repo(&repo()).await.unwrap();
    store.add_repo(&other).await.unwrap();
    // The same commit in both repos builds the same artifacts.
    complete(store.as_ref(), &repo(), &sha(1), Some(MAIN)).await;
    complete(store.as_ref(), &other, &sha(1), Some(MAIN)).await;
    complete(store.as_ref(), &repo(), &sha(2), Some(MAIN)).await;
    let mut known = store.known_repos().await.unwrap();
    known.sort_by_key(RepoId::storage_key);
    assert_eq!(known, vec![other.clone(), repo()]);
    assert!(store.knows_repo(&repo()).await.unwrap());

    let due = START + 100;
    let after = due + 1;
    let purged = store.purge_repo(&repo(), due).await.unwrap();
    let unique = completed_artifacts(&sha(2));
    assert_eq!(purged.builds, 2);
    assert_eq!(purged.queued, unique.len() as u64);
    assert!(store.get_repo(&repo()).await.unwrap().is_none());
    assert!(store.status(&repo(), &sha(1)).await.unwrap().is_none());
    assert!(store.status(&repo(), &sha(2)).await.unwrap().is_none());
    assert_eq!(store.known_repos().await.unwrap(), vec![other.clone()]);
    assert!(!store.knows_repo(&repo()).await.unwrap());
    assert!(store.knows_repo(&other).await.unwrap());
    let still = store.status(&other, &sha(1)).await.unwrap().unwrap();
    assert_eq!(ready_manifest(&still.head), Some(manifest(&sha(1), "head")));

    assert!(store.due_deletions(due, 100).await.unwrap().is_empty());
    let mut queued = store.due_deletions(after, 100).await.unwrap();
    queued.sort();
    assert_eq!(queued, unique);
    assert_eq!(store.due_deletions(after, 2).await.unwrap().len(), 2);
    for hash in &unique {
        assert!(!store.deletable(hash, due).await.unwrap(), "not yet due");
        assert!(store.deletable(hash, after).await.unwrap());
    }
    for hash in completed_artifacts(&sha(1)) {
        assert!(
            !store.deletable(&hash, after).await.unwrap(),
            "the fork uses it"
        );
    }

    // A dequeue before the due time leaves the artifact queued.
    store.dequeue_deletions(&unique[..1], due).await.unwrap();
    assert!(store.deletable(&unique[0], after).await.unwrap());
    store.dequeue_deletions(&unique, after).await.unwrap();
    assert!(store.due_deletions(after, 100).await.unwrap().is_empty());
}

async fn purge_of_an_unknown_repo_does_nothing(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    complete(store.as_ref(), &repo(), &sha(1), Some(MAIN)).await;
    let missing = RepoId::github("acme/missing");
    assert!(!store.knows_repo(&missing).await.unwrap());
    let purged = store.purge_repo(&missing, START).await.unwrap();
    assert_eq!(purged, crate::control::Purged::default());
    assert!(
        store
            .due_deletions(START + 1, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.status(&repo(), &sha(1)).await.unwrap().is_some());
}

async fn a_queued_artifact_keeps_its_later_due_time(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let hash = crate::cas::hash(b"orphan");
    let later = START + 500;
    store
        .queue_deletions(std::slice::from_ref(&hash), later)
        .await
        .unwrap();
    store
        .queue_deletions(std::slice::from_ref(&hash), START)
        .await
        .unwrap();
    assert!(
        store
            .due_deletions(START + 1, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!store.deletable(&hash, START + 1).await.unwrap());
    store
        .dequeue_deletions(std::slice::from_ref(&hash), START + 1)
        .await
        .unwrap();
    assert!(
        store.deletable(&hash, later + 1).await.unwrap(),
        "still queued"
    );

    // A purge that queues it later still moves it later.
    store.add_repo(&repo()).await.unwrap();
    complete(store.as_ref(), &repo(), &sha(1), Some(MAIN)).await;
    let shared = completed_artifacts(&sha(1));
    store.queue_deletions(&shared, START).await.unwrap();
    store.purge_repo(&repo(), later).await.unwrap();
    assert!(!store.deletable(&shared[0], START + 1).await.unwrap());
    assert!(store.deletable(&shared[0], later + 1).await.unwrap());
}

async fn a_referenced_artifact_is_never_deletable(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    complete(store.as_ref(), &repo(), &sha(1), Some(MAIN)).await;
    let referenced = completed_artifacts(&sha(1));
    let now = START + 1;
    store.queue_deletions(&referenced, START).await.unwrap();
    assert_eq!(
        store.due_deletions(now, 100).await.unwrap().len(),
        referenced.len()
    );
    for hash in &referenced {
        assert!(!store.deletable(hash, now).await.unwrap());
    }
    store.dequeue_deletions(&referenced, now).await.unwrap();
    assert!(store.due_deletions(now, 100).await.unwrap().is_empty());
    assert!(
        !store
            .deletable(&crate::cas::hash(b"never queued"), now)
            .await
            .unwrap()
    );
}

/// A build that stores an artifact again just after a sweep deleted it, and
/// then fails, queues it in the sweep's second: the sweep's dequeue keeps it.
async fn an_artifact_queued_again_during_a_sweep_stays_queued(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let hash = crate::cas::hash(b"stored again");
    let one = std::slice::from_ref(&hash);
    store.queue_deletions(one, START).await.unwrap();
    let now = START + 1;
    assert_eq!(store.due_deletions(now, 100).await.unwrap(), one);
    store.queue_deletions(one, now).await.unwrap();
    store.dequeue_deletions(one, now).await.unwrap();
    assert!(store.due_deletions(now, 100).await.unwrap().is_empty());
    assert!(
        store.deletable(&hash, now + 1).await.unwrap(),
        "still queued"
    );
}

async fn an_unapplied_publish_references_nothing(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let commit = sha(1);
    store.admit(&repo(), &commit, None).await.unwrap();
    let claim = store.claim("c", Pool::Head, LEASE).await.unwrap().unwrap();
    assert!(
        store
            .publish(&claim, head(&commit, "head"))
            .await
            .unwrap()
            .applied
    );
    let stale = head(&commit, "stale");
    assert!(!store.publish(&claim, stale.clone()).await.unwrap().applied);
    assert!(
        !store
            .publish(&claim, files(&commit, "wrong kind"))
            .await
            .unwrap()
            .applied
    );
    let unreferenced: Vec<String> = stale
        .artifacts()
        .into_iter()
        .chain(files(&commit, "wrong kind").artifacts())
        .map(str::to_string)
        .collect();
    store.queue_deletions(&unreferenced, START).await.unwrap();
    for hash in &unreferenced {
        assert!(store.deletable(hash, START + 1).await.unwrap(), "{hash}");
    }
}

async fn refs_are_absent_until_written_and_replaced(backend: &dyn Backend) {
    let (clock, store) = open(backend).await;
    let missing = RepoId::github("acme/missing");
    assert_eq!(store.get_refs(&repo()).await.unwrap(), None);
    let first = RepoRefs {
        tags_artifact: crate::cas::hash(b"first"),
        tags_len: 120,
        tags_count: 3,
        tags_hash: crate::cas::hash(b"tags 1"),
        branches: None,
        ready: None,
    };
    assert!(
        !store.set_refs(&repo(), &first).await.unwrap(),
        "the repo is not added"
    );
    assert_eq!(store.get_refs(&repo()).await.unwrap(), None);
    store.add_repo(&repo()).await.unwrap();
    assert!(store.set_refs(&repo(), &first).await.unwrap());
    assert_eq!(store.get_refs(&repo()).await.unwrap(), Some(first.clone()));
    let with_side = RepoRefs {
        branches: Some(RepoBranches {
            artifact: crate::cas::hash(b"branches"),
            len: 40,
            refs_hash: crate::cas::hash(b"refs 1"),
            default_branch: Some("main".into()),
            base: Some("1".repeat(40)),
            side_base: Some("2".repeat(40)),
            side_tips: crate::cas::hash(b"tips 1"),
            side_packs: vec![SidePack {
                pack: crate::cas::hash(b"pack"),
                pack_len: 1 << 40,
                idx: crate::cas::hash(b"idx"),
                idx_len: 1100,
            }],
            tags: PublishedTags {
                artifact: crate::cas::hash(b"first"),
                len: 120,
                count: 3,
                hash: crate::cas::hash(b"tags 1"),
            },
        }),
        ..first.clone()
    };
    assert!(store.set_refs(&repo(), &with_side).await.unwrap());
    assert_eq!(
        store.get_refs(&repo()).await.unwrap(),
        Some(with_side.clone())
    );
    let second = RepoRefs {
        tags_artifact: crate::cas::hash(b"second"),
        tags_len: 0,
        tags_count: 0,
        tags_hash: crate::cas::hash(b"tags 2"),
        branches: Some(RepoBranches {
            artifact: crate::cas::hash(b"branches 2"),
            len: 30,
            refs_hash: crate::cas::hash(b"refs 2"),
            default_branch: None,
            base: None,
            side_base: None,
            side_tips: crate::cas::hash(b"tips 2"),
            side_packs: Vec::new(),
            tags: PublishedTags::default(),
        }),
        ready: with_side.branches.clone(),
    };
    assert!(store.set_refs(&repo(), &second).await.unwrap());
    let store = reopen(backend, &clock, store).await;
    assert_eq!(store.get_refs(&repo()).await.unwrap(), Some(second));
    assert_eq!(store.get_refs(&missing).await.unwrap(), None);
}

async fn refs_name_their_artifacts_until_purged(backend: &dyn Backend) {
    let (_, store) = open(backend).await;
    let other = RepoId::github("acme/fork");
    store.add_repo(&repo()).await.unwrap();
    store.add_repo(&other).await.unwrap();
    let mine = RepoRefs {
        tags_artifact: crate::cas::hash(b"shared tags"),
        tags_len: 10,
        tags_count: 1,
        tags_hash: crate::cas::hash(b"tags"),
        branches: Some(RepoBranches {
            artifact: crate::cas::hash(b"my branches"),
            len: 40,
            refs_hash: crate::cas::hash(b"refs"),
            default_branch: Some("main".into()),
            base: Some("1".repeat(40)),
            side_base: Some("1".repeat(40)),
            side_tips: crate::cas::hash(b"tips"),
            side_packs: vec![SidePack {
                pack: crate::cas::hash(b"my pack"),
                pack_len: 100,
                idx: crate::cas::hash(b"my idx"),
                idx_len: 10,
            }],
            tags: PublishedTags {
                artifact: crate::cas::hash(b"shared tags"),
                len: 10,
                count: 1,
                hash: crate::cas::hash(b"tags"),
            },
        }),
        // The older branches keep the tags of their time.
        ready: Some(RepoBranches {
            artifact: crate::cas::hash(b"my older branches"),
            len: 30,
            refs_hash: crate::cas::hash(b"older refs"),
            default_branch: Some("main".into()),
            base: Some("2".repeat(40)),
            side_base: Some("2".repeat(40)),
            side_tips: crate::cas::hash(b"older tips"),
            side_packs: Vec::new(),
            tags: PublishedTags {
                artifact: crate::cas::hash(b"my older tags"),
                len: 5,
                count: 0,
                hash: crate::cas::hash(b"older tags"),
            },
        }),
    };
    let theirs = RepoRefs {
        branches: None,
        ready: None,
        ..mine.clone()
    };
    assert!(store.set_refs(&repo(), &mine).await.unwrap());
    assert!(store.set_refs(&other, &theirs).await.unwrap());
    let named: Vec<String> = mine.artifacts().into_iter().map(str::to_string).collect();
    assert_eq!(named.len(), 6);
    store.queue_deletions(&named, START).await.unwrap();
    for hash in &named {
        assert!(!store.deletable(hash, START + 1).await.unwrap(), "{hash}");
    }

    let purged = store.purge_repo(&repo(), START).await.unwrap();
    assert_eq!(purged.queued, 5, "the shared tags stay");
    assert_eq!(store.get_refs(&repo()).await.unwrap(), None);
    assert_eq!(store.get_refs(&other).await.unwrap(), Some(theirs));
    for hash in &named {
        let shared = *hash == mine.tags_artifact;
        assert_eq!(
            store.deletable(hash, START + 1).await.unwrap(),
            !shared,
            "{hash}"
        );
    }
    assert!(
        !store.set_refs(&repo(), &mine).await.unwrap(),
        "a refresh that ends after the purge writes nothing"
    );
    assert_eq!(store.get_refs(&repo()).await.unwrap(), None);
    let branches = &mine.branches.as_ref().unwrap().artifact;
    assert!(
        store.deletable(branches, START + 1).await.unwrap(),
        "the refused write records no references"
    );
}
