//! One set of artifact-download rules, proved end to end through the CLI's
//! install path.
//!
//! Two families live here:
//!
//! * **Overlap.** Files extraction must write output before the last archive
//!   chunk arrives, and an editable clone must install and materialize an
//!   earlier pack while a later pack is still downloading. Both are proved with
//!   the server-side [`ArtifactBarrier`] armed on one named artifact hash, so
//!   the window is deterministic rather than timing-dependent.
//! * **Failure classification.** A transient 503 retries; a missing object, a
//!   wrong length, and a wrong hash fail immediately, leave no target, and never
//!   ask the clone driver to refresh signed URLs.

use crate::common;

use common::*;
use ripclone::mode::CloneMode;
use ripclone::server::{ArtifactBarrier, BarrierTarget};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// How long a staging observation may wait before the overlap claim is
/// considered disproved. Generous: a false pass is impossible (the artifact the
/// barrier holds is never released until we release it), so the only thing this
/// bounds is how long a genuine failure takes to report.
const OVERLAP_DEADLINE: Duration = Duration::from_secs(60);

/// An [`ArtifactBarrier`] whose target hash is chosen after the repo is built.
struct HashBarrier {
    slot: Arc<StdMutex<Option<String>>>,
    entered: oneshot::Receiver<()>,
    proceed: oneshot::Sender<()>,
}

fn hash_barrier(after_bytes: usize) -> (ArtifactBarrier, HashBarrier) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (proceed_tx, proceed_rx) = oneshot::channel();
    let slot = Arc::new(StdMutex::new(None));
    let barrier = ArtifactBarrier {
        after_bytes,
        target: BarrierTarget::Hash(Arc::clone(&slot)),
        entered: Arc::new(StdMutex::new(Some(entered_tx))),
        proceed: Arc::new(StdMutex::new(Some(proceed_rx))),
        close_on_proceed: false,
        consumed: Arc::new(AtomicBool::new(false)),
    };
    (
        barrier,
        HashBarrier {
            slot,
            entered: entered_rx,
            proceed: proceed_tx,
        },
    )
}

impl HashBarrier {
    fn arm(&self, hash: &str) {
        *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(hash.to_string());
    }
}

/// Deterministic, poorly-compressible bytes. The archive chunk size is a fixed
/// 4 MiB of *compressed* frames, so an overlap fixture only gets a second chunk
/// if its content resists compression.
fn noisy_bytes(seed: u64, len: usize) -> Vec<u8> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len + 16);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut bits = state;
        for _ in 0..10 {
            out.push(ALPHABET[(bits & 0x3f) as usize]);
            bits >>= 6;
        }
    }
    out.truncate(len);
    out
}

/// Deterministic, poorly-compressible text for the small editable fixtures.
fn noisy(seed: u64, len: usize) -> String {
    String::from_utf8(noisy_bytes(seed, len)).expect("alphabet is ascii")
}

/// The in-progress staging directory a clone into `<parent>/clone` creates.
fn staging_dir(parent: &Path) -> Option<PathBuf> {
    std::fs::read_dir(parent)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("clone.") && n.ends_with(".tmp"))
        })
}

/// Poll `probe` against the live staging directory until it reports success or
/// the deadline passes. Returns the last observed staging path for diagnostics.
async fn wait_for_staging<F>(parent: &Path, mut probe: F) -> (bool, Option<PathBuf>)
where
    F: FnMut(&Path) -> bool,
{
    let end = Instant::now() + OVERLAP_DEADLINE;
    let mut last = None;
    while Instant::now() < end {
        if let Some(staging) = staging_dir(parent) {
            last = Some(staging.clone());
            if probe(&staging) {
                return (true, last);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    (false, last)
}

fn pack_dir_entries(staging: &Path, suffix: &str) -> Vec<String> {
    let pack_dir = staging.join(".git").join("objects").join("pack");
    let Ok(entries) = std::fs::read_dir(&pack_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(suffix))
        .collect()
}

/// Files mode must start writing the working tree before every archive chunk has
/// arrived. Fails if the client collects all chunks first: with the final chunk
/// held open, no output file can appear until we release it.
#[tokio::test(flavor = "multi_thread")]
async fn files_extraction_writes_an_early_file_while_the_final_archive_chunk_is_held() {
    init(false);
    let (barrier, control) = hash_barrier(0);
    let server = start_server_with_barrier(barrier).await;

    // Archive chunks are a fixed 4 MiB of compressed frames, so the fixture has
    // to be large and incompressible enough to publish more than one chunk.
    let origin = make_origin("acme", "overlap-files");
    let files: Vec<(String, Vec<u8>)> = (0..8u64)
        .map(|i| (format!("f{i:02}.bin"), noisy_bytes(i + 1, 1 << 20)))
        .collect();
    let refs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(p, c)| (p.as_str(), c.as_slice()))
        .collect();
    origin.commit_bytes(&refs, "c1");
    origin.publish();

    let info = sync_until_archive_ready(&server, "acme", "overlap-files").await;
    let (manifest, _metadata) = server
        .client()
        .fetch_clonepack(&info)
        .await
        .expect("fetch clonepack");
    assert!(
        manifest.archive_chunks.len() >= 2,
        "the overlap fixture needs more than one archive chunk, got {}",
        manifest.archive_chunks.len()
    );
    let last_chunk = ripclone::clonepack::hash_to_hex(
        &manifest.archive_chunks.last().expect("archive chunk").hash,
    );
    control.arm(&last_chunk);

    let out = tempfile::tempdir().expect("clone out");
    let parent = out.path().to_path_buf();
    let target = parent.join("clone");
    let client = server.client();
    let clone = tokio::spawn(async move {
        client
            .install_repo_with_mode_at(
                "acme/overlap-files",
                "HEAD",
                None,
                &target,
                CloneMode::Files,
                Some("full"),
                None,
            )
            .await
    });

    control
        .entered
        .await
        .expect("final archive chunk must reach the barrier");

    // The final chunk is still open. Extraction must already have written a
    // complete earlier file into staging.
    let expected = files.clone();
    let (observed, staging) = wait_for_staging(&parent, move |staging| {
        expected.iter().any(|(name, content)| {
            std::fs::read(staging.join(name)).is_ok_and(|got| &got == content)
        })
    })
    .await;
    assert!(
        observed,
        "extraction must write an early file before the final archive chunk arrives (staging: {staging:?})"
    );

    control.proceed.send(()).expect("release final chunk");
    clone
        .await
        .expect("clone task joined")
        .expect("clone completes after the final chunk is released");
    let target = parent.join("clone");
    for (name, content) in &files {
        assert_eq!(
            &std::fs::read(target.join(name)).expect("read cloned file"),
            content,
            "cloned {name} must match the origin"
        );
    }
}

/// An editable clone must install an earlier pack and materialize its HEAD files
/// while a later pack is still downloading. Fails if the client waits for every
/// pack before doing any install work.
#[tokio::test(flavor = "multi_thread")]
async fn editable_installs_an_earlier_pack_while_a_later_pack_is_held() {
    init(false);
    let (barrier, control) = hash_barrier(64);
    let server = start_server_with_barrier(barrier).await;

    let origin = make_origin("acme", "overlap-editable");
    origin.commit(&[("a.txt", noisy(7, 4096).as_str())], "c1");
    origin.commit(
        &[
            ("a.txt", noisy(9, 4096).as_str()),
            ("b.txt", noisy(11, 4096).as_str()),
        ],
        "c2",
    );
    origin.publish();

    let info = sync_until_archive_ready(&server, "acme", "overlap-editable").await;
    let (manifest, metadata) = server
        .client()
        .fetch_clonepack(&info)
        .await
        .expect("fetch clonepack");
    assert!(
        manifest.packs.len() >= 2,
        "the overlap fixture needs at least two packs, got {}",
        manifest.packs.len()
    );
    assert!(
        !metadata.files.is_empty(),
        "the fixture must have HEAD files to materialize"
    );
    let last = manifest.packs.last().expect("pack entry");
    assert!(
        last.history_only,
        "the last full-variant pack is the history pack"
    );
    let last_pack = ripclone::clonepack::hash_to_hex(&last.pack.as_ref().expect("pack ref").hash);
    control.arm(&last_pack);

    let out = tempfile::tempdir().expect("clone out");
    let parent = out.path().to_path_buf();
    let target = parent.join("clone");
    let client = server.client();
    let clone = tokio::spawn(async move {
        client
            .install_repo_with_mode_at(
                "acme/overlap-editable",
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });

    control
        .entered
        .await
        .expect("the later pack must reach the barrier");

    let (installed, staging) = wait_for_staging(&parent, |staging| {
        // An earlier pack is installed …
        !pack_dir_entries(staging, ".pack").is_empty()
            // … and its HEAD blobs are materialized in the working tree.
            && std::fs::read_to_string(staging.join("b.txt")).is_ok()
    })
    .await;
    assert!(
        installed,
        "an earlier pack must be installed and its HEAD files materialized while a later pack is blocked (staging: {staging:?})"
    );

    control.proceed.send(()).expect("release the later pack");
    clone
        .await
        .expect("clone task joined")
        .expect("clone completes after the later pack is released");
    let target = parent.join("clone");
    assert_eq!(git(&target, &["rev-list", "--count", "HEAD"]), "2");
    assert_eq!(git(&target, &["status", "--porcelain"]), "");
}

/// A history-only pack is streamed into exactly one temporary file inside the
/// pack directory and renamed into place. Fails if the client buffers it, copies
/// it a second time, or re-reads it to hash it: mid-download there is exactly one
/// `.ripclone-download` file and no pack file for it, and after release there is
/// exactly one pack file and no temporary left.
#[tokio::test(flavor = "multi_thread")]
async fn editable_history_pack_streams_to_one_temp_file_then_is_renamed() {
    init(false);
    let (barrier, control) = hash_barrier(64);
    let server = start_server_with_barrier(barrier).await;

    let origin = make_origin("acme", "stream-history");
    origin.commit(&[("a.txt", noisy(3, 8192).as_str())], "c1");
    origin.commit(&[("a.txt", noisy(5, 8192).as_str())], "c2");
    origin.commit(&[("a.txt", noisy(13, 8192).as_str())], "c3");
    origin.publish();

    let info = sync_until_archive_ready(&server, "acme", "stream-history").await;
    let (manifest, _metadata) = server
        .client()
        .fetch_clonepack(&info)
        .await
        .expect("fetch clonepack");
    let history: Vec<_> = manifest.packs.iter().filter(|p| p.history_only).collect();
    assert_eq!(
        history.len(),
        1,
        "fixture must publish exactly one history pack, got {}",
        history.len()
    );
    let history_pack =
        ripclone::clonepack::hash_to_hex(&history[0].pack.as_ref().expect("pack ref").hash);
    control.arm(&history_pack);

    let out = tempfile::tempdir().expect("clone out");
    let parent = out.path().to_path_buf();
    let target = parent.join("clone");
    let client = server.client();
    let clone = tokio::spawn(async move {
        client
            .install_repo_with_mode_at(
                "acme/stream-history",
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });

    control
        .entered
        .await
        .expect("the history pack must reach the barrier");

    let (streaming, staging) = wait_for_staging(&parent, |staging| {
        pack_dir_entries(staging, ".ripclone-download").len() == 1
    })
    .await;
    let staging = staging.expect("clone staging directory");
    assert!(
        streaming,
        "the history pack must stream into exactly one temporary file, saw {:?}",
        pack_dir_entries(&staging, ".ripclone-download")
    );
    let packs_mid_download = pack_dir_entries(&staging, ".pack");
    assert!(
        packs_mid_download.len() < manifest.packs.len(),
        "the streamed history pack must not be installed before its body finishes: {packs_mid_download:?}"
    );

    control.proceed.send(()).expect("release the history pack");
    clone
        .await
        .expect("clone task joined")
        .expect("clone completes after the history pack is released");

    let target = parent.join("clone");
    let installed: Vec<String> = std::fs::read_dir(target.join(".git/objects/pack"))
        .expect("read installed pack dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        installed.iter().all(|n| !n.ends_with(".ripclone-download")),
        "the streamed temporary file must be renamed, not left behind: {installed:?}"
    );
    assert_eq!(
        installed.iter().filter(|n| n.ends_with(".pack")).count(),
        manifest.packs.len(),
        "every pack is installed exactly once: {installed:?}"
    );
    assert_eq!(git(&target, &["rev-list", "--count", "HEAD"]), "3");
}

/// A transient 503 on artifact GETs must be retried on the same URL and the
/// editable clone must still produce a correct repo. (The files-mode sibling
/// lives in `e2e_roundtrip.rs`.)
#[tokio::test]
async fn transient_pack_failure_is_retried_and_the_editable_clone_succeeds() {
    init(false);
    // Below the default 3-attempt budget, so however the faults distribute
    // across the concurrent manifest/idx/pack fetches, each artifact recovers.
    let server = start_server_faulting(2).await;
    let origin = make_origin("acme", "retry-editable");
    origin.commit(&[("a.txt", "one\n")], "c1");
    origin.commit(&[("a.txt", "two\n"), ("dir/b.txt", "x\n")], "c2");
    origin.publish();
    sync_until_manifest(&server, "acme", "retry-editable").await;

    let (_out, target) = clone_only(&server, "acme", "retry-editable", 0, CloneMode::Editable)
        .await
        .expect("retried pack fetches must still install the repo");
    assert_eq!(read(&target, "a.txt"), "two\n");
    assert_eq!(read(&target, "dir/b.txt"), "x\n");
    assert_eq!(git(&target, &["rev-list", "--count", "HEAD"]), "2");
    assert!(git_ok(&target, &["fsck", "--connectivity-only", "HEAD"]));
}

/// Tamper with the published bytes of one archive chunk, then prove the clone
/// fails on the classification the tampering implies, publishes no target, and
/// never asks the clone driver to refresh URLs.
async fn assert_chunk_tampering_fails_permanently(
    repo: &str,
    tamper: impl Fn(&Path, u64),
    want_message: &str,
) {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", repo);
    origin.commit(
        &[
            ("a.txt", noisy(21, 4096).as_str()),
            ("b.txt", noisy(23, 4096).as_str()),
        ],
        "c1",
    );
    origin.publish();

    let repo_path = format!("acme/{repo}");
    let info = sync_until_archive_ready(&server, "acme", repo).await;
    let client = server.client();
    let (manifest, _metadata) = client
        .fetch_clonepack(&info)
        .await
        .expect("fetch clonepack");
    let chunk = manifest
        .archive_chunks
        .first()
        .expect("fixture publishes an archive chunk");
    let chunk_hex = ripclone::clonepack::hash_to_hex(&chunk.hash);
    let path = server.cas_path(&chunk_hex);
    assert!(path.exists(), "archive chunk must be published");
    tamper(&path, chunk.len);

    let out = tempfile::tempdir().expect("clone out");
    let target = out.path().join("clone");
    let error = client
        .install_repo_with_mode_at(
            &repo_path,
            "HEAD",
            None,
            &target,
            CloneMode::Files,
            Some("full"),
            None,
        )
        .await
        .expect_err("tampered artifact must fail the clone");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains(want_message),
        "expected {want_message:?} in the failure, got: {rendered}"
    );
    assert!(
        !ripclone::client::is_stale_signed_url(&error),
        "a permanent artifact failure must not enter the signed-URL refresh loop: {rendered}"
    );
    assert!(
        !target.exists(),
        "a failed clone must publish no target: {}",
        target.display()
    );
    let leftovers: Vec<String> = std::fs::read_dir(out.path())
        .expect("read clone parent")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        leftovers.is_empty(),
        "a failed clone must leave no staging directory: {leftovers:?}"
    );
    // The verified cache never records a bad object.
    assert!(
        ripclone::cas::Cas::new(&server.cas_dir)
            .expect("open server cas")
            .verify_object(&chunk_hex)
            .is_err(),
        "the tampered fixture must still be tampered"
    );
}

#[tokio::test]
async fn missing_archive_chunk_fails_without_a_url_refresh() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "gone-chunk");
    origin.commit(&[("a.txt", noisy(31, 4096).as_str())], "c1");
    origin.publish();

    let info = sync_until_archive_ready(&server, "acme", "gone-chunk").await;
    let client = server.client();
    let (manifest, _metadata) = client
        .fetch_clonepack(&info)
        .await
        .expect("fetch clonepack");
    let chunk_hex = ripclone::clonepack::hash_to_hex(
        &manifest
            .archive_chunks
            .first()
            .expect("fixture publishes an archive chunk")
            .hash,
    );
    std::fs::remove_file(server.cas_path(&chunk_hex)).expect("delete published chunk");

    let out = tempfile::tempdir().expect("clone out");
    let target = out.path().join("clone");
    let error = client
        .install_repo_with_mode_at(
            "acme/gone-chunk",
            "HEAD",
            None,
            &target,
            CloneMode::Files,
            Some("full"),
            None,
        )
        .await
        .expect_err("a missing artifact must fail the clone");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("404"),
        "a missing artifact must surface its 404: {rendered}"
    );
    assert!(
        !ripclone::client::is_stale_signed_url(&error),
        "a missing artifact must not enter the signed-URL refresh loop: {rendered}"
    );
    assert!(!target.exists(), "a failed clone must publish no target");
}

#[tokio::test]
async fn short_archive_chunk_fails_on_length_without_a_url_refresh() {
    assert_chunk_tampering_fails_permanently(
        "short-chunk",
        |path, _len| std::fs::write(path, b"truncated").expect("truncate published chunk"),
        "size mismatch",
    )
    .await;
}

#[tokio::test]
async fn corrupt_archive_chunk_fails_on_hash_without_a_url_refresh() {
    assert_chunk_tampering_fails_permanently(
        "corrupt-chunk",
        |path, len| {
            // Same length, different bytes: the length check passes and the
            // content address is what catches it.
            let filler = vec![b'z'; usize::try_from(len).expect("chunk length fits usize")];
            std::fs::write(path, &filler).expect("rewrite published chunk");
        },
        "hash mismatch",
    )
    .await;
}
