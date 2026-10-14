//! F. Downloads: the client's rules for broken, short, and interrupted
//! artifact responses, with faults injected by a proxy in front of the server.

use crate::harness::*;

/// Artifacts at least this large are the history pack holding the big blob.
pub const BIG: usize = 1 << 20;

/// Deterministic incompressible bytes.
fn noise(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// An origin whose history (not its tip) holds a 2 MiB blob, so a full clone
/// streams one large history pack. Returns the origin and its tip.
pub fn big_history_origin(world: &World, name: &str) -> (Origin, String) {
    let origin = world.origin("acme", name);
    std::fs::write(origin.work.join("blob.bin"), noise(2 << 20)).unwrap();
    origin.commit(&[("README.md", "a\n")], "A");
    std::fs::write(origin.work.join("blob.bin"), "small\n").unwrap();
    let tip = origin.commit(&[("README.md", "b\n")], "B");
    origin.publish();
    (origin, tip)
}

/// [`big_history_origin`], added to a server behind a fault proxy.
fn big_history(world: &World, name: &str) -> (Origin, Server, FaultProxy, String) {
    let (origin, tip) = big_history_origin(world, name);
    let server = world.server(&[]);
    world
        .cli(&server, &["add", &format!("acme/{name}"), "--wait"])
        .ok();
    let proxy = FaultProxy::start(&server);
    (origin, server, proxy, tip)
}

/// GETs of the one large artifact, in order.
fn big_gets(proxy: &FaultProxy, big_path: &str) -> Vec<Seen> {
    proxy
        .artifact_gets()
        .into_iter()
        .filter(|s| s.path == big_path)
        .collect()
}

/// The path of the first artifact a fault rule hit.
fn faulted_path(proxy: &FaultProxy) -> String {
    let gets = proxy.artifact_gets();
    gets.iter()
        .find(|s| s.faulted)
        .map(|s| s.path.clone())
        .unwrap_or_else(|| panic!("no faulted artifact among {gets:#?}"))
}

/// The object names in a client artifact cache.
fn cached_objects(dir: &std::path::Path) -> Vec<String> {
    let Ok(shards) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    shards
        .flatten()
        .filter(|e| e.path().is_dir())
        .flat_map(|e| std::fs::read_dir(e.path()).unwrap().flatten())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

fn assert_clean_failure(world: &World, target: &std::path::Path) {
    assert!(!target.exists(), "failed clone left {}", target.display());
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());
}

/// F1: one flipped byte fails the clone on the hash check, leaves nothing,
/// and puts nothing in the client's artifact cache. Covers the streamed
/// history pack and a buffered artifact (the only kind the cache holds).
#[test]
fn f1_flipped_byte_fails_on_hash() {
    let world = World::new();
    let (_origin, server, _proxy, _tip) = big_history(&world, "f1");
    let cache = world.path("cli-cache");
    let env = [("RIPCLONE_CACHE_DIR", cache.to_str().unwrap())];

    // Control: an intact clone caches its buffered artifacts.
    let warm = world.target("f1-warm");
    world
        .cli_env(&server, &["clone", "acme/f1", warm.to_str().unwrap()], &env)
        .ok();
    let buffered = cached_objects(&cache);
    assert!(!buffered.is_empty(), "an intact clone cached nothing");

    // `None` faults the streamed pack; `Some` one buffered artifact.
    for (case, buffered_hash) in [("streamed", None), ("buffered", Some(&buffered[0]))] {
        std::fs::remove_dir_all(&cache).unwrap();
        let proxy = FaultProxy::start(&server);
        match buffered_hash {
            None => proxy.fault_artifacts(Fault::FlipByte, BIG, 1),
            Some(hash) => {
                proxy.fault_requests(&format!("/v1/artifacts/{hash}"), Fault::FlipByte, 1)
            }
        }

        let target = world.target(&format!("f1-{case}"));
        let err = world
            .cli_env(
                &proxy,
                &["clone", "acme/f1", target.to_str().unwrap()],
                &env,
            )
            .err();

        assert!(err.contains("hash mismatch"), "{case}: {err}");
        assert_clean_failure(&world, &target);
        let path = faulted_path(&proxy);
        let hash = path.rsplit('/').next().unwrap();
        assert!(
            !cached_objects(&cache).iter().any(|h| h == hash),
            "{case}: the corrupt artifact {hash} was cached"
        );
    }
}

/// F2: a body shorter than the manifest promised fails at once, without a
/// retry or a URL refresh.
#[test]
fn f2_truncated_body_fails_without_retry() {
    let world = World::new();
    let (_origin, _server, proxy, _tip) = big_history(&world, "f2");
    proxy.fault_artifacts(Fault::Truncate(BIG / 2), BIG, 1);

    let target = world.target("f2");
    let err = world
        .cli(&proxy, &["clone", "acme/f2", target.to_str().unwrap()])
        .err();

    assert!(err.contains("size mismatch"), "{err}");
    let gets = proxy.artifact_gets();
    let count = |p: &str| gets.iter().filter(|s| s.path == p).count();
    assert!(
        gets.iter().all(|s| count(&s.path) == 1),
        "an artifact was fetched twice: {gets:#?}"
    );
    let refreshes = proxy
        .seen()
        .into_iter()
        .filter(|s| s.path.contains("/refs/") && s.query.contains("pinned="))
        .count();
    assert_eq!(refreshes, 0, "a short body must not refresh URLs");
    assert_clean_failure(&world, &target);
}

/// F3: a 404 for an artifact fails at once.
#[test]
fn f3_missing_artifact_fails_at_once() {
    let world = World::new();
    let (_origin, _server, proxy, _tip) = big_history(&world, "f3");
    proxy.fault_artifacts(Fault::Status(404), BIG, 1);

    let target = world.target("f3");
    let err = world
        .cli(&proxy, &["clone", "acme/f3", target.to_str().unwrap()])
        .err();

    assert!(err.contains("404"), "{err}");
    let path = faulted_path(&proxy);
    assert_eq!(
        big_gets(&proxy, &path).len(),
        1,
        "a 404 must not be retried"
    );
    assert_clean_failure(&world, &target);
}

/// F4: one 503 is retried and the clone succeeds.
#[test]
fn f4_one_503_is_retried() {
    let world = World::new();
    let (origin, _server, proxy, tip) = big_history(&world, "f4");
    proxy.fault_artifacts(Fault::Status(503), BIG, 1);

    let target = world.target("f4");
    world
        .cli(&proxy, &["clone", "acme/f4", target.to_str().unwrap()])
        .ok();

    let path = faulted_path(&proxy);
    let statuses: Vec<u16> = big_gets(&proxy, &path).iter().map(|s| s.status).collect();
    assert_eq!(statuses, [503, 200]);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    git(&target, &["fsck", "--full"]);
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// F5: a connection cut mid-body resumes with Range from the saved offset.
#[test]
fn f5_cut_connection_resumes_with_range() {
    let world = World::new();
    let (origin, _server, proxy, tip) = big_history(&world, "f5");
    let cut = 256 * 1024;
    proxy.fault_artifacts(Fault::CutAfter(cut), BIG, 1);

    let target = world.target("f5");
    world
        .cli(&proxy, &["clone", "acme/f5", target.to_str().unwrap()])
        .ok();

    let path = faulted_path(&proxy);
    let gets = big_gets(&proxy, &path);
    let ranges: Vec<Option<String>> = gets.iter().map(|s| s.range.clone()).collect();
    assert_eq!(ranges, [None, Some(format!("bytes={cut}-"))], "{gets:#?}");
    assert_eq!(gets[1].status, 206);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    git(&target, &["fsck", "--full"]);
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// F6: when Range is ignored, only the interrupted artifact starts over.
#[test]
fn f6_ignored_range_restarts_only_that_artifact() {
    let world = World::new();
    let (origin, _server, proxy, tip) = big_history(&world, "f6");
    proxy.ignore_range();
    proxy.fault_artifacts(Fault::CutAfter(256 * 1024), BIG, 1);

    let target = world.target("f6");
    world
        .cli(&proxy, &["clone", "acme/f6", target.to_str().unwrap()])
        .ok();

    // The proxy stripped Range before forwarding, so the retry got a 200.
    let gets = proxy.artifact_gets();
    let path = faulted_path(&proxy);
    let statuses: Vec<u16> = big_gets(&proxy, &path).iter().map(|s| s.status).collect();
    assert_eq!(statuses, [200, 200]);
    for get in &gets {
        if get.path != path {
            let n = gets.iter().filter(|s| s.path == get.path).count();
            assert_eq!(n, 1, "{} was fetched {n} times", get.path);
        }
    }
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    git(&target, &["fsck", "--full"]);
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// F8: Ctrl-C during the download removes the staging directory and exits
/// 130; the next clone into the same path succeeds.
#[test]
fn f8_interrupted_clone_cleans_up() {
    let world = World::new();
    let (origin, _server, proxy, tip) = big_history(&world, "f8");
    proxy.fault_artifacts(Fault::Hold, BIG, usize::MAX);

    let target = world.target("f8");
    let clone = world.cli_spawn(&proxy, &["clone", "acme/f8", target.to_str().unwrap()]);
    proxy.wait_held(1);
    assert!(
        !world.staging_leftovers().is_empty(),
        "the clone should be staging while its download is held"
    );
    clone.interrupt();
    let out = clone.wait();
    out.err();
    assert_eq!(out.code(), Some(130), "{}", out.stderr());
    assert!(
        !target.exists(),
        "interrupted clone left {}",
        target.display()
    );
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());

    proxy.release();
    world
        .cli(&proxy, &["clone", "acme/f8", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));
}
