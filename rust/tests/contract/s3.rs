//! C2, C3, C7, C8, F7, H2, H6 and H10 against S3 storage: a versitygw emulator per test,
//! started by `scripts/s3_emulator.sh`. Ignored by default because the script fetches
//! the emulator binary; the `s3` CI job runs them with `--ignored`.

use crate::downloads::{BIG, big_history_origin};
use crate::harness::*;
use crate::outputs::output_states;
use std::collections::BTreeMap;
use std::time::Duration;

/// F7: a signed URL expires mid-download. The CLI refreshes URLs for the same
/// commit and resumes from the saved offset.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_f7_expired_url_refreshes_and_resumes() {
    let world = World::new();
    let s3 = S3::start(&world);
    // The server signs URLs for the proxy's address, so downloads pass through
    // it; it keeps the Host header so the signatures still verify.
    let s3_proxy = FaultProxy::start(&s3);
    s3_proxy.preserve_host();
    let (origin, tip) = big_history_origin(&world, "f7");
    let mut env = s3.server_env(&s3_proxy.url());
    env.push(("RIPCLONE_SIGNED_URL_TTL_SECS".into(), "2".into()));
    let server = world.server(&env_refs(&env));
    world.cli(&server, &["add", "acme/f7", "--wait"]).ok();

    // Send part of the large pack, stall past the URL's lifetime, then drop
    // the connection: the resume reaches S3 with an expired URL.
    let cut = 256 * 1024;
    let bucket = format!("/{}/", s3.bucket);
    s3_proxy.fault_large(
        &bucket,
        Fault::StallThenCut(cut, Duration::from_secs(4)),
        BIG,
        1,
    );
    let server_proxy = FaultProxy::start(&server);
    let target = world.target("f7");
    world
        .cli(
            &server_proxy,
            &["clone", "acme/f7", target.to_str().unwrap()],
        )
        .ok();

    let mut by_path: BTreeMap<String, Vec<Seen>> = BTreeMap::new();
    for get in s3_proxy.gets(&bucket) {
        by_path.entry(get.path.clone()).or_default().push(get);
    }
    let big = by_path
        .values()
        .find(|gets| gets.iter().any(|g| g.range.is_some()))
        .unwrap_or_else(|| panic!("no resumed download among {by_path:#?}"));
    let statuses: Vec<u16> = big.iter().map(|g| g.status).collect();
    let ranges: Vec<Option<String>> = big.iter().map(|g| g.range.clone()).collect();
    let resume = Some(format!("bytes={cut}-"));
    assert_eq!(statuses, [200, 403, 206], "{big:#?}");
    assert_eq!(ranges, [None, resume.clone(), resume], "{big:#?}");
    assert_ne!(
        big[0].query, big[2].query,
        "the resume reused the expired URL"
    );

    // The first read of the commit finds it ready; later reads refresh URLs.
    // Every read names the same commit.
    let reads: Vec<Seen> = server_proxy
        .seen()
        .into_iter()
        .filter(|s| s.path.contains(api::COMMIT_READS))
        .collect();
    assert!(
        reads.len() >= 2 && reads.iter().all(|s| api::is_commit_read(s, &tip)),
        "the CLI never refreshed URLs for {tip}: {reads:#?}"
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    git(&target, &["fsck", "--full"]);
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// H2: artifacts are in the bucket. With the server's local cache deleted and
/// the server restarted, they still clone without a rebuild.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_h2_artifacts_live_in_bucket_and_survive_restart() {
    let world = World::new();
    let s3 = S3::start(&world);
    let origin = world.origin("acme", "h2");
    let tip = origin.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "A");
    origin.publish();
    let env = s3.server_env(&s3.endpoint);
    let mut server = world.server(&env_refs(&env));
    world.cli(&server, &["add", "acme/h2", "--wait"]).ok();

    let objects = s3.objects();
    assert!(!objects.is_empty(), "the build put nothing in the bucket");

    server.kill();
    let cache = world.path("server").join("cache");
    if cache.exists() {
        std::fs::remove_dir_all(&cache).unwrap();
    }
    server.restart();
    let target = world.target("h2");
    world
        .cli(&server, &["clone", "acme/h2", target.to_str().unwrap()])
        .ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));
    assert_eq!(server.metric("ripclone_builds_queued_total"), 0);
    assert_eq!(s3.objects(), objects);
}

/// Head(B) ready on a server with no Files or Full slots; then a restart
/// with them on and the bucket refusing every PUT, until Full and Files have
/// failed. Returns the S3 proxy so the caller keeps it alive.
fn background_failed(world: &World, s3: &S3, name: &str) -> (Origin, Server, FaultProxy, String) {
    let s3_proxy = FaultProxy::start(s3);
    s3_proxy.preserve_host();
    let origin = world.origin("acme", name);
    let tip = origin.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "B");
    origin.publish();
    let repo = format!("acme/{name}");
    let mut env = s3.server_env(&s3_proxy.url());
    env.push(("RIPCLONE_FILES_SLOTS".into(), "0".into()));
    env.push(("RIPCLONE_FULL_SLOTS".into(), "0".into()));
    let mut server = world.server(&env_refs(&env));
    world.cli(&server, &["add", &repo]).ok();
    wait_for("Head ready", || {
        (output_states(&server, &repo, &tip) == ["ready", "pending", "pending"]).then_some(())
    });

    // A 403 is permanent, so each output fails on its first attempt.
    s3_proxy.fail_puts(&format!("/{}/", s3.bucket), Fault::Status(403));
    server.restart_with(&[("RIPCLONE_FILES_SLOTS", "1"), ("RIPCLONE_FULL_SLOTS", "1")]);
    wait_for("Full and Files failed", || {
        (output_states(&server, &repo, &tip) == ["ready", "failed", "failed"]).then_some(())
    });
    (origin, server, s3_proxy, tip)
}

/// C2: storage rejects the background writes. Full and Files fail with the
/// storage error, Head still clones, and a full clone fails clearly.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_c2_failed_background_outputs_leave_head_cloneable() {
    let world = World::new();
    let s3 = S3::start(&world);
    let (origin, server, s3_proxy, tip) = background_failed(&world, &s3, "c2");
    assert!(
        s3_proxy
            .seen()
            .iter()
            .any(|s| s.method == "PUT" && s.faulted),
        "no write reached the failing bucket"
    );
    let (_, summary) = api::read_commit(&server, "acme/c2", &tip, None);
    for kind in ["full", "files"] {
        let error = summary[kind]["error"].as_str().unwrap_or_default();
        assert!(
            !error.is_empty() && !error.starts_with("gave up"),
            "{kind} should fail with the storage error: {summary}"
        );
    }

    let shallow = world.target("c2-shallow");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/c2",
                shallow.to_str().unwrap(),
                "--depth",
                "1",
            ],
        )
        .ok();
    assert_eq!(git(&shallow, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&shallow), origin.tree(&tip));

    let full = world.target("c2-full");
    let out = world.cli(&server, &["clone", "acme/c2", full.to_str().unwrap()]);
    let err = out.err();
    assert!(
        err.contains("failed") && err.contains("ripclone sync"),
        "{err}"
    );
    assert!(!full.exists(), "failed clone left {}", full.display());
}

/// C7: a new push's Files does not wait behind another commit's Full. The
/// bucket holds Full(B)'s first write while push C arrives: C's Head and
/// Files become ready with Full(B) still building. Once released, Full(B)
/// lands before Full(C), so Full(C) builds on B's history.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_c7_new_push_files_skip_a_running_full() {
    let world = World::new();
    let s3 = S3::start(&world);
    let s3_proxy = FaultProxy::start(&s3);
    s3_proxy.preserve_host();
    let origin = world.origin("acme", "c7");
    let b = origin.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "B");
    origin.publish();
    let mut env = s3.server_env(&s3_proxy.url());
    env.push(("RIPCLONE_FULL_SLOTS".into(), "0".into()));
    let mut server = world.server(&env_refs(&env));
    world.cli(&server, &["add", "acme/c7"]).ok();
    wait_for("Head(B) and Files(B) ready", || {
        (output_states(&server, "acme/c7", &b) == ["ready", "pending", "ready"]).then_some(())
    });

    // Nothing else writes while Full(B) starts, so the held PUT is Full's.
    s3_proxy.hold_puts(&format!("/{}/", s3.bucket), 1);
    server.restart_with(&[("RIPCLONE_FULL_SLOTS", "1")]);
    s3_proxy.wait_held(1);

    let c = origin.commit(&[("a.txt", "c\n")], "C");
    origin.publish();
    world.cli(&server, &["sync", "acme/c7"]).ok();
    wait_for("Head(C) and Files(C) ready", || {
        (output_states(&server, "acme/c7", &c) == ["ready", "pending", "ready"]).then_some(())
    });
    assert_eq!(
        output_states(&server, "acme/c7", &b),
        ["ready", "pending", "ready"],
        "Full(B) finished while its write was held"
    );

    s3_proxy.release();
    wait_for("Full(C) ready", || {
        (output_states(&server, "acme/c7", &c) == ["ready"; 3]).then_some(())
    });
    assert_eq!(output_states(&server, "acme/c7", &b), ["ready"; 3]);
    let log = server.log();
    let published = |commit: &str| {
        let suffix = format!("/acme%2Fc7@{}", &commit[..7]);
        log.lines()
            .position(|l| l.contains("published full result for") && l.ends_with(&suffix))
            .unwrap_or_else(|| panic!("no Full published for {commit}:\n{log}"))
    };
    assert!(
        published(&b) < published(&c),
        "Full(C) before Full(B):\n{log}"
    );
}

/// C3: with the fault gone, `sync --at` retries the failed outputs. Only Full
/// and Files rebuild; Head is not published again.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_c3_retry_rebuilds_only_failed_outputs() {
    let world = World::new();
    let s3 = S3::start(&world);
    let (origin, mut server, _s3_proxy, tip) = background_failed(&world, &s3, "c3");

    let direct = s3.server_env(&s3.endpoint);
    server.restart_with(&env_refs(&direct));
    world.cli(&server, &["sync", "acme/c3", "--at", &tip]).ok();
    assert_eq!(output_states(&server, "acme/c3", &tip), ["ready"; 3]);
    let published = |kind: &str| {
        server.metric(&format!(
            "ripclone_outputs_published_total{{kind=\"{kind}\"}}"
        ))
    };
    assert_eq!(
        (published("head"), published("full"), published("files")),
        (0, 1, 1)
    );
    assert_eq!(server.metric("ripclone_builds_queued_total"), 0);

    let target = world.target("c3");
    world
        .cli(&server, &["clone", "acme/c3", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    git(&target, &["fsck", "--full"]);
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// H6: with S3 storage the server's local cache is disposable. Retention
/// trims cache objects older than its limit once their bucket copy is
/// confirmed; the bucket is untouched, published artifacts still clone
/// without a rebuild, and the next push still builds.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_h6_retention_trims_only_the_local_cache() {
    let world = World::new();
    let s3 = S3::start(&world);
    let origin = world.origin("acme", "h6");
    let a = origin.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "A");
    origin.publish();
    let mut env = s3.server_env(&s3.endpoint);
    env.push(("RIPCLONE_RETENTION_INTERVAL_SECS".into(), "1".into()));
    env.push(("RIPCLONE_RETENTION_MAX_AGE_DAYS".into(), "1".into()));
    let server = world.server(&env_refs(&env));
    world.cli(&server, &["add", "acme/h6", "--wait"]).ok();
    let objects = s3.objects();
    let cached = server.storage_listing();
    assert!(!cached.is_empty(), "the build cached nothing locally");

    crate::state::backdate(&world.path("server").join("cache"), 30);
    wait_for("retention to trim the local cache", || {
        (server.metric("ripclone_local_cache_removed_objects_total") > 0).then_some(())
    });
    assert!(server.storage_listing().len() < cached.len());
    assert_eq!(s3.objects(), objects, "retention changed the bucket");

    let queued = server.metric("ripclone_builds_queued_total");
    let target = world.target("h6-a");
    world
        .cli(&server, &["clone", "acme/h6", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), a);
    assert_eq!(listing(&target), origin.tree(&a));
    assert_eq!(server.metric("ripclone_builds_queued_total"), queued);

    let b = origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/h6", "--wait"]).ok();
    for (shape, args) in [("full", &[][..]), ("files", &["--mode", "files"][..])] {
        let target = world.target(&format!("h6-b-{shape}"));
        let mut argv = vec!["clone", "acme/h6", target.to_str().unwrap()];
        argv.extend_from_slice(args);
        world.cli(&server, &argv).ok();
        assert_eq!(listing(&target), origin.tree(&b), "{shape}");
    }
    assert_eq!(
        server.metric("ripclone_local_cache_cleanup_errors_total"),
        0
    );
}

/// C8: the bucket fails a write while Head is building. One write is held in
/// flight and the next is refused: Head fails with the storage error and
/// publishes nothing, so a depth-1 clone fails and leaves no target. With
/// the fault gone, `sync --at` builds every output and each shape clones
/// exactly.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_c8_failed_upload_publishes_nothing() {
    let world = World::new();
    let s3 = S3::start(&world);
    let s3_proxy = FaultProxy::start(&s3);
    s3_proxy.preserve_host();
    let origin = world.origin("acme", "c8");
    let tip = origin.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "A");
    origin.publish();
    let mut env = s3.server_env(&s3_proxy.url());
    env.push(("RIPCLONE_FILES_SLOTS".into(), "0".into()));
    env.push(("RIPCLONE_FULL_SLOTS".into(), "0".into()));
    let mut server = world.server(&env_refs(&env));

    let bucket = format!("/{}/", s3.bucket);
    s3_proxy.hold_puts(&bucket, 1);
    // A 403 is permanent, so Head fails on its first attempt.
    s3_proxy.fail_puts(&bucket, Fault::Status(403));
    world.cli(&server, &["add", "acme/c8"]).ok();
    s3_proxy.wait_held(1);
    wait_for("Head failed", || {
        (output_states(&server, "acme/c8", &tip)[0] == "failed").then_some(())
    });
    let (_, summary) = api::read_commit(&server, "acme/c8", &tip, None);
    let error = summary["head"]["error"].as_str().unwrap_or_default();
    assert!(error.contains("upload"), "{summary}");
    assert_eq!(
        server.metric("ripclone_outputs_published_total{kind=\"head\"}"),
        0
    );
    let shallow = world.target("c8-failed");
    let out = world.cli(
        &server,
        &[
            "clone",
            "acme/c8",
            shallow.to_str().unwrap(),
            "--depth",
            "1",
        ],
    );
    assert!(!out.success(), "a clone of an unpublished Head succeeded");
    assert!(!shallow.exists(), "failed clone left {}", shallow.display());

    s3_proxy.release();
    server.restart_with(&[("RIPCLONE_FILES_SLOTS", "1"), ("RIPCLONE_FULL_SLOTS", "1")]);
    world.cli(&server, &["sync", "acme/c8", "--at", &tip]).ok();
    assert_eq!(output_states(&server, "acme/c8", &tip), ["ready"; 3]);
    clone_every_shape(&world, &server, "acme/c8", &origin, &tip);
}

/// Clone `repo` at depth 1, in full and as files, and compare each with the
/// origin at `tip`.
fn clone_every_shape(world: &World, server: &Server, repo: &str, origin: &Origin, tip: &str) {
    let shapes: [(&str, &[&str]); 3] = [
        ("depth1", &["--depth", "1"]),
        ("full", &[]),
        ("files", &["--mode", "files"]),
    ];
    for (shape, args) in shapes {
        let target = world.target(&format!("{}-{shape}-{}", repo.replace('/', "-"), &tip[..7]));
        let mut argv = vec!["clone", repo, target.to_str().unwrap()];
        argv.extend_from_slice(args);
        world.cli(server, &argv).ok();
        assert_eq!(listing(&target), origin.tree(tip), "{shape}");
        if shape == "full" {
            assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
            assert_eq!(
                git(&target, &["rev-list", "--count", "HEAD"]),
                origin.count(tip)
            );
            git(&target, &["fsck", "--full"]);
        }
    }
}

/// An origin whose commit A holds 6 MiB of incompressible files, the same
/// bytes for every `name`.
fn noisy_origin(world: &World, name: &str) -> Origin {
    let origin = world.origin("acme", name);
    for i in 0..3u64 {
        let path = origin.work.join(format!("data/{i}.bin"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, crate::downloads::noise(i + 1, 2 << 20)).unwrap();
    }
    origin.commit(&[("zz.txt", "a\n")], "A");
    origin.publish();
    origin
}

/// H10: a build does not upload an object of 1 MiB or more that the bucket
/// already holds at the same length. After a one-line push, B's Files
/// rebuilds A's unchanged archive frames; the server finds each with a HEAD
/// and writes only small objects over keys the bucket had. A bucket copy cut
/// short is uploaded again: a second repository with the same files builds
/// the same frames, and its HEAD sees the wrong length. Every shape of the
/// second repository clones exactly, and so does B's Files clone, which
/// reads the repaired objects.
#[test]
#[ignore = "needs the S3 emulator; run by the s3 CI job"]
fn s3_h10_existing_objects_are_not_uploaded_again() {
    const LARGE: u64 = 1 << 20;
    let world = World::new();
    let s3 = S3::start(&world);
    let s3_proxy = FaultProxy::start(&s3);
    s3_proxy.preserve_host();
    let origin = noisy_origin(&world, "h10");
    let env = s3.server_env(&s3_proxy.url());
    let server = world.server(&env_refs(&env));
    world.cli(&server, &["add", "acme/h10", "--wait"]).ok();

    let bucket = format!("/{}/", s3.bucket);
    let size = |key: &str| std::fs::metadata(s3.object_file(key)).unwrap().len();
    // Run `build` and return each key the bucket held before with its
    // length, and the bucket requests made meanwhile as (method, key).
    let observe = |build: &dyn Fn()| {
        let before: BTreeMap<String, u64> = s3
            .objects()
            .into_iter()
            .map(|key| {
                let len = size(&key);
                (key, len)
            })
            .collect();
        let mark = s3_proxy.seen().len();
        build();
        let requests: Vec<(String, String)> = s3_proxy.seen()[mark..]
            .iter()
            .filter_map(|s| {
                let key = s.path.strip_prefix(&bucket)?;
                Some((s.method.clone(), key.to_string()))
            })
            .collect();
        (before, requests)
    };
    let put = |requests: &[(String, String)], key: &str| {
        requests.iter().any(|(m, k)| m == "PUT" && k == key)
    };

    let b = origin.commit(&[("zz.txt", "b\n")], "B");
    origin.publish();
    let (before, requests) = observe(&|| {
        world.cli(&server, &["sync", "acme/h10", "--wait"]).ok();
    });
    for (method, key) in &requests {
        if method == "PUT"
            && let Some(len) = before.get(key)
        {
            assert!(*len < LARGE, "B uploaded {key} ({len} bytes) again");
        }
    }
    let skipped: Vec<String> = requests
        .iter()
        .filter(|(m, k)| m == "HEAD" && before.get(k).is_some_and(|len| *len >= LARGE))
        .map(|(_, k)| k.clone())
        .filter(|k| !put(&requests, k))
        .collect();
    assert!(
        !skipped.is_empty(),
        "B checked no large object it already had: {requests:#?}"
    );

    // Cut the skipped objects short in the bucket, then build a second
    // repository with the same files: it makes those objects again, and its
    // HEAD sees the wrong length.
    for key in &skipped {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(s3.object_file(key))
            .unwrap();
        file.set_len(before[key] / 2).unwrap();
    }
    let twin = noisy_origin(&world, "h10-twin");
    let (_, requests) = observe(&|| {
        world.cli(&server, &["add", "acme/h10-twin", "--wait"]).ok();
    });
    for key in &skipped {
        assert!(
            put(&requests, key),
            "the twin did not repair {key}: {requests:#?}"
        );
        assert_eq!(size(key), before[key], "{key}");
    }
    let twin_tip = twin.head();
    clone_every_shape(&world, &server, "acme/h10-twin", &twin, &twin_tip);
    // B's Files clone reads the repaired frames.
    let files = world.target("h10-b-files");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/h10",
                files.to_str().unwrap(),
                "--mode",
                "files",
            ],
        )
        .ok();
    assert_eq!(listing(&files), origin.tree(&b));
}
