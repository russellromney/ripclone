//! F7 and H2 against S3 storage: a versitygw emulator per test, started by
//! `scripts/s3_emulator.sh`. Ignored by default because the script fetches
//! the emulator binary; the `s3` CI job runs them with `--ignored`.

use crate::downloads::{BIG, big_history_origin};
use crate::harness::*;
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
