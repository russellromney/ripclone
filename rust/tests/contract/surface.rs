//! I. Auth and integrations.

use crate::harness::*;
use reqwest::Method;

/// I1: the server token is required; a missing or wrong token gets 401.
#[test]
fn i1_server_token_is_required() {
    let world = World::new();
    let server = world.server(&[]);

    assert_eq!(server.request(Method::GET, "/v1/repos", None).status, 401);
    assert_eq!(
        server
            .request(Method::GET, "/v1/repos", Some("wrong-token"))
            .status,
        401
    );
    assert_eq!(
        server.request(Method::GET, "/v1/repos", Some(TOKEN)).status,
        200
    );

    world
        .cli_env(
            &server,
            &["list"],
            &[("RIPCLONE_SERVER_TOKEN", "wrong-token")],
        )
        .err();
    world.cli(&server, &["list"]).ok();
}

/// I5: with no webhook and no sync, the poller builds a push within one poll
/// interval.
#[test]
fn i5_poller_builds_a_push() {
    let world = World::new();
    let origin = world.origin("acme", "i5");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[("RIPCLONE_POLL_INTERVAL_SECS", "1")]);
    world.cli(&server, &["add", "acme/i5", "--wait"]).ok();
    let completed = server.metric("ripclone_builds_completed_total");

    let c = origin.commit(&[("a.txt", "c\n")], "C");
    origin.publish();
    wait_for("the poller to build the push", || {
        (server.metric("ripclone_builds_completed_total") > completed).then_some(())
    });

    let queued = server.metric("ripclone_builds_queued_total");
    let target = world.target("i5");
    world
        .cli(&server, &["clone", "acme/i5", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), c);
    assert_eq!(
        server.metric("ripclone_builds_queued_total"),
        queued,
        "the clone should find C already built"
    );
}
