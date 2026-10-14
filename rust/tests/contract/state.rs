//! H. Storage and server state.

use crate::harness::*;

/// H1: published artifacts survive a server crash; the restarted server serves
/// them without rebuilding.
#[test]
fn h1_published_artifacts_survive_restart() {
    let world = World::new();
    let origin = world.origin("acme", "h1");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/h1", "--wait"]).ok();

    server.restart();
    let target = world.target("h1");
    world
        .cli(&server, &["clone", "acme/h1", target.to_str().unwrap()])
        .ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));
    assert_eq!(server.metric("ripclone_builds_queued_total"), 0);
}

/// H3: a second server on the same control database exits before it binds.
#[test]
fn h3_second_server_on_same_control_db_is_rejected() {
    let world = World::new();
    let _first = world.server(&[]);

    let failure = world.server_must_fail(&[]);
    assert!(
        failure.to_lowercase().contains("control"),
        "rejection should name the control database:\n{failure}"
    );
}
