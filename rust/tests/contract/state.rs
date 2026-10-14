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

/// Names in the server's state directory, sorted.
fn state_entries(world: &World) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(world.path("server"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// H4: each invalid storage or control setting stops the server before it
/// creates anything: no control database, cache, or mirror directory.
#[test]
fn h4_invalid_config_fails_before_side_effects() {
    type Env<'a> = &'a [(&'a str, &'a str)];
    // (case, config.toml, env, text the error must contain)
    let cases: &[(&str, &str, Env, &str)] = &[
        (
            "unknown backend",
            "[storage]\nbackend = \"gcs\"\n",
            &[],
            "unknown storage backend",
        ),
        (
            "s3 without endpoint",
            "[storage]\nbackend = \"s3\"\nbucket = \"b\"\n",
            &[],
            "RIPCLONE_S3_ENDPOINT",
        ),
        (
            "s3 without bucket",
            "",
            &[("RIPCLONE_S3_ENDPOINT", "http://127.0.0.1:9")],
            "RIPCLONE_S3_BUCKET",
        ),
        ("malformed file", "[storage\n", &[], "parse config"),
        (
            "removed [queue]",
            "[queue]\nurl = \"sqlite:///q.db\"\n",
            &[],
            "removed [queue]",
        ),
        (
            "empty control path",
            "[control]\npath = \"\"\n",
            &[],
            "control.path must not be empty",
        ),
        (
            "empty control env",
            "",
            &[("RIPCLONE_CONTROL_DB_PATH", " ")],
            "RIPCLONE_CONTROL_DB_PATH must not be empty",
        ),
        (
            "bad rate limit",
            "",
            &[("RIPCLONE_RATE_LIMIT_BURST", "many")],
            "RIPCLONE_RATE_LIMIT_BURST",
        ),
        (
            "no head slots",
            "",
            &[("RIPCLONE_HEAD_SLOTS", "0")],
            "RIPCLONE_HEAD_SLOTS",
        ),
    ];
    let world = World::new();
    let config = world.path("server").join("ripclone.toml");
    for (case, file, env, text) in cases {
        std::fs::write(&config, file).unwrap();
        let failure = world.server_must_fail(env);
        assert!(failure.contains(text), "{case}:\n{failure}");
        assert_eq!(state_entries(&world), ["ripclone.toml"], "{case}");
    }
}

/// H5: on local disk the artifact store is the durable copy, so retention
/// never trims it. With retention set to run every second and keep nothing
/// older than a day, the server reports retention off, and artifacts last
/// written a month ago still clone.
#[test]
fn h5_local_retention_never_trims_artifacts() {
    let world = World::new();
    let origin = world.origin("acme", "h5");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[
        ("RIPCLONE_RETENTION_INTERVAL_SECS", "1"),
        ("RIPCLONE_RETENTION_MAX_AGE_DAYS", "1"),
        ("RIPCLONE_RETENTION_MAX_GB", "1"),
    ]);
    world.cli(&server, &["add", "acme/h5", "--wait"]).ok();
    let stored = server.storage_listing();
    backdate(&world.path("server").join("cache"), 30);

    assert!(
        server.log().contains("local cache retention disabled"),
        "{}",
        server.log()
    );
    let target = world.target("h5");
    world
        .cli(&server, &["clone", "acme/h5", target.to_str().unwrap()])
        .ok();
    assert_eq!(listing(&target), origin.tree(&tip));
    assert_eq!(server.storage_listing(), stored);
    assert_eq!(server.metric("ripclone_local_cache_cleanup_runs_total"), 0);
}

/// Set the mtime of every file under `dir` to `days` ago.
pub fn backdate(dir: &std::path::Path, days: u64) {
    let when = std::time::SystemTime::now() - std::time::Duration::from_secs(days * 24 * 3600);
    let when = filetime::FileTime::from_system_time(when);
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            backdate(&path, days);
        } else {
            filetime::set_file_mtime(&path, when).unwrap();
        }
    }
}

/// H7: a repository's build config survives a server restart.
#[test]
fn h7_repo_config_survives_restart() {
    let world = World::new();
    let mut server = world.server(&[]);
    let path = "/v1/admin/config/github/acme/h7";
    let config = serde_json::json!({
        "clonepack_depths": [
            { "name": "shallow", "depth": 1 },
            { "name": "full", "depth": null }
        ],
        "compression_level": 9,
        "archive_chunk_size": 8_388_608
    });
    let (status, stored) = api::parse(server.post_json(path, &config));
    assert_eq!(status, 200, "{stored}");

    server.restart();
    let (status, read) = api::parse(server.get(path));
    assert_eq!(status, 200, "{read}");
    assert_eq!(read, stored);
}
