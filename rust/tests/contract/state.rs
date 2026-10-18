//! H. Storage and server state.

use crate::harness::*;
use std::collections::BTreeSet;

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

/// H4: each invalid storage, credential or numeric setting stops the server
/// before it creates anything: no control database, cache, or mirror
/// directory. A number that does not parse is an error naming the variable,
/// never a silent default.
#[test]
fn h4_invalid_config_fails_before_side_effects() {
    type Env<'a> = &'a [(&'a str, &'a str)];
    // (case, config.toml, env, text the error must contain)
    let cases: &[(&str, &str, Env, &str)] = &[
        (
            "s3 without endpoint",
            "",
            &[("RIPCLONE_S3_BUCKET", "b")],
            "RIPCLONE_S3_ENDPOINT",
        ),
        ("malformed file", "[providers\n", &[], "parse config"),
        (
            "token in the file",
            "[providers.forge]\nkind = \"gitea\"\nhost = \"h\"\ntoken = \"t\"\n",
            &[],
            "unknown field `token`",
        ),
        (
            "app key without app id",
            "",
            &[("RIPCLONE_APP_KEY_GITHUB", "k")],
            "RIPCLONE_APP_KEY_GITHUB is set but provider 'github' has no app_id",
        ),
        (
            "no head slots",
            "",
            &[("RIPCLONE_HEAD_SLOTS", "0")],
            "RIPCLONE_HEAD_SLOTS",
        ),
        (
            "bad build retry base",
            "",
            &[("RIPCLONE_BUILD_RETRY_BASE_MS", "2s")],
            "RIPCLONE_BUILD_RETRY_BASE_MS must be a non-negative integer",
        ),
        (
            "bad poll interval",
            "",
            &[("RIPCLONE_POLL_INTERVAL_SECS", "5m")],
            "RIPCLONE_POLL_INTERVAL_SECS must be a non-negative integer",
        ),
        (
            "bad retention interval",
            "",
            &[("RIPCLONE_RETENTION_INTERVAL_SECS", "soon")],
            "RIPCLONE_RETENTION_INTERVAL_SECS must be a non-negative integer",
        ),
        (
            "bad retention size",
            "",
            &[("RIPCLONE_RETENTION_MAX_GB", "1.5")],
            "RIPCLONE_RETENTION_MAX_GB must be a non-negative integer",
        ),
        (
            "retention size overflows",
            "",
            &[("RIPCLONE_RETENTION_MAX_GB", "18446744073709551615")],
            "RIPCLONE_RETENTION_MAX_GB is too large",
        ),
        (
            "bad signed URL TTL",
            "",
            &[("RIPCLONE_SIGNED_URL_TTL_SECS", "-1")],
            "RIPCLONE_SIGNED_URL_TTL_SECS must be a non-negative integer",
        ),
        (
            "bad ls-remote timeout",
            "",
            &[("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", "30s")],
            "RIPCLONE_LS_REMOTE_TIMEOUT_SECS must be a non-negative integer",
        ),
        (
            "bad fetch timeout",
            "",
            &[("RIPCLONE_FETCH_TIMEOUT_SECS", "1h")],
            "RIPCLONE_FETCH_TIMEOUT_SECS must be a non-negative integer",
        ),
        (
            "bad LSM levels",
            "",
            &[("RIPCLONE_LSM_MAX_LEVELS", "")],
            "RIPCLONE_LSM_MAX_LEVELS must be a non-negative integer",
        ),
        (
            "bad head rebase size",
            "",
            &[("RIPCLONE_HEAD_REBASE_BYTES", "128M")],
            "RIPCLONE_HEAD_REBASE_BYTES must be a non-negative integer",
        ),
    ];
    let world = World::new();
    let config = world.path("server").join("config.toml");
    for (case, file, env, text) in cases {
        std::fs::write(&config, file).unwrap();
        let failure = world.server_must_fail(env);
        assert!(failure.contains(text), "{case}:\n{failure}");
        assert_eq!(state_entries(&world), ["config.toml"], "{case}");
    }
}

/// H5: on local disk the artifact store is the durable copy, so retention
/// never trims it. With retention set to run every second and keep at most
/// 1 GB, the server reports retention off, and artifacts last written a
/// month ago, past the week retention keeps, still clone.
#[test]
fn h5_local_retention_never_trims_artifacts() {
    let world = World::new();
    let origin = world.origin("acme", "h5");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    // Retention runs every second, the harness default.
    let server = world.server(&[("RIPCLONE_RETENTION_MAX_GB", "1")]);
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

/// H8: `/readyz` fails once the control database file is deleted or replaced
/// under a running server, and the log says which.
#[test]
fn h8_readyz_fails_when_the_control_db_file_goes() {
    for (case, change) in [
        (
            "deleted",
            (|db| std::fs::remove_file(db).unwrap()) as fn(&std::path::Path),
        ),
        ("replaced", |db| {
            let fresh = db.with_extension("new");
            std::fs::write(&fresh, b"").unwrap();
            std::fs::rename(&fresh, db).unwrap();
        }),
    ] {
        let world = World::new();
        let server = world.server(&[]);
        change(&world.path("server").join("control.db"));
        let readyz = server.get("/readyz");
        assert_eq!(readyz.status, 503, "{case}: {}", readyz.body);
        let line = format!("was {case} after the server opened it");
        assert!(server.log().contains(&line), "{case}:\n{}", server.log());
    }
}

/// H9: build scratch lives under the cache directory and is emptied at
/// startup, so a killed server leaks nothing into the system temp directory.
/// The server's `TMPDIR` is read-only: a build that staged there would fail.
#[test]
fn h9_build_scratch_is_under_the_cache_and_cleared_at_start() {
    use std::os::unix::fs::PermissionsExt;
    let world = World::new();
    let origin = world.origin("acme", "h9");
    let tip = origin.commit(&[("a.txt", "a\n"), ("b/c.txt", "c\n")], "A");
    origin.publish();
    let scratch = world.path("server").join("cache").join("tmp");
    std::fs::create_dir_all(scratch.join(".tmpkilled")).unwrap();
    std::fs::write(scratch.join(".tmpkilled").join("pack-x.pack"), b"x").unwrap();
    let system_tmp = world.path("system-tmp");
    std::fs::create_dir_all(&system_tmp).unwrap();
    std::fs::set_permissions(&system_tmp, std::fs::Permissions::from_mode(0o555)).unwrap();

    let server = world.server(&[("TMPDIR", system_tmp.to_str().unwrap())]);
    assert!(!scratch.join(".tmpkilled").exists(), "stale scratch kept");
    world.cli(&server, &["add", "acme/h9", "--wait"]).ok();
    let target = world.target("h9");
    world
        .cli(&server, &["clone", "acme/h9", target.to_str().unwrap()])
        .ok();
    assert_eq!(listing(&target), origin.tree(&tip));
    assert_eq!(std::fs::read_dir(&system_tmp).unwrap().count(), 0);
}

/// H11: a scratch leftover the server cannot remove, as when a killed
/// server's git child is still writing into it, does not stop the next start.
/// The server logs it, starts, and builds in a scratch directory of its own;
/// once the leftover can go, the next start removes it.
#[test]
fn h11_a_scratch_leftover_in_use_does_not_stop_startup() {
    use std::os::unix::fs::PermissionsExt;
    let world = World::new();
    let origin = world.origin("acme", "h11");
    let tip = origin.commit(&[("a.txt", "a\n"), ("b/c.txt", "c\n")], "A");
    origin.publish();
    let scratch = world.path("server").join("cache").join("tmp");
    let stuck = scratch.join(".tmpkilled");
    std::fs::create_dir_all(stuck.join("pack")).unwrap();
    std::fs::write(stuck.join("pack").join("pack-x.pack"), b"x").unwrap();
    let mode = |mode| {
        std::fs::set_permissions(stuck.join("pack"), std::fs::Permissions::from_mode(mode)).unwrap()
    };
    mode(0o555);

    let mut server = world.server(&[]);
    assert!(
        server.log().contains("left stale scratch"),
        "{}",
        server.log()
    );
    assert!(stuck.join("pack").join("pack-x.pack").exists());
    world.cli(&server, &["add", "acme/h11", "--wait"]).ok();
    let target = world.target("h11");
    world
        .cli(&server, &["clone", "acme/h11", target.to_str().unwrap()])
        .ok();
    assert_eq!(listing(&target), origin.tree(&tip));

    mode(0o755);
    server.restart();
    assert!(!stuck.exists(), "leftover kept after it could go");
    let runs: Vec<_> = std::fs::read_dir(&scratch).unwrap().flatten().collect();
    assert_eq!(runs.len(), 1, "{runs:?}");
}

/// Every artifact hash (64 hex digits) in a JSON value.
fn hashes_in(value: &serde_json::Value, out: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(s)
            if s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) =>
        {
            out.insert(s.clone());
        }
        serde_json::Value::Array(items) => items.iter().for_each(|v| hashes_in(v, out)),
        serde_json::Value::Object(map) => map.values().for_each(|v| hashes_in(v, out)),
        _ => {}
    }
}

/// The artifacts every output of `commit` names.
pub(crate) fn commit_artifacts(server: &Server, repo: &str, commit: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for kind in ["head", "full", "files"] {
        let (status, body) = api::read_commit(server, repo, commit, Some(kind));
        assert_eq!(status, 200, "{repo}@{commit} {kind}: {body}");
        hashes_in(&body, &mut out);
    }
    assert!(!out.is_empty(), "{repo}@{commit} names no artifacts");
    out
}

fn stored(server: &Server, hash: &str) -> bool {
    match server.get_bytes(&format!("/v1/artifacts/{hash}")).0 {
        200 => true,
        404 => false,
        status => panic!("artifact {hash}: {status}"),
    }
}

/// The mirror directory names under the server's repo root.
pub(crate) fn mirrors(world: &World) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(world.path("server").join("repos"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".git"))
        .collect();
    names.sort();
    names
}

/// H13: purging a repository deletes its builds, its mirror, and the
/// artifacts no other repository uses, after twice the signed URL lifetime.
/// A fork that built the same commit keeps the shared artifacts and clones
/// in every shape. A clone of the purged repository that started before the
/// purge finishes. Added again, the repository builds and clones.
#[test]
fn h13_purge_removes_only_what_no_other_repo_uses() {
    let world = World::new();
    let origin = world.origin("acme", "h13");
    let a = origin.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "A");
    origin.publish();
    let fork = world.origin("acme", "h13-fork");
    git(
        &origin.work,
        &["push", "-q", fork.bare.to_str().unwrap(), "main"],
    );
    // Grace of 6 s; the harness sweeps every second.
    let server = world.server(&[("RIPCLONE_SIGNED_URL_TTL_SECS", "3")]);
    world.cli(&server, &["add", "acme/h13-fork", "--wait"]).ok();
    world.cli(&server, &["add", "acme/h13", "--wait"]).ok();
    let b = origin.commit(&[("c.txt", "c\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/h13", "--at", &b]).ok();

    let forked = commit_artifacts(&server, "acme/h13-fork", &a);
    let mut mine = commit_artifacts(&server, "acme/h13", &a);
    assert_eq!(mine, forked, "the same commit builds the same artifacts");
    mine.extend(commit_artifacts(&server, "acme/h13", &b));
    let unique: BTreeSet<&String> = mine.difference(&forked).collect();
    assert!(!unique.is_empty(), "B builds artifacts the fork lacks");
    assert_eq!(mirrors(&world).len(), 2);

    // Every download of a clone of B waits until the purge has happened.
    let proxy = FaultProxy::start(&server);
    proxy.fault_artifacts(Fault::Hold, 0, usize::MAX);
    let target = world.target("h13-during");
    let during = world.cli_spawn(&proxy, &["clone", "acme/h13", target.to_str().unwrap()]);
    proxy.wait_held(1);
    world.cli(&server, &["rm", "--purge", "acme/h13"]).ok();
    let (status, _) = api::read_commit(&server, "acme/h13", &b, Some("head"));
    assert_eq!(status, 404, "the purged builds are gone");
    let listed = world.cli(&server, &["list"]).ok();
    assert!(!listed.lines().any(|line| line == "acme/h13"), "{listed}");
    // Sweeps run while the clone is held; nothing is due yet.
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert!(unique.iter().all(|hash| stored(&server, hash)));
    proxy.release();
    during.wait().ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&target), origin.tree(&b));

    wait_for("the purged repository's artifacts to be deleted", || {
        unique
            .iter()
            .all(|hash| !stored(&server, hash))
            .then_some(())
    });
    assert!(forked.iter().all(|hash| stored(&server, hash)));
    assert_eq!(mirrors(&world), ["github_acme%2Fh13-fork.git"]);
    for (name, args) in [
        ("depth1", &["--depth", "1"][..]),
        ("full", &[][..]),
        ("files", &["--mode", "files"][..]),
    ] {
        let target = world.target(&format!("h13-fork-{name}"));
        let mut argv = vec!["clone", "acme/h13-fork", target.to_str().unwrap()];
        argv.extend_from_slice(args);
        world.cli(&server, &argv).ok();
        assert_eq!(listing(&target), origin.tree(&a), "{name}");
    }
    let full = world.target("h13-fork-full");
    git(&full, &["fsck", "--full"]);

    // The purge is idempotent, and the repository can come back.
    world.cli(&server, &["rm", "--purge", "acme/h13"]).ok();
    world.cli(&server, &["add", "acme/h13", "--wait"]).ok();
    let again = world.target("h13-again");
    world
        .cli(&server, &["clone", "acme/h13", again.to_str().unwrap()])
        .ok();
    assert_eq!(git(&again, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&again), origin.tree(&b));
    assert_eq!(
        git(&again, &["rev-list", "--count", "HEAD"]),
        origin.count(&b)
    );
}
