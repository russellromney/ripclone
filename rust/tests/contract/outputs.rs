//! C. Outputs are independent and immutable.

use crate::harness::*;

/// The `/status` entry for `commit` once Head, Full and Files are all ready.
fn wait_all_ready(server: &Server, repo: &str, commit: &str) -> serde_json::Value {
    wait_for(&format!("every output of {commit} ready"), || {
        let resp = server.get(&format!("/v1/repos/github/{repo}/status"));
        let status: serde_json::Value = serde_json::from_str(&resp.body).ok()?;
        status["refs"]
            .as_array()?
            .iter()
            .find(|entry| {
                entry["commit"] == commit
                    && entry["head"] == true
                    && entry["full"] == true
                    && entry["files"] == true
            })
            .cloned()
    })
}

/// The state of each output of `commit`, from the API's build summary.
pub fn output_states(server: &Server, repo: &str, commit: &str) -> [String; 3] {
    let (status, body) = api::read_commit(server, repo, commit, None);
    assert_eq!(status, 200, "{body}");
    ["head", "full", "files"].map(|kind| body[kind]["state"].as_str().unwrap_or("").to_string())
}

/// C1: with no background slots, a depth-1 clone succeeds on Head alone while
/// Full and Files stay pending.
#[test]
fn c1_head_clones_while_background_outputs_pend() {
    let world = World::new();
    let origin = world.origin("acme", "c1");
    let tip = origin.commit(&[("a.txt", "a\n"), ("b/c.txt", "c\n")], "B");
    origin.publish();
    let server = world.server(&[("RIPCLONE_BACKGROUND_SLOTS", "0")]);
    world.cli(&server, &["add", "acme/c1"]).ok();

    let target = world.target("c1");
    world
        .cli(
            &server,
            &["clone", "acme/c1", target.to_str().unwrap(), "--depth", "1"],
        )
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));

    assert_eq!(
        output_states(&server, "acme/c1", &tip),
        ["ready", "pending", "pending"]
    );
    for kind in ["full", "files"] {
        assert_eq!(
            server.metric(&format!(
                "ripclone_outputs_published_total{{kind=\"{kind}\"}}"
            )),
            0,
            "{kind}"
        );
    }
}

/// C4: SIGKILL while the origin holds the build's fetch; after a restart and
/// release the build completes once and every output is ready.
#[test]
fn c4_build_killed_mid_fetch_completes_once_after_restart() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "c4");
    let tip = origin.commit(&[("a.txt", "a\n"), ("b/c.txt", "c\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    http.hold_fetches();
    world.cli(&server, &["add", "acme/c4"]).ok();
    http.wait_held(1);

    server.restart();
    // The killed server's fetch is still parked; the restarted server leases
    // the same build again and parks its own fetch.
    http.wait_held(2);
    http.release_fetches();

    let entry = wait_all_ready(&server, "acme/c4", &tip);
    assert_eq!(entry["job"], "done", "{entry}");
    wait_for("the build to complete", || {
        (server.metric("ripclone_builds_completed_total") == 1).then_some(())
    });
    // The restarted server admitted nothing new and published each output
    // once.
    assert_eq!(server.metric("ripclone_builds_queued_total"), 0);
    for kind in ["head", "full", "files"] {
        assert_eq!(
            server.metric(&format!(
                "ripclone_outputs_published_total{{kind=\"{kind}\"}}"
            )),
            1,
            "{kind}"
        );
    }
    assert_eq!(server.metric("ripclone_builds_failed_total"), 0);

    let target = world.target("c4");
    world
        .cli(&server, &["clone", "acme/c4", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// C6 (Linux): SIGKILL the server while the origin holds its fetch. The
/// server's git children die with it instead of running on against its
/// mirrors. macOS has no parent-death signal, so this row is Linux only.
#[cfg(target_os = "linux")]
#[test]
fn c6_server_git_children_die_with_the_server() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "c6");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    http.hold_fetches();
    world.cli(&server, &["add", "acme/c6"]).ok();
    http.wait_held(1);

    let children = proc_children(server.pid(), "git");
    assert!(!children.is_empty(), "no git child while the fetch is held");
    server.kill();
    wait_for("the killed server's git children to exit", || {
        children.iter().all(|p| !proc_alive(*p)).then_some(())
    });
}

/// A process, by pid and start time so a reused pid does not count.
#[cfg(target_os = "linux")]
type Proc = (u32, u64);

/// `(comm, state, ppid, start time)` from `/proc/<pid>/stat`.
#[cfg(target_os = "linux")]
fn proc_stat(pid: u32) -> Option<(String, String, u32, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // `pid (comm) state ppid ...`; comm may contain spaces and parens.
    let (head, rest) = stat.rsplit_once(") ")?;
    let comm = head.split_once(" (")?.1.to_string();
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // `rest` starts at field 3 (state); start time is field 22.
    Some((
        comm,
        fields.first()?.to_string(),
        fields.get(1)?.parse().ok()?,
        fields.get(19)?.parse().ok()?,
    ))
}

/// Processes whose parent is `parent` and whose command name is `name`.
#[cfg(target_os = "linux")]
fn proc_children(parent: u32, name: &str) -> Vec<Proc> {
    std::fs::read_dir("/proc")
        .unwrap()
        .flatten()
        .filter_map(|entry| {
            let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
            let (comm, _, ppid, start) = proc_stat(pid)?;
            (ppid == parent && comm == name).then_some((pid, start))
        })
        .collect()
}

/// Whether the process is still running: same start time, not a zombie.
#[cfg(target_os = "linux")]
fn proc_alive((pid, start): Proc) -> bool {
    proc_stat(pid)
        .is_some_and(|(_, state, _, started)| started == start && state != "Z" && state != "X")
}

/// A ref response with every URL's query string removed: signatures and
/// expiry change per request, the object keys must not.
fn without_queries(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) if s.contains("://") => {
            serde_json::Value::String(s.split('?').next().unwrap_or_default().to_string())
        }
        serde_json::Value::Array(items) => items.into_iter().map(without_queries).collect(),
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(k, v)| (k, without_queries(v)))
            .collect(),
        other => other,
    }
}

/// Every output of `commit` as the API reads it, without queries, plus the
/// bytes of each manifest.
fn published(server: &Server, commit: &str) -> Vec<(serde_json::Value, String)> {
    ["head", "full", "files"]
        .iter()
        .map(|kind| {
            let (status, body) = api::read_commit(server, "acme/c5", commit, Some(kind));
            assert_eq!(status, 200, "{kind}: {body}");
            assert_eq!(body["state"], "ready", "{kind}: {body}");
            let manifest = body["clonepack_manifest"].as_str().unwrap().to_string();
            let bytes = server.get(&format!("/v1/artifacts/{manifest}"));
            assert_eq!(bytes.status, 200, "{kind} manifest {manifest}");
            (without_queries(body), bytes.body)
        })
        .collect()
}

/// C5: after B is fully ready, building C never changes B's published
/// results, even when B's Full compacts a new Head base that C builds on.
#[test]
fn c5_later_build_never_changes_published_results() {
    let world = World::new();
    let origin = world.origin("acme", "c5");
    origin.commit(&[("a.txt", "a\n"), ("keep.txt", "keep\n")], "A");
    origin.publish();
    // Any Head delta compacts, so Full(B) records a new Head base.
    let server = world.server(&[("RIPCLONE_HEAD_REBASE_BYTES", "1")]);
    world.cli(&server, &["add", "acme/c5", "--wait"]).ok();

    let b = origin.commit(&[("a.txt", "b\n"), ("b.txt", "b\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/c5", "--wait"]).ok();
    wait_all_ready(&server, "acme/c5", &b);
    let before = published(&server, &b);
    assert!(
        server.log().contains("compacted Head packs"),
        "Full(B) did not compact a Head base:\n{}",
        server.log()
    );

    let c = origin.commit(&[("a.txt", "c\n"), ("c.txt", "c\n")], "C");
    origin.publish();
    world.cli(&server, &["sync", "acme/c5", "--wait"]).ok();
    wait_all_ready(&server, "acme/c5", &c);

    let base_line = format!("delta vs base {}", &b[..7]);
    assert!(
        server.log().contains(&base_line),
        "Head(C) did not build on Full(B)'s base:\n{}",
        server.log()
    );

    let after = published(&server, &b);
    for ((kind, before), after) in ["head", "full", "files"].iter().zip(&before).zip(&after) {
        assert_eq!(before.0, after.0, "{kind} response of B changed");
        assert!(before.1 == after.1, "{kind} manifest bytes of B changed");
    }

    // C's Head is a delta on the base Full(B) compacted; both clone right.
    for (commit, name) in [(&c, "c5-c"), (&b, "c5-b")] {
        let target = world.target(name);
        world
            .cli(
                &server,
                &[
                    "clone",
                    "acme/c5",
                    target.to_str().unwrap(),
                    "--depth",
                    "1",
                    "--at",
                    commit,
                ],
            )
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), *commit);
        git(&target, &["fsck"]);
        assert_eq!(listing(&target), origin.tree(commit));
    }
}
