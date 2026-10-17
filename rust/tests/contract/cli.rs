//! K. The command line: repo arguments, usage errors, provider commands and
//! the login token, as a user types them.

use crate::harness::*;

/// K1: `owner/name`, `owner/name.git` and pasted clone URLs name the same
/// repo in every command. A malformed argument or a host with no provider
/// exits 2 and changes nothing.
#[test]
fn k1_repo_arguments_name_one_repo() {
    let world = World::new();
    let origin = world.origin("acme", "k1");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);

    world
        .cli(&server, &["add", "https://github.com/acme/k1", "--wait"])
        .ok();
    world
        .cli(&server, &["add", "git@github.com:acme/k1.git"])
        .ok();
    let out = world.cli(&server, &["sync", "acme/k1.git"]).ok();
    assert_eq!(out, format!("acme/k1 is current at {tip}\n"));
    assert_eq!(world.cli(&server, &["list"]).ok(), "acme/k1\n");
    let target = world.target("k1");
    let out = world
        .cli(
            &server,
            &[
                "clone",
                "ssh://git@github.com/acme/k1.git",
                target.to_str().unwrap(),
                "--depth",
                "1",
            ],
        )
        .ok();
    assert!(out.contains("installed acme/k1"), "{out}");
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);

    for (arg, message) in [
        ("acme", "expected owner/name or a clone URL, got 'acme'"),
        (
            "https://github.com/acme/k1/tree/main",
            "got 'acme/k1/tree/main'",
        ),
        ("https://github.com/", "expected a clone URL with a path"),
        (
            "https://git.example.com/acme/k1",
            "no provider is configured for git.example.com",
        ),
        ("forge:acme/k1", "unknown provider 'forge'"),
    ] {
        for command in ["add", "sync", "clone"] {
            let out = world.cli(&server, &[command, arg]);
            assert_eq!(out.code(), Some(2), "{command} {arg}: {}", out.stderr());
            assert!(
                out.err().contains(message),
                "{command} {arg}: {}",
                out.stderr()
            );
        }
    }
    assert_eq!(world.cli(&server, &["list"]).ok(), "acme/k1\n");

    world
        .cli(&server, &["rm", "https://github.com/acme/k1"])
        .ok();
    let again = world.cli(&server, &["rm", "acme/k1"]);
    assert_eq!(again.code(), Some(5), "{}", again.stderr());
    assert!(again.err().contains("repo not added"));
    assert_eq!(world.cli(&server, &["list"]).ok(), "");
}

/// K2: a bad clone or sync command line exits 2 with a message naming the
/// problem, before it asks the server for anything or creates the target.
#[test]
fn k2_bad_command_lines_exit_2_and_touch_nothing() {
    let world = World::new();
    let origin = world.origin("acme", "k2");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/k2", "--wait"]).ok();
    let queued = server.metric("ripclone_builds_queued_total");
    let target = world.target("k2");
    let dir = target.to_str().unwrap();

    for (args, message) in [
        (vec!["--depth", "3"], "not --depth 3"),
        (vec!["--at", "a b"], "invalid selector `a b`"),
        (vec!["--mode", "foo"], "invalid value 'foo'"),
        (vec!["--bogus"], "unexpected argument '--bogus'"),
        (
            vec!["--at", &tip, "--verify-upstream=always"],
            "cannot verify a non-tip rev",
        ),
        (
            vec!["--mode", "files", "--verify-upstream=always"],
            "cannot verify a files-mode clone",
        ),
        (
            vec!["--verify-upstream=maybe"],
            "expected 'auto', 'always', or 'never'",
        ),
    ] {
        let mut argv = vec!["clone", "acme/k2", dir];
        argv.extend(&args);
        let out = world.cli(&server, &argv);
        assert_eq!(out.code(), Some(2), "{args:?}: {}", out.stderr());
        assert!(out.err().contains(message), "{args:?}: {}", out.stderr());
        assert!(!target.exists(), "{args:?} created the target");
    }
    let out = world.cli(&server, &["sync", "acme/k2", "--at", "a b"]);
    assert_eq!(out.code(), Some(2), "{}", out.stderr());
    assert_eq!(server.metric("ripclone_builds_queued_total"), queued);

    // A `--at` clone under the default `auto` check prints no warning.
    let out = world.cli(&server, &["clone", "acme/k2", dir, "--at", &tip]);
    out.ok();
    assert_eq!(out.stderr(), "", "{}", out.stderr());
}

/// K3: the provider commands manage the CLI's own config. A user adds a
/// private Gitea host, checks it with `provider test`, fixes the token,
/// then adds and clones a repo by pasting its URL.
#[test]
fn k3_provider_commands_manage_the_cli_config() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    http.require_header("authorization", "token k3-secret");
    let origin = world.origin("acme", "k3");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server_providers = providers_json(
        http.url(),
        &[("forge", "gitea", Some("k3-secret"), serde_json::json!({}))],
    );
    let server = world.server(&[("RIPCLONE_PROVIDERS", &server_providers)]);
    let host = http.url();
    let url = format!("{host}/acme/k3.git");

    let list = world.cli(&server, &["provider", "list"]).ok();
    assert!(
        list.lines()
            .any(|l| l.split_whitespace().collect::<Vec<_>>()
                == ["github", "github", "github.com", "none"]),
        "{list}"
    );

    let out = world.cli(&server, &["provider", "add", "forge", "--host", host]);
    assert_eq!(out.code(), Some(2));
    assert!(out.err().contains("--kind is required"));
    let out = world.cli(
        &server,
        &[
            "provider", "add", "forge", "--kind", "gitea", "--host", host, "--token", "wrong",
        ],
    );
    assert_eq!(out.ok(), "added provider 'forge'\n");
    let out = world.cli(&server, &["provider", "test", "forge", "acme/k3"]);
    assert_eq!(out.code(), Some(1));
    assert!(
        out.err().contains(&format!("read {url}")),
        "{}",
        out.stderr()
    );

    // Updating keeps the kind and host.
    let out = world.cli(
        &server,
        &["provider", "add", "forge", "--token", "k3-secret"],
    );
    assert_eq!(out.ok(), "updated provider 'forge'\n");
    let out = world
        .cli(&server, &["provider", "test", "forge", "acme/k3"])
        .ok();
    assert_eq!(out, format!("{url} HEAD is {tip}\n"));
    let out = world.cli(
        &server,
        &["provider", "test", "forge", "acme/k3", "-b", "nope"],
    );
    assert!(out.err().contains("has no ref 'nope'"), "{}", out.stderr());
    let list = world.cli(&server, &["provider", "list"]).ok();
    let clean = host.trim_start_matches("http://");
    assert!(
        list.lines()
            .any(|l| l.split_whitespace().collect::<Vec<_>>()
                == ["forge", "gitea", clean, "configured"]),
        "{list}"
    );

    // The pasted URL picks the provider by host.
    world.cli(&server, &["add", &url, "--wait"]).ok();
    assert_eq!(world.cli(&server, &["list"]).ok(), "forge:acme/k3\n");
    let target = world.target("k3");
    world
        .cli(&server, &["clone", &url, target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(git(&target, &["config", "remote.origin.url"]), url);

    world.cli(&server, &["provider", "rm", "forge"]).ok();
    let out = world.cli(&server, &["provider", "rm", "forge"]);
    assert_eq!(out.code(), Some(1));
    assert!(out.err().contains("not in the CLI's config"));
    let out = world.cli(
        &server,
        &["clone", &url, world.target("k3b").to_str().unwrap()],
    );
    assert_eq!(out.code(), Some(2));
    assert!(
        out.err().contains("no provider is configured"),
        "{}",
        out.stderr()
    );
}

/// K4: `logout` with no saved token says so; `login` against a self-hosted
/// server points at the token variable instead of starting a device flow.
#[test]
fn k4_login_and_logout() {
    let world = World::new();
    let server = world.server(&[]);
    assert_eq!(world.cli(&server, &["logout"]).ok(), "Not logged in.\n");
    let out = world.cli(&server, &["login"]);
    assert_eq!(out.code(), Some(2));
    assert!(
        out.err().contains("set RIPCLONE_SERVER_TOKEN"),
        "{}",
        out.stderr()
    );
}
