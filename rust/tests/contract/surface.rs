//! I. Auth and integrations.

use crate::harness::*;
use reqwest::Method;

type Env<'a> = Vec<(&'a str, &'a str)>;
type Unset<'a> = &'a [&'a str];

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
/// interval, and a tag pushed with no commit reaches clones of the same
/// commit.
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

    let before = api::tags_artifact(&server, "acme/i5", &c);
    git(&origin.work, &["tag", "-a", "-m", "v1", "v1"]);
    git(
        &origin.work,
        &["push", "-q", origin.bare.to_str().unwrap(), "v1"],
    );
    wait_for("the poller to publish the tag", || {
        (api::tags_artifact(&server, "acme/i5", &c) != before).then_some(())
    });
    let tagged = world.target("i5-tagged");
    world
        .cli(&server, &["clone", "acme/i5", tagged.to_str().unwrap()])
        .ok();
    let (refs, all, tags) = git_clone_tags(&origin.bare, &[]);
    assert!(refs.starts_with("v1 tag "), "{refs}");
    assert_eq!(tag_refs(&tagged), refs);
    assert_eq!(describe(&tagged), (all, tags));
    assert_eq!(
        server.metric("ripclone_builds_queued_total"),
        queued,
        "a tag admits no build"
    );
}

/// I15: an upstream whose `HEAD` names no branch, as a detached `HEAD`
/// leaves it. The poller still refreshes on a pushed branch, and keeps the
/// default branch the server last saw; it does not refresh again while
/// nothing changes.
#[test]
fn i15_poller_with_no_upstream_default_branch_refreshes_once() {
    let world = World::new();
    let origin = world.origin("acme", "i15");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let wrapper = GitWrapper::new(
        &world,
        "server",
        r#"case " $* " in
            *" ls-remote "*) note listing ;;
            *" fetch --atomic --prune "*) note refresh ;;
        esac"#,
    );
    let server = world.server(&[
        ("RIPCLONE_POLL_INTERVAL_SECS", "1"),
        ("PATH", &wrapper.path),
    ]);
    world.cli(&server, &["add", "acme/i15", "--wait"]).ok();
    wait_first_refs(&server, "acme/i15", &a);

    git(&origin.bare, &["update-ref", "--no-deref", "HEAD", &a]);
    let listed = git(&origin.bare, &["ls-remote", "--symref", ".", "HEAD"]);
    assert!(!listed.contains("ref:"), "{listed}");
    let before = api::branches_artifact(&server, "acme/i15", &a);
    git(
        &origin.work,
        &[
            "push",
            "-q",
            origin.bare.to_str().unwrap(),
            &format!("{a}:refs/heads/side"),
        ],
    );
    wait_for("the poller to publish the branch", || {
        (api::branches_artifact(&server, "acme/i15", &a) != before).then_some(())
    });
    let full = world.target("full");
    world
        .cli_env(
            &server,
            &["clone", "acme/i15", full.to_str().unwrap()],
            &[NO_UPSTREAM],
        )
        .ok();
    assert_eq!(
        remote_refs(&full),
        format!("origin/HEAD {a} origin/main\norigin/main {a} \norigin/side {a}")
    );

    let refreshes = wrapper.notes("refresh");
    let listings = wrapper.notes("listing");
    wait_for("four more listings", || {
        (wrapper.notes("listing") >= listings + 4).then_some(())
    });
    assert_eq!(
        wrapper.notes("refresh"),
        refreshes,
        "the poller refreshed again"
    );
}

/// I8: only the probe routes answer without the server token: health,
/// readiness, metrics and version. Webhooks check their own signature and
/// fail closed without a secret. Every other route, the artifact route
/// included, answers 401 to a missing or wrong token.
#[test]
fn i8_only_probe_routes_are_public() {
    let world = World::new();
    let origin = world.origin("acme", "i8");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/i8", "--wait"]).ok();
    let artifact = server
        .storage_listing()
        .into_iter()
        .filter_map(|path| path.rsplit('/').next().map(str::to_string))
        .find(|name| name.len() == 64)
        .expect("the build stored no artifact");

    for path in ["/healthz", "/readyz", "/metrics", "/v1/version"] {
        let status = server.request(Method::GET, path, None).status;
        assert_eq!(status, 200, "GET {path} without a token");
    }
    let webhook = server.post("/webhooks/github", &[], "{}");
    assert_eq!(webhook.status, 503, "unsigned webhook: {}", webhook.body);

    // One entry per route in `build_app` behind the auth layer.
    let repo = "/v1/repos/github/acme/i8";
    let protected = [
        (Method::GET, "/v1/repos".to_string()),
        (Method::GET, format!("{repo}/status")),
        (Method::GET, format!("{repo}/commits/{tip}")),
        (Method::POST, format!("{repo}/builds")),
        (Method::POST, format!("{repo}/add")),
        (Method::DELETE, format!("{repo}/add")),
        (Method::GET, format!("/v1/artifacts/{artifact}")),
        (Method::POST, "/v1/clones/some-clone/metrics".to_string()),
    ];
    for (method, path) in &protected {
        for token in [None, Some("wrong-token")] {
            let status = server.request(method.clone(), path, token).status;
            assert_eq!(status, 401, "{method} {path} with token {token:?}");
        }
    }

    let with_token = server.get(&format!("/v1/artifacts/{artifact}"));
    assert_eq!(with_token.status, 200, "{}", with_token.body);
    assert_eq!(server.metric("ripclone_builds_queued_total"), 1);
}

/// I9: a client that declares another wire protocol gets 426 and an error
/// that names the mismatch and says what to do; nothing is admitted or
/// installed.
#[test]
fn i9_protocol_mismatch_fails_clearly() {
    let world = World::new();
    let origin = world.origin("acme", "i9");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/i9", "--wait"]).ok();
    let queued = server.metric("ripclone_builds_queued_total");
    // The proxy rewrites the protocol the real CLI declares, as an older
    // CLI would send it.
    let proxy = FaultProxy::start(&server);
    proxy.set_request_header("x-ripclone-protocol", "2");

    let target = world.target("i9");
    let clone = world.cli(&proxy, &["clone", "acme/i9", target.to_str().unwrap()]);
    let sync = world.cli(&proxy, &["sync", "acme/i9"]);
    for (verb, out) in [("clone", &clone), ("sync", &sync)] {
        let err = out.err();
        assert_eq!(out.code(), Some(1), "{verb}: {err}");
        assert!(
            err.contains("client protocol 2 does not match this server's")
                && err.contains("ripclone version"),
            "{verb}: {err}"
        );
    }
    let seen = proxy.seen();
    assert!(
        !seen.is_empty() && seen.iter().all(|s| s.status == 426),
        "{seen:#?}"
    );
    assert!(!target.exists(), "failed clone left {}", target.display());
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());
    assert_eq!(server.metric("ripclone_builds_queued_total"), queued);
}

/// I11: the CLI reads `config.toml` from `~/.config/ripclone/config.toml` or
/// from `RIPCLONE_CONFIG`, and `RIPCLONE_SERVER` beats the file.
#[test]
fn i11_config_file_discovery_and_precedence() {
    let world = World::new();
    let server = world.server(&[]);

    // Which server `list` talks to. Three proxies in front of the one server
    // tell the sources apart.
    let proxies: Vec<FaultProxy> = (0..3).map(|_| FaultProxy::start(&server)).collect();
    let home = world.path("home");
    let default_file = home.join(".config/ripclone/config.toml");
    std::fs::create_dir_all(default_file.parent().unwrap()).unwrap();
    std::fs::write(
        &default_file,
        format!("server = \"{}\"\n", proxies[0].url()),
    )
    .unwrap();
    let explicit = home.join("explicit.toml");
    std::fs::write(&explicit, format!("server = \"{}\"\n", proxies[1].url())).unwrap();
    let explicit = explicit.to_str().unwrap();
    let env_server = proxies[2].url();
    // (case, env, unset env, the proxy that must see the request)
    let cases: [(&str, Env, Unset, usize); 3] = [
        (
            "default file",
            vec![],
            &["RIPCLONE_CONFIG", "RIPCLONE_SERVER"],
            0,
        ),
        (
            "RIPCLONE_CONFIG",
            vec![("RIPCLONE_CONFIG", explicit)],
            &["RIPCLONE_SERVER"],
            1,
        ),
        (
            "env beats file",
            vec![("RIPCLONE_SERVER", env_server.as_str())],
            &["RIPCLONE_CONFIG"],
            2,
        ),
    ];
    for (case, env, unset, want) in cases {
        let before: Vec<usize> = proxies.iter().map(|p| p.seen().len()).collect();
        world.cli_unset(&server, &["list"], &env, unset).ok();
        let used: Vec<usize> = proxies
            .iter()
            .enumerate()
            .filter(|(i, p)| p.seen().len() > before[*i])
            .map(|(i, _)| i)
            .collect();
        assert_eq!(used, [want], "{case}");
    }
}
