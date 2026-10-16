//! I. Integrations: provider auth headers, signed webhooks, and the CLI's
//! upstream verification.

use crate::harness::*;
use hmac::{KeyInit, Mac};

/// I3: for each provider kind, the server sends that provider's auth header
/// on every request to the origin, and the origin accepts nothing else.
#[test]
fn i3_provider_auth_header_reaches_origin() {
    // (id, kind, token, extra provider fields, header name, header value)
    let cases = [
        (
            "gh-http",
            "github",
            "gh-secret",
            serde_json::json!({}),
            "authorization",
            basic("x-access-token", "gh-secret"),
        ),
        (
            "gl",
            "gitlab",
            "gl-secret",
            serde_json::json!({}),
            "authorization",
            basic("oauth2", "gl-secret"),
        ),
        (
            "gt",
            "gitea",
            "gt-secret",
            serde_json::json!({}),
            "authorization",
            "token gt-secret".to_string(),
        ),
        (
            "gen",
            "generic",
            "gen-secret",
            serde_json::json!({
                "auth_template": "Bearer {token}",
                "auth_header_name": "X-Origin-Token",
            }),
            "x-origin-token",
            "Bearer gen-secret".to_string(),
        ),
    ];
    for (id, kind, token, extra, header, value) in cases {
        let world = World::new();
        let http = world.serve_origins_over_http();
        http.require_header(header, &value);
        let name = format!("i3-{kind}");
        let origin = world.origin("acme", &name);
        let tip = origin.commit(&[("a.txt", "a\n")], "A");
        origin.publish();

        let server_providers =
            providers_json(http.url(), &[(id, kind, Some(token), extra.clone())]);
        let server = world.server(&[("RIPCLONE_PROVIDERS", &server_providers)]);
        // The CLI must know the provider id, but holds no upstream token, so
        // only the server's credential can reach the origin.
        let cli_providers = providers_json(http.url(), &[(id, kind, None, extra)]);
        let target = world.target(&name);
        let repo = format!("{id}:acme/{name}");
        let cli_env = [("RIPCLONE_PROVIDERS", cli_providers.as_str())];
        world
            .cli_env(&server, &["add", &repo, "--wait"], &cli_env)
            .ok();
        world
            .cli_env(
                &server,
                &[
                    "clone",
                    &repo,
                    target.to_str().unwrap(),
                    "--depth",
                    "1",
                    "--verify-upstream",
                    "never",
                ],
                &cli_env,
            )
            .ok();

        assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip, "{kind}");
        assert_eq!(listing(&target), origin.tree(&tip), "{kind}");
        let requests = http.requests();
        assert!(http.fetches() > 0, "{kind}: the origin served no fetch");
        for request in &requests {
            assert_eq!(
                request.header(header),
                Some(value.as_str()),
                "{kind}: {} {} lacked the provider header",
                request.method,
                request.path
            );
        }
    }
}

/// I2: a private repo builds with the server's provider credential alone.
/// The CLI holds no upstream token and still adds, waits and clones with
/// default upstream verification; the origin refuses unauthenticated reads.
#[test]
fn i2_server_credential_builds_a_private_repo() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let header = basic("x-access-token", "i2-secret");
    http.require_header("authorization", &header);
    let origin = world.origin("acme", "i2");
    let tip = origin.commit(&[("a.txt", "a\n"), ("d/b.txt", "b\n")], "A");
    origin.publish();

    let anonymous = git_output(
        &world.path("work"),
        &["ls-remote", &format!("{}/acme/i2.git", http.url())],
    );
    assert!(
        !anonymous.status.success() && String::from_utf8_lossy(&anonymous.stderr).contains("403"),
        "the origin must refuse an unauthenticated read: {anonymous:?}"
    );

    let server_providers = providers_json(
        http.url(),
        &[("gh", "github", Some("i2-secret"), serde_json::json!({}))],
    );
    let server = world.server(&[("RIPCLONE_PROVIDERS", &server_providers)]);
    let cli_providers =
        providers_json(http.url(), &[("gh", "github", None, serde_json::json!({}))]);
    let cli_env = [("RIPCLONE_PROVIDERS", cli_providers.as_str())];
    world
        .cli_env(&server, &["add", "gh:acme/i2", "--wait"], &cli_env)
        .ok();
    let target = world.target("i2");
    world
        .cli_env(
            &server,
            &["clone", "gh:acme/i2", target.to_str().unwrap()],
            &cli_env,
        )
        .ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));
    assert!(http.fetches() > 0, "the origin served no fetch");
    // Only the server's credential ever reached the origin.
    for request in http.requests() {
        if let Some(sent) = request.header("authorization") {
            assert_eq!(sent, header, "{} {}", request.method, request.path);
        }
    }
}

fn hmac_hex(secret: &str, body: &str) -> String {
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// A signed push webhook for `kind`: (headers, body).
fn push_hook(
    kind: &str,
    secret: &str,
    repo: &str,
    after: &str,
) -> (Vec<(&'static str, String)>, String) {
    let (owner, name) = repo.split_once('/').unwrap();
    match kind {
        "github" => {
            let body = serde_json::json!({
                "ref": "refs/heads/main",
                "after": after,
                "repository": {
                    "name": name,
                    "owner": { "login": owner },
                    "default_branch": "main",
                    "private": false,
                },
            })
            .to_string();
            let sig = format!("sha256={}", hmac_hex(secret, &body));
            (
                vec![
                    ("X-GitHub-Event", "push".into()),
                    ("X-Hub-Signature-256", sig),
                ],
                body,
            )
        }
        "gitlab" => {
            let body = serde_json::json!({
                "ref": "refs/heads/main",
                "after": after,
                "project": {
                    "path_with_namespace": repo,
                    "default_branch": "main",
                    "visibility_level": 20,
                },
            })
            .to_string();
            (
                vec![
                    ("X-Gitlab-Event", "Push Hook".into()),
                    ("X-Gitlab-Token", secret.into()),
                ],
                body,
            )
        }
        "gitea" => {
            let body = serde_json::json!({
                "ref": "refs/heads/main",
                "after": after,
                "repository": {
                    "full_name": repo,
                    "default_branch": "main",
                    "private": false,
                },
            })
            .to_string();
            let sig = hmac_hex(secret, &body);
            (
                vec![("X-Gitea-Event", "push".into()), ("X-Gitea-Signature", sig)],
                body,
            )
        }
        other => panic!("no webhook format for {other}"),
    }
}

/// I4: for each provider's webhook, a bad signature gets 401 and builds
/// nothing; a signed push builds the new tip before any clone asks for it.
#[test]
fn i4_signed_webhook_builds_before_clone() {
    for (id, kind) in [
        ("gh-hook", "github"),
        ("gl-hook", "gitlab"),
        ("gt-hook", "gitea"),
    ] {
        let world = World::new();
        let http = world.serve_origins_over_http();
        let name = format!("i4-{kind}");
        let slug = format!("acme/{name}");
        let origin = world.origin("acme", &name);
        origin.commit(&[("a.txt", "a\n")], "A");
        origin.publish();

        let secret = format!("{kind}-hook-secret");
        let secret_var = format!(
            "RIPCLONE_WEBHOOK_SECRET_{}",
            id.to_uppercase().replace('-', "_")
        );
        let providers = providers_json(http.url(), &[(id, kind, None, serde_json::json!({}))]);
        let server = world.server(&[("RIPCLONE_PROVIDERS", &providers), (&secret_var, &secret)]);
        let cli_env = [("RIPCLONE_PROVIDERS", providers.as_str())];
        let repo = format!("{id}:{slug}");
        world
            .cli_env(&server, &["add", &repo, "--wait"], &cli_env)
            .ok();
        assert_eq!(server.metric("ripclone_builds_queued_total"), 1);

        let tip = origin.commit(&[("a.txt", "b\n")], "B");
        origin.publish();
        let hook = format!("/webhooks/{id}");

        let (headers, body) = push_hook(kind, "wrong-secret", &slug, &tip);
        let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let bad = server.post(&hook, &headers, &body);
        assert_eq!(bad.status, 401, "{kind}: {}", bad.body);
        assert_eq!(server.metric("ripclone_builds_queued_total"), 1, "{kind}");

        let (headers, body) = push_hook(kind, &secret, &slug, &tip);
        let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let good = server.post(&hook, &headers, &body);
        assert_eq!(good.status, 200, "{kind}: {}", good.body);
        assert_eq!(server.metric("ripclone_builds_queued_total"), 2, "{kind}");
        wait_for(&format!("{kind} webhook build"), || {
            (server.metric("ripclone_builds_completed_total") == 2).then_some(())
        });

        let target = world.target(&name);
        world
            .cli_env(
                &server,
                &["clone", &repo, target.to_str().unwrap()],
                &cli_env,
            )
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip, "{kind}");
        assert_eq!(listing(&target), origin.tree(&tip), "{kind}");
        assert_eq!(
            server.metric("ripclone_builds_queued_total"),
            2,
            "{kind}: the clone needed a build"
        );
    }
}

/// I7 setup: an origin the server has built, and its tip.
fn i7_world(name: &str) -> (World, Server, Origin, String) {
    let world = World::new();
    let origin = world.origin("acme", name);
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world
        .cli(&server, &["add", &format!("acme/{name}"), "--wait"])
        .ok();
    (world, server, origin, tip)
}

/// Clone with each `--verify-upstream` mode while the CLI sees the upstream at
/// `base` (empty: the real origin). Expected per mode: `Ok(warns)` or
/// `Err(text)`.
fn i7_cases(
    world: &World,
    server: &Server,
    name: &str,
    tip: &str,
    base: &str,
    cases: &[(&str, Result<bool, &str>)],
) {
    for (mode, expected) in cases {
        let case = format!("{name}-{mode}");
        let target = world.target(&case);
        let mut env = vec![];
        if !base.is_empty() {
            env.push(("RIPCLONE_ORIGIN_BASE", base));
        }
        let out = world.cli_env(
            server,
            &[
                "clone",
                &format!("acme/{name}"),
                target.to_str().unwrap(),
                "--depth",
                "1",
                &format!("--verify-upstream={mode}"),
            ],
            &env,
        );
        match expected {
            Ok(warns) => {
                out.ok();
                assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip, "{case}");
                assert_eq!(
                    out.stderr().contains("--verify-upstream skipped"),
                    *warns,
                    "{case}: {}",
                    out.stderr()
                );
            }
            Err(text) => {
                let err = out.err();
                assert!(err.contains(text), "{case}: {err}");
                assert!(!target.exists(), "{case} left {}", target.display());
            }
        }
    }
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());
}

/// I7: `--verify-upstream=always` passes when the CLI's upstream matches what
/// the server built.
#[test]
fn i7_verify_upstream_same_tip() {
    let (world, server, _origin, tip) = i7_world("i7-same");
    i7_cases(
        &world,
        &server,
        "i7-same",
        &tip,
        "",
        &[("always", Ok(false))],
    );
}

/// I7: the CLI's upstream is broken. `never` stays quiet, `auto` warns and
/// clones, `always` fails.
#[test]
fn i7_verify_upstream_broken() {
    let (world, server, _origin, tip) = i7_world("i7-broken");
    let broken = Hangup::new();
    i7_cases(
        &world,
        &server,
        "i7-broken",
        &tip,
        &broken.url(),
        &[
            ("never", Ok(false)),
            ("auto", Ok(true)),
            ("always", Err("upstream verification failed")),
        ],
    );
}

/// I7: the CLI's upstream has moved past what the server built. `never`
/// clones; `auto` and `always` refuse, and remove the clone they installed.
/// A target that was an empty directory is left as an empty directory.
#[test]
fn i7_verify_upstream_moved_tip() {
    let (world, server, origin, tip) = i7_world("i7-moved");
    // A copy of the origin whose main has a commit the server never saw.
    let moved = world.path("moved");
    let moved_bare = moved.join("acme").join("i7-moved.git");
    std::fs::create_dir_all(moved_bare.parent().unwrap()).unwrap();
    git(
        &moved,
        &[
            "clone",
            "--bare",
            "-q",
            origin.bare.to_str().unwrap(),
            moved_bare.to_str().unwrap(),
        ],
    );
    origin.commit(&[("a.txt", "moved\n")], "moved");
    git(
        &origin.work,
        &["push", "-q", moved_bare.to_str().unwrap(), "HEAD:main"],
    );
    i7_cases(
        &world,
        &server,
        "i7-moved",
        &tip,
        &format!("file://{}", moved.display()),
        &[
            ("never", Ok(false)),
            ("auto", Err("does not match upstream tip")),
            ("always", Err("does not match upstream tip")),
        ],
    );

    let empty = world.target("i7-moved-empty");
    std::fs::create_dir_all(&empty).unwrap();
    let base = format!("file://{}", moved.display());
    let out = world.cli_env(
        &server,
        &[
            "clone",
            "acme/i7-moved",
            empty.to_str().unwrap(),
            "--depth",
            "1",
            "--verify-upstream=always",
        ],
        &[("RIPCLONE_ORIGIN_BASE", &base)],
    );
    let err = out.err();
    assert!(err.contains("does not match upstream tip"), "{err}");
    let entries = std::fs::read_dir(&empty)
        .unwrap_or_else(|e| panic!("the empty target is gone: {e}"))
        .count();
    assert_eq!(entries, 0, "the failed clone left files in the target");
}

/// I10: a Files clone has no git objects to check against the upstream.
/// `never` and `auto` clone without contacting the upstream (here a host that
/// hangs up, which `auto` would warn about); `always` fails before any
/// download.
#[test]
fn i10_verify_upstream_in_files_mode() {
    let (world, server, origin, tip) = i7_world("i10");
    let proxy = FaultProxy::start(&server);
    let broken = Hangup::new();
    let env = [("RIPCLONE_ORIGIN_BASE", broken.url())];
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    for (mode, clones) in [("never", true), ("auto", true), ("always", false)] {
        let target = world.target(&format!("i10-{mode}"));
        let downloads = proxy.artifact_gets().len();
        let out = world.cli_env(
            &proxy,
            &[
                "clone",
                "acme/i10",
                target.to_str().unwrap(),
                "--mode",
                "files",
                &format!("--verify-upstream={mode}"),
            ],
            &env,
        );
        if clones {
            out.ok();
            assert!(
                !out.stderr().contains("verify-upstream"),
                "{mode}: {}",
                out.stderr()
            );
            assert_eq!(listing(&target), origin.tree(&tip), "{mode}");
        } else {
            let err = out.err();
            assert_eq!(out.code(), Some(2), "{mode}: {err}");
            assert!(
                err.contains("cannot verify a files-mode clone"),
                "{mode}: {err}"
            );
            assert!(!target.exists(), "failed clone left {}", target.display());
            assert_eq!(proxy.artifact_gets().len(), downloads, "{mode} downloaded");
            assert_eq!(world.staging_leftovers(), Vec::<String>::new());
        }
    }
}
