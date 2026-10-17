//! E. Top-up: while Full(B) is pending, a full clone installs the newest
//! ready Full and fetches the rest from the upstream with the user's git.

use crate::harness::*;

/// A private repo on an origin that accepts two credentials: the server's
/// provider token and the user's own git credential. Full(A) is ready; then
/// the server restarts with no Files or Full slots and the origin parks every
/// fetch except the user's, so no build of a later commit can finish.
struct TopUp {
    world: World,
    http: GitHttp,
    origin: Origin,
    server: Server,
    repo: String,
    /// Where the CLI's provider config says the repo lives.
    upstream: String,
    a: String,
    cli_providers: String,
    user_auth: String,
}

impl TopUp {
    fn new(name: &str) -> Self {
        let world = World::new();
        let http = world.serve_origins_over_http();
        let server_auth = basic("x-access-token", "server-secret");
        let user_auth = basic("alice", "user-secret");
        http.require_header("authorization", &server_auth);
        http.require_header("authorization", &user_auth);
        let origin = world.origin("acme", name);
        let a = origin.commit(&[("a.txt", "a\n"), ("d/b.txt", "b\n")], "A");
        origin.publish();

        let server_providers = providers_json(
            http.url(),
            &[("gh", "github", Some("server-secret"), serde_json::json!({}))],
        );
        let mut server = world.server(&[("RIPCLONE_PROVIDERS", &server_providers)]);
        let cli_providers =
            providers_json(http.url(), &[("gh", "github", None, serde_json::json!({}))]);
        let repo = format!("gh:acme/{name}");
        let upstream = format!("{}/acme/{name}.git", http.url());
        world
            .cli_env(
                &server,
                &["add", &repo, "--wait"],
                &[("RIPCLONE_PROVIDERS", &cli_providers)],
            )
            .ok();
        server.restart_with(&[("RIPCLONE_FILES_SLOTS", "0"), ("RIPCLONE_FULL_SLOTS", "0")]);
        http.hold_fetches_without("authorization", &user_auth);
        Self {
            world,
            http,
            origin,
            server,
            repo,
            upstream,
            a,
            cli_providers,
            user_auth,
        }
    }

    /// Full-clone the branch, with the user's credential in their git config,
    /// and check the clone equals the origin at `b`.
    fn clone_equals_origin(&self, b: &str) {
        let target = self.world.target("clone");
        let header = format!("Authorization: {}", self.user_auth);
        let env = [
            ("RIPCLONE_PROVIDERS", self.cli_providers.as_str()),
            ("GIT_CONFIG_COUNT", "1"),
            ("GIT_CONFIG_KEY_0", "http.extraHeader"),
            ("GIT_CONFIG_VALUE_0", header.as_str()),
        ];
        self.world
            .cli_env(
                &self.server,
                &["clone", &self.repo, target.to_str().unwrap()],
                &env,
            )
            .ok();

        assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
        assert_eq!(git(&target, &["symbolic-ref", "HEAD"]), "refs/heads/main");
        // The branch tracks the upstream at the topped-up commit.
        assert_eq!(git(&target, &["rev-parse", "@{upstream}"]), b);
        assert_eq!(
            git(&target, &["config", "remote.origin.url"]),
            self.upstream
        );
        assert_eq!(listing(&target), self.origin.tree(b));
        // The top-up's fetch and checkout leave nothing `git clone` would
        // not: the reflog starts at B, and there is no ORIG_HEAD.
        assert_eq!(
            git(&target, &["reflog", "--format=%H %gs"]),
            format!("{b} clone: from {}", self.upstream)
        );
        assert!(!target.join(".git").join("ORIG_HEAD").exists());
        assert_eq!(
            git(&target, &["rev-list", "--count", "HEAD"]),
            self.origin.count(b)
        );
        let fsck = git_output(&target, &["fsck", "--no-dangling", "--no-progress"]);
        assert!(
            fsck.status.success() && fsck.stdout.is_empty() && fsck.stderr.is_empty(),
            "git fsck: {fsck:?}"
        );
        assert!(self.world.staging_leftovers().is_empty());

        // The server tried to build B and is still parked: the clone did not
        // wait on it.
        self.http.wait_held(1);
        let requests = self.http.requests();
        assert!(
            requests
                .iter()
                .any(|r| r.header("authorization") == Some(self.user_auth.as_str())),
            "the user's git never reached the origin"
        );
        for request in &requests {
            for (name, value) in &request.headers {
                assert!(
                    !value.contains(TOKEN),
                    "the ripclone token reached the origin in {name}: {} {}",
                    request.method,
                    request.path
                );
            }
        }
    }
}

/// E1: one new commit on top of the ready Full(A).
#[test]
fn e1_top_up_installs_b_without_a_server_build() {
    let t = TopUp::new("e1");
    let b = t.origin.commit(&[("a.txt", "a2\n"), ("c.txt", "c\n")], "B");
    t.origin.publish();

    t.clone_equals_origin(&b);
}

/// E2: several commits and a merge; A is an ancestor of B, not its parent.
#[test]
fn e2_top_up_across_a_merge() {
    let t = TopUp::new("e2");
    let work = &t.origin.work;
    git(work, &["checkout", "-q", "-b", "side"]);
    t.origin.commit(&[("side/1.txt", "1\n")], "S1");
    t.origin.commit(&[("side/2.txt", "2\n")], "S2");
    git(work, &["checkout", "-q", "main"]);
    t.origin.commit(&[("a.txt", "a2\n")], "M1");
    git(work, &["merge", "-q", "--no-ff", "-m", "B", "side"]);
    let b = t.origin.head();
    t.origin.publish();
    assert!(
        git_output(&t.origin.bare, &["merge-base", "--is-ancestor", &t.a, &b])
            .status
            .success()
    );
    assert_ne!(git(&t.origin.bare, &["rev-parse", &format!("{b}^1")]), t.a);

    t.clone_equals_origin(&b);
}

/// E3: a force-push replaces the history; A is not an ancestor of B.
#[test]
fn e3_top_up_after_a_force_push() {
    let t = TopUp::new("e3");
    let work = &t.origin.work;
    git(work, &["checkout", "-q", "--orphan", "rewritten"]);
    git(work, &["rm", "-q", "-r", "-f", "d"]);
    t.origin.commit(&[("a.txt", "r1\n")], "R1");
    t.origin.commit(&[("e.txt", "e\n")], "B");
    git(work, &["branch", "-q", "-M", "main"]);
    let b = t.origin.head();
    t.origin.publish();
    assert!(
        !git_output(&t.origin.bare, &["merge-base", "--is-ancestor", &t.a, &b])
            .status
            .success()
    );

    t.clone_equals_origin(&b);
}

/// E4: with no ready Full for the ref there is no base, so a full clone
/// waits for Full(B) and installs it once the server builds it.
#[test]
fn e4_no_base_waits_for_full() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "e4");
    origin.commit(&[("a.txt", "a\n")], "A");
    let b = origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();
    let server = world.server(&[]);
    http.hold_fetches();
    world.cli(&server, &["add", "acme/e4"]).ok();
    http.wait_held(1);

    let proxy = FaultProxy::start(&server);
    let target = world.target("e4");
    let clone = world.cli_spawn(&proxy, &["clone", "acme/e4", target.to_str().unwrap()]);
    proxy.wait_seen("two pending reads of B", |seen| {
        seen.iter().filter(|s| api::is_commit_read(s, &b)).count() >= 2
    });
    http.release_fetches();
    let out = clone.wait();
    out.ok();

    assert!(out.stderr().contains("warming"), "{}", out.stderr());
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&target), origin.tree(&b));
    assert_eq!(
        git(&target, &["rev-list", "--count", "HEAD"]),
        origin.count(&b)
    );
}
