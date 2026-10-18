//! A live Gitea: a private repo whose token only the server holds. Ignored by
//! default because it needs a running Gitea; the `gitea` CI job starts one and
//! runs it with `--ignored gitea_`.

use crate::harness::*;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct Gitea {
    url: String,
    token: String,
    user: String,
}

impl Gitea {
    fn from_env() -> Self {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| panic!("{name} is not set"))
        };
        Gitea {
            url: var("RIPCLONE_GITEA_URL").trim_end_matches('/').to_string(),
            token: var("RIPCLONE_GITEA_TOKEN"),
            user: std::env::var("RIPCLONE_GITEA_USER").unwrap_or_else(|_| "ci".into()),
        }
    }

    fn api(&self, method: reqwest::Method, path: &str) -> reqwest::blocking::RequestBuilder {
        reqwest::blocking::Client::new()
            .request(method, format!("{}/api/v1{path}", self.url))
            .header("Authorization", format!("token {}", self.token))
    }

    fn plain_url(&self, repo: &str) -> String {
        format!("{}/{}/{repo}.git", self.url, self.user)
    }

    fn authed_url(&self, repo: &str) -> String {
        let (scheme, host) = self.url.split_once("://").expect("URL has a scheme");
        format!(
            "{scheme}://{}:{}@{host}/{}/{repo}.git",
            self.user, self.token, self.user
        )
    }
}

/// Push one commit to `repo` from a fresh authenticated clone; return its SHA
/// and tree listing.
fn push(world: &World, gitea: &Gitea, repo: &str, file: &str, text: &str) -> (String, String) {
    let work = world.path(&format!("push-{file}"));
    let parent = work.parent().unwrap();
    git(
        parent,
        &[
            "clone",
            "-q",
            &gitea.authed_url(repo),
            work.to_str().unwrap(),
        ],
    );
    std::fs::write(work.join(file), text).unwrap();
    git(&work, &["add", file]);
    git(&work, &["commit", "-q", "-m", file]);
    git(&work, &["push", "-q", "origin", "HEAD:main"]);
    let tip = git(&work, &["rev-parse", "HEAD"]);
    (tip, listing(&work))
}

#[test]
#[ignore = "needs a live Gitea; run by the gitea CI job"]
fn gitea_private_repo_with_server_side_token() {
    let gitea = Gitea::from_env();
    let world = World::new();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let repo = format!("ripclone-ci-{nanos}");
    let created = gitea
        .api(reqwest::Method::POST, "/user/repos")
        .json(&serde_json::json!({
            "name": repo,
            "private": true,
            "auto_init": true,
            "default_branch": "main",
        }))
        .send()
        .expect("create repo");
    assert!(created.status().is_success(), "{}", created.status());

    let (tip, tree) = push(&world, &gitea, &repo, "hello.txt", "server-side token\n");
    // The repo really is private, so only the server's token can read it.
    let anonymous = Command::new("git")
        .args(["ls-remote", &gitea.plain_url(&repo)])
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        !anonymous.status.success(),
        "the repo is readable anonymously"
    );

    world.server_providers(&gitea.url, &[("gitea", "gitea", serde_json::json!({}))]);
    let server = world.server(&[("RIPCLONE_TOKEN_GITEA", &gitea.token)]);
    world.cli_providers(&gitea.url, &[("gitea", "gitea", serde_json::json!({}))]);
    let arg = format!("gitea:{}/{repo}", gitea.user);
    world.cli(&server, &["add", &arg, "--wait"]).ok();

    let first = world.target("first");
    let clone = [
        "clone",
        &arg,
        first.to_str().unwrap(),
        "--verify-upstream",
        "never",
    ];
    world.cli(&server, &clone).ok();
    assert_eq!(git(&first, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&first), tree);

    let (tip, tree) = push(&world, &gitea, &repo, "update.txt", "second\n");
    world.cli(&server, &["sync", &arg, "--wait"]).ok();
    let second = world.target("second");
    let clone = [
        "clone",
        &arg,
        second.to_str().unwrap(),
        "--verify-upstream",
        "never",
    ];
    world.cli(&server, &clone).ok();
    assert_eq!(git(&second, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&second), tree);

    let _ = gitea
        .api(
            reqwest::Method::DELETE,
            &format!("/repos/{}/{repo}", gitea.user),
        )
        .send();
}
