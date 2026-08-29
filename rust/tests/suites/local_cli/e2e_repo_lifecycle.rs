//! Real-binary coverage for the explicit add/list/rm repository lifecycle.

use crate::common::*;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

fn cli_binary() -> PathBuf {
    std::env::var_os("RIPCLONE_TEST_CLI_BIN")
        .or_else(|| std::env::var_os("CARGO_BIN_EXE_ripclone"))
        .map(PathBuf::from)
        .expect("RIPCLONE_TEST_CLI_BIN or CARGO_BIN_EXE_ripclone")
}

async fn run_cli(server: &Server, home: &Path, args: &[&str]) -> Output {
    let mut command = tokio::process::Command::new(cli_binary());
    command
        .args(args)
        .env("HOME", home)
        .env("RIPCLONE_SERVER", &server.url)
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        // The server knows GitLab, but this CLI intentionally does not. This
        // proves a provider-qualified line emitted by `list` is removable
        // without a local provider configuration or upstream credential.
        .env_remove("RIPCLONE_PROVIDERS")
        .kill_on_drop(true);
    tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .expect("CLI lifecycle command stayed bounded")
        .expect("spawn ripclone CLI")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn diagnostics(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[tokio::test]
async fn real_cli_lists_stably_and_round_trips_unconfigured_provider_names() {
    setup(false);
    let providers = serde_json::json!({
        "providers": [{
            "id": "gitlab",
            "kind": "gitlab",
            "host": "gitlab.example.test"
        }]
    })
    .to_string();
    let server = start_server_env(&[("RIPCLONE_PROVIDERS", &providers)]).await;
    let home = tempfile::tempdir().unwrap();

    let empty = run_cli(&server, home.path(), &["list"]).await;
    assert!(empty.status.success(), "{}", diagnostics(&empty));
    assert_eq!(stdout(&empty), "");

    let origin = make_origin("zeta", "repo");
    origin.commit(&[("README.md", "registered through the CLI\n")], "initial");
    origin.publish();
    let added = run_cli(&server, home.path(), &["add", "zeta/repo"]).await;
    assert!(added.status.success(), "{}", diagnostics(&added));

    // Insert the remaining fixtures in non-sort order. Registration itself is
    // already covered by the real `add` above; these rows exercise formatting.
    register_added_without_build_for_provider(&server, "gitlab", "group/sub/repo")
        .await
        .unwrap();
    register_added_without_build(&server, "alpha/repo")
        .await
        .unwrap();

    let listed = run_cli(&server, home.path(), &["list"]).await;
    assert!(listed.status.success(), "{}", diagnostics(&listed));
    assert_eq!(
        stdout(&listed),
        "alpha/repo\ngitlab:group/sub/repo\nzeta/repo\n"
    );
    assert_eq!(
        stdout(&listed)
            .lines()
            .filter(|line| *line == "zeta/repo")
            .count(),
        1,
        "a repository added through the CLI appears exactly once"
    );

    // Selecting GitLab as the CLI default changes only presentation. The CLI
    // still has no local GitLab provider entry.
    let selected_default = run_cli(&server, home.path(), &["--provider", "gitlab", "list"]).await;
    assert!(
        selected_default.status.success(),
        "{}",
        diagnostics(&selected_default)
    );
    let selected_default_stdout = stdout(&selected_default);
    assert_eq!(
        selected_default_stdout,
        "github:alpha/repo\ngithub:zeta/repo\ngroup/sub/repo\n"
    );

    let bare_name = selected_default_stdout
        .lines()
        .find(|name| *name == "group/sub/repo")
        .expect("selected default provider should render as a bare name");
    let removed_bare = run_cli(
        &server,
        home.path(),
        &["--provider", "gitlab", "rm", bare_name],
    )
    .await;
    assert!(
        removed_bare.status.success(),
        "{}",
        diagnostics(&removed_bare)
    );
    assert_eq!(stdout(&removed_bare), "removed group/sub/repo\n");

    // Re-register the row so the provider-qualified form printed by the
    // default listing is independently round-tripped too.
    register_added_without_build_for_provider(&server, "gitlab", "group/sub/repo")
        .await
        .unwrap();

    let removed = run_cli(&server, home.path(), &["rm", "gitlab:group/sub/repo"]).await;
    assert!(removed.status.success(), "{}", diagnostics(&removed));
    assert_eq!(stdout(&removed), "removed gitlab:group/sub/repo\n");

    let after_remove = run_cli(&server, home.path(), &["list"]).await;
    assert!(
        after_remove.status.success(),
        "{}",
        diagnostics(&after_remove)
    );
    assert_eq!(stdout(&after_remove), "alpha/repo\nzeta/repo\n");

    let before_missing = server_ref_store(&server)
        .await
        .list_added_repos()
        .await
        .unwrap();
    let missing = run_cli(&server, home.path(), &["rm", "acme/not-added"]).await;
    assert!(
        !missing.status.success(),
        "missing rm unexpectedly succeeded"
    );
    assert!(
        diagnostics(&missing).contains("repo not added"),
        "{}",
        diagnostics(&missing)
    );
    let after_missing = server_ref_store(&server)
        .await
        .list_added_repos()
        .await
        .unwrap();
    assert_eq!(before_missing, after_missing);

    let removed_added = run_cli(&server, home.path(), &["rm", "zeta/repo"]).await;
    assert!(
        removed_added.status.success(),
        "{}",
        diagnostics(&removed_added)
    );
    assert_eq!(stdout(&removed_added), "removed zeta/repo\n");

    let target = home.path().join("removed-clone");
    let clone_removed = run_cli(
        &server,
        home.path(),
        &[
            "clone",
            "zeta/repo",
            target.to_str().unwrap(),
            "--verify-upstream=never",
        ],
    )
    .await;
    assert!(!clone_removed.status.success());
    assert!(
        diagnostics(&clone_removed).contains("repo not added"),
        "{}",
        diagnostics(&clone_removed)
    );
    assert!(!target.exists());
}
