//! End-to-end test for the `ripclone provider` CLI.
//!
//! Uses a temporary $HOME so the provider config and token files are isolated.

use std::process::Command;

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

fn run(args: &[&str], home: &std::path::Path) -> std::process::Output {
    run_with_env(args, home, std::iter::empty::<(&str, &str)>())
}

fn run_with_env<'a>(
    args: &[&str],
    home: &std::path::Path,
    extra: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> std::process::Output {
    Command::new(ripclone_bin())
        .args(args)
        .env("HOME", home)
        .env("RIPCLONE_SERVER", "http://localhost:1")
        .envs(extra)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("spawn ripclone")
}

#[test]
fn provider_add_list_rm_lifecycle() {
    let home = tempfile::tempdir().unwrap();

    let add = run(
        &[
            "provider",
            "add",
            "gitlab",
            "--kind",
            "gitlab",
            "--host",
            "gitlab.com",
            "--token",
            "glpat-test",
        ],
        home.path(),
    );
    assert!(
        add.status.success(),
        "provider add failed: {}\n{}",
        String::from_utf8_lossy(&add.stdout),
        String::from_utf8_lossy(&add.stderr)
    );

    // Resolve the token from the per-provider env var, which takes precedence
    // over the token file.
    let list = run_with_env(
        &["provider", "list"],
        home.path(),
        [("RIPCLONE_PROVIDER_GITLAB_TOKEN", "glpat-test")],
    );
    let list_out = String::from_utf8_lossy(&list.stdout);
    assert!(list.status.success(), "provider list failed: {list_out}");
    assert!(
        list_out.contains("gitlab"),
        "provider list should contain gitlab: {list_out}"
    );
    assert!(
        list_out.contains("configured"),
        "provider list should show token configured: {list_out}"
    );

    // The config file should declare the provider but never contain the token.
    let config = home
        .path()
        .join(".config")
        .join("ripclone")
        .join("config.toml");
    let config_text = std::fs::read_to_string(&config).expect("read config.toml");
    assert!(
        config_text.contains("gitlab"),
        "config.toml should declare gitlab: {config_text}"
    );
    assert!(
        !config_text.contains("glpat-test"),
        "config.toml must not contain the token: {config_text}"
    );

    let rm = run(&["provider", "rm", "gitlab"], home.path());
    assert!(
        rm.status.success(),
        "provider rm failed: {}",
        String::from_utf8_lossy(&rm.stderr)
    );

    let list2 = run(&["provider", "list"], home.path());
    let list2_out = String::from_utf8_lossy(&list2.stdout);
    assert!(
        !list2_out.contains("gitlab"),
        "provider list should not contain gitlab after rm: {list2_out}"
    );
}

#[test]
fn provider_add_requires_generic_auth_template() {
    let home = tempfile::tempdir().unwrap();
    let out = run(
        &["provider", "add", "mygit", "--kind", "generic"],
        home.path(),
    );
    assert!(
        !out.status.success(),
        "generic provider without auth_template should fail"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("auth-template") || stderr.contains("auth_template"),
        "error should mention auth_template: {stderr}"
    );
}
