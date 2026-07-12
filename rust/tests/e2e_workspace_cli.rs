//! Workspace CLI contract: one upstream, durable canonical config, and
//! validation before mutation.

use std::process::Command;

fn run(args: &[&str], home: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ripclone"))
        .args(args)
        .env("HOME", home)
        .env("RIPCLONE_SERVER", "http://localhost:1")
        .env_remove("RIPCLONE_WORKSPACE")
        .env_remove("RIPCLONE_PROVIDERS")
        .output()
        .expect("spawn ripclone")
}

fn config_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".config").join("ripclone").join("config.toml")
}

#[test]
fn workspace_set_and_show_round_trip() {
    let home = tempfile::tempdir().unwrap();
    let set = run(
        &[
            "workspace",
            "set",
            "acme",
            "--provider",
            "gitlab",
            "--host",
            "gitlab.example.com",
            "--token",
            "secret",
        ],
        home.path(),
    );
    assert!(
        set.status.success(),
        "set failed: {}",
        String::from_utf8_lossy(&set.stderr)
    );
    let text = std::fs::read_to_string(config_path(home.path())).unwrap();
    assert!(text.contains("default_workspace = \"acme\""), "{text}");
    assert!(text.contains("[workspace]"), "{text}");
    assert!(text.contains("provider = \"gitlab\""), "{text}");

    let show = run(&["workspace", "show"], home.path());
    assert!(
        show.status.success(),
        "{}",
        String::from_utf8_lossy(&show.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&show.stdout).trim(),
        "acme\tgitlab\tgitlab.example.com"
    );
}

#[test]
fn invalid_workspace_does_not_write_config() {
    let home = tempfile::tempdir().unwrap();
    let out = run(
        &["workspace", "set", "../escape", "--provider", "github"],
        home.path(),
    );
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid workspace"));
    assert!(!config_path(home.path()).exists());
}

#[test]
fn invalid_generic_upstream_does_not_replace_valid_workspace() {
    let home = tempfile::tempdir().unwrap();
    assert!(
        run(
            &["workspace", "set", "acme", "--provider", "github"],
            home.path()
        )
        .status
        .success()
    );
    let before = std::fs::read_to_string(config_path(home.path())).unwrap();

    let out = run(
        &[
            "workspace",
            "set",
            "acme",
            "--provider",
            "generic",
            "--host",
            "git.example.com",
        ],
        home.path(),
    );
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("auth_template"));
    assert_eq!(
        std::fs::read_to_string(config_path(home.path())).unwrap(),
        before
    );
}

#[test]
fn workspace_set_repairs_parseable_but_invalid_legacy_provider_config() {
    let home = tempfile::tempdir().unwrap();
    let path = config_path(home.path());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        r#"default_provider = "broken"

[providers.broken]
kind = "generic"
"#,
    )
    .unwrap();

    let out = run(
        &["workspace", "set", "acme", "--provider", "github"],
        home.path(),
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("[workspace]"), "{text}");
    assert!(!text.contains("providers.broken"), "{text}");
    assert!(!text.contains("default_provider"), "{text}");
    assert!(run(&["workspace", "show"], home.path()).status.success());
}
