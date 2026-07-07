//! End-to-end tests for agent-fleet ergonomics (F6).
//!
//! Drives the real `ripclone` binary as a headless fleet VM would: token in the
//! environment, stdin closed (no TTY), no login round-trip. Verifies:
//!   1. `RIPCLONE_AGENT=1` flips the clone default to depth-1 (shallow) — agents
//!      on giant repos don't want full history — while the human default stays
//!      full history (this is an explicit switch, not a silent size heuristic).
//!   2. An explicit `--depth 0` still wins over the agent default.
//!   3. A files-mode fleet clone materializes only the working tree, correctly.
//!
//! All clones run with stdin closed to prove the headless path never prompts.

mod common;

use common::*;
use std::process::{Command, Stdio};

fn ripclone_bin() -> String {
    std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone not set")
}

/// Run the CLI like a fleet VM: HOME + server token in env, stdin CLOSED so any
/// interactive prompt would fail fast rather than hang. `agent` toggles the
/// `RIPCLONE_AGENT` env var that turns on fleet defaults.
async fn run_fleet(
    bin: &str,
    home: &std::path::Path,
    cwd: &std::path::Path,
    server_url: &str,
    agent: bool,
    args: &[&str],
) -> std::process::Output {
    let bin = bin.to_string();
    let home = home.to_path_buf();
    let cwd = cwd.to_path_buf();
    let server_url = server_url.to_string();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::new(&bin);
        cmd.args(&args)
            .current_dir(&cwd)
            .env("HOME", &home)
            .env("RIPCLONE_SERVER", &server_url)
            .env("RIPCLONE_SERVER_TOKEN", TOKEN)
            // Closed stdin: a real agent VM has no TTY. A prompt here would
            // error instead of blocking, which is the behavior we want to prove.
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if agent {
            cmd.env("RIPCLONE_AGENT", "1");
        }
        cmd.output().expect("spawn ripclone")
    })
    .await
    .expect("subprocess panicked")
}

fn assert_ok(what: &str, out: &std::process::Output) {
    assert!(
        out.status.success(),
        "{what} failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test]
async fn agent_mode_defaults_to_depth1_and_is_explicit() {
    setup(false);

    // Three commits so full history (count 3) is clearly distinguishable from a
    // depth-1 shallow clone (count 1).
    let origin = make_origin("acme", "fleet");
    origin.commit(&[("README.md", "v1\n")], "c1");
    origin.commit(&[("README.md", "v2\n")], "c2");
    origin.commit(
        &[("README.md", "v3\n"), ("src/main.rs", "fn main() {}\n")],
        "c3",
    );
    origin.publish();

    let server = start_server().await;
    let home = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let bin = ripclone_bin();

    assert_ok(
        "add",
        &run_fleet(
            &bin,
            home.path(),
            cwd.path(),
            &server.url,
            true,
            &["add", "acme/fleet"],
        )
        .await,
    );
    assert_ok(
        "sync",
        &run_fleet(
            &bin,
            home.path(),
            cwd.path(),
            &server.url,
            true,
            &["sync", "acme/fleet"],
        )
        .await,
    );

    // 1. Agent mode, no --depth → shallow depth-1 clone. No prompt (stdin closed)
    //    and the mode announces itself on stderr (explicit, not silent).
    let agent_out = run_fleet(
        &bin,
        home.path(),
        cwd.path(),
        &server.url,
        true,
        &["clone", "acme/fleet", "agent_clone"],
    )
    .await;
    assert_ok("agent clone", &agent_out);
    let agent_target = cwd.path().join("agent_clone");
    assert!(
        agent_target.join(".git/shallow").exists(),
        "agent-mode clone must default to depth-1 (shallow)"
    );
    assert_eq!(
        git(&agent_target, &["rev-list", "--count", "HEAD"]),
        "1",
        "depth-1 has a single commit"
    );
    assert_eq!(read(&agent_target, "README.md"), "v3\n");
    assert!(
        String::from_utf8_lossy(&agent_out.stderr).contains("agent-fleet mode"),
        "agent mode should announce itself on stderr: {}",
        String::from_utf8_lossy(&agent_out.stderr)
    );

    // 2. No agent mode → full history (the human D8 default). Proves depth-1 is
    //    an explicit opt-in, not a silent size-based switch applied to everyone.
    let human_out = run_fleet(
        &bin,
        home.path(),
        cwd.path(),
        &server.url,
        false,
        &["clone", "acme/fleet", "human_clone"],
    )
    .await;
    assert_ok("human clone", &human_out);
    let human_target = cwd.path().join("human_clone");
    assert!(
        !human_target.join(".git/shallow").exists(),
        "the human default must stay full history, not shallow"
    );
    assert_eq!(
        git(&human_target, &["rev-list", "--count", "HEAD"]),
        "3",
        "human default clones full history"
    );

    // 3. Explicit --depth 0 under agent mode still wins over the agent default.
    let override_out = run_fleet(
        &bin,
        home.path(),
        cwd.path(),
        &server.url,
        true,
        &["clone", "--depth", "0", "acme/fleet", "override_clone"],
    )
    .await;
    assert_ok("depth-0 override under agent mode", &override_out);
    let override_target = cwd.path().join("override_clone");
    assert!(
        !override_target.join(".git/shallow").exists(),
        "an explicit --depth 0 must override the agent depth-1 default"
    );
}

#[tokio::test]
async fn agent_files_mode_fleet_clone_is_correct() {
    setup(false);

    let origin = make_origin("acme", "worktree");
    origin.commit(
        &[
            ("README.md", "fleet worktree agent\n"),
            ("src/lib.rs", "pub fn f() {}\n"),
            ("docs/guide.md", "guide\n"),
        ],
        "c1",
    );
    origin.publish();

    let server = start_server().await;
    let home = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let bin = ripclone_bin();

    assert_ok(
        "add",
        &run_fleet(
            &bin,
            home.path(),
            cwd.path(),
            &server.url,
            true,
            &["add", "acme/worktree"],
        )
        .await,
    );
    assert_ok(
        "sync",
        &run_fleet(
            &bin,
            home.path(),
            cwd.path(),
            &server.url,
            true,
            &["sync", "acme/worktree"],
        )
        .await,
    );

    // Pure worktree agent: files mode + agent mode, headless.
    let clone_out = run_fleet(
        &bin,
        home.path(),
        cwd.path(),
        &server.url,
        true,
        &["clone", "--mode", "files", "acme/worktree", "files_clone"],
    )
    .await;
    assert_ok("files-mode fleet clone", &clone_out);

    let target = cwd.path().join("files_clone");
    assert_eq!(read(&target, "README.md"), "fleet worktree agent\n");
    assert_eq!(read(&target, "src/lib.rs"), "pub fn f() {}\n");
    assert_eq!(read(&target, "docs/guide.md"), "guide\n");
    assert!(
        !target.join(".git").exists(),
        "files mode materializes only the working tree, no .git"
    );
}
