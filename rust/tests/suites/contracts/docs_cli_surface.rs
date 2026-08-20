//! Doc-lint: every `ripclone <verb>` the OSS docs tell a user to run must exist.
//! Included in the bounded `local_cli` integration-test crate.
//!
//! The README and `docs/` are the product's front door. A doc that names a verb
//! the CLI doesn't have is a broken quick start, so this test walks every fenced
//! code block in the docs, pulls out the `ripclone` invocations, and asks the
//! real binary whether each verb exists. It also pins the removed `track` /
//! `untrack` verbs staying removed from the prose.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rust/ has a parent")
        .to_path_buf()
}

fn doc_files() -> Vec<PathBuf> {
    let root = repo_root();
    let mut files = vec![root.join("README.md")];
    let mut stack = vec![root.join("docs")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read docs dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                // internal/ holds design notes, not user-facing instructions.
                if path.file_name().is_some_and(|n| n == "internal") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// Pull `ripclone <verb>` out of the shell blocks in a doc.
///
/// A line counts only when `ripclone` is the *command* being run — the first
/// word, after stripping a shell prompt, `sudo`, and `KEY=value` env prefixes.
/// That skips `ripclone-server`, `ripclone://` URLs, and prose or YAML that
/// merely contains the word (e.g. `name: ripclone cache`).
fn documented_verbs(markdown: &str) -> BTreeSet<String> {
    let mut verbs = BTreeSet::new();
    let mut in_block = false;
    for line in markdown.lines() {
        if line.trim_start().starts_with("```") {
            in_block = !in_block;
            continue;
        }
        if !in_block || line.trim_start().starts_with('#') {
            continue;
        }
        let mut words = line.split_whitespace().peekable();
        // Strip prompt / sudo / env prefixes to find the command word.
        while let Some(w) = words.peek().copied() {
            if w == "$" || w == "sudo" || (w.contains('=') && !w.starts_with('-')) {
                words.next();
            } else {
                break;
            }
        }
        if words.next() != Some("ripclone") {
            continue;
        }
        // Skip global flags that sit before the subcommand.
        while let Some(w) = words.peek().copied() {
            if !w.starts_with('-') {
                break;
            }
            words.next();
            if matches!(
                w,
                "--server" | "--token" | "--provider" | "-s" | "-t" | "-p"
            ) {
                words.next();
            }
        }
        if let Some(verb) = words.next()
            && !verb.is_empty()
            && verb.chars().all(|c| c.is_ascii_lowercase() || c == '-')
        {
            verbs.insert(verb.to_string());
        }
    }
    verbs
}

#[test]
fn every_documented_ripclone_verb_exists() {
    let bin = std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone");
    let mut missing: Vec<String> = Vec::new();

    for doc in doc_files() {
        let text = std::fs::read_to_string(&doc).expect("read doc");
        for verb in documented_verbs(&text) {
            let out = Command::new(&bin)
                .args([&verb, "--help"])
                .output()
                .expect("spawn ripclone");
            if !out.status.success() {
                missing.push(format!(
                    "{}: `ripclone {verb}` does not exist ({})",
                    doc.display(),
                    String::from_utf8_lossy(&out.stderr)
                        .lines()
                        .next()
                        .unwrap_or("")
                ));
            }
        }
    }

    assert!(
        missing.is_empty(),
        "docs reference CLI verbs the binary does not have:\n  {}",
        missing.join("\n  ")
    );
}

/// `track` / `untrack` were removed in favour of the added-repo set
/// (`ripclone add`). They must not creep back into the user-facing docs.
#[test]
fn removed_track_verbs_stay_out_of_the_docs() {
    let mut hits: Vec<String> = Vec::new();
    for doc in doc_files() {
        // CHANGELOG records the removal, and WEBHOOKS explains that they do not
        // exist; both must be able to name them.
        if doc
            .file_name()
            .is_some_and(|n| n == "CHANGELOG.md" || n == "WEBHOOKS.md")
        {
            continue;
        }
        let text = std::fs::read_to_string(&doc).expect("read doc");
        for verb in documented_verbs(&text) {
            if verb == "track" || verb == "untrack" || verb == "tracked" {
                hits.push(format!("{}: `ripclone {verb}`", doc.display()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "removed verbs resurfaced in the docs:\n  {}",
        hits.join("\n  ")
    );
}

/// The two materialize surfaces the README promises must both be real flags on
/// the binary, with the documented spellings.
#[test]
fn the_two_documented_materialize_surfaces_exist() {
    let bin = std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone");
    let clone_help = String::from_utf8(
        Command::new(&bin)
            .args(["clone", "--help"])
            .output()
            .expect("spawn")
            .stdout,
    )
    .expect("utf8");
    assert!(clone_help.contains("--mode"), "clone --mode is documented");
    assert!(
        clone_help.contains("--depth"),
        "clone --depth is documented"
    );

    for mode in ["editable", "files"] {
        assert!(
            mode.parse::<ripclone::mode::CloneMode>().is_ok(),
            "`--mode {mode}` must parse"
        );
    }
}

/// The experimental `worktree` command was deleted. Clap must reject it as an
/// unknown subcommand rather than leaving a hidden or half-wired surface, and
/// the user-facing docs must not name it.
#[test]
fn removed_worktree_verb_is_unknown_and_undocumented() {
    let bin = std::env::var("CARGO_BIN_EXE_ripclone").expect("CARGO_BIN_EXE_ripclone");
    let out = Command::new(&bin)
        .args(["worktree", "--help"])
        .output()
        .expect("spawn");
    assert!(
        !out.status.success(),
        "`ripclone worktree` must be rejected as an unknown command"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
        "clap must name it as unknown: {stderr}"
    );

    let mut hits: Vec<String> = Vec::new();
    for doc in doc_files() {
        // CHANGELOG records the removal and must be able to name it.
        if doc.file_name().is_some_and(|n| n == "CHANGELOG.md") {
            continue;
        }
        let text = std::fs::read_to_string(&doc).expect("read doc");
        for verb in documented_verbs(&text) {
            if verb == "worktree" {
                hits.push(format!("{}: `ripclone {verb}`", doc.display()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "the removed worktree verb resurfaced in the docs:\n  {}",
        hits.join("\n  ")
    );
}
