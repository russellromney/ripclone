//! I6: every `ripclone <verb>` the user-facing docs tell a user to run exists.
//! A doc that names a verb the CLI doesn't have is a broken quick start, so
//! this walks every fenced code block in the README and `docs/`, pulls out the
//! `ripclone` invocations, and asks the real binary for each verb's help.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::harness::bin;

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
/// That skips `ripclone-server` and prose or YAML that
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
fn i6_every_documented_cli_verb_exists() {
    let bin = bin("ripclone");
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
