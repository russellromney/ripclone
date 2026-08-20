//! Static caller checks for the deleted client paths.
//!
//! Deleting a function is only durable if nothing quietly reintroduces it. These
//! read the real sources: the named dead entry points must have no definition
//! and no caller, and `extract.rs` must perform no network request — its only
//! HTTP client existed for the removed `worktree` command.

use std::path::{Path, PathBuf};

fn src_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_sources() -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![src_dir()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read src dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// The client install/fetch entry points Slice 7 deleted. A definition or a call
/// site anywhere in `src/` means one of them came back.
const DELETED_CLIENT_ITEMS: &[&str] = &[
    "install_ref",
    "install_repo",
    "install_repo_with_mode",
    "install_worktree_files",
    "install_prebuilt_blob_pack",
    "install_chunked_pack",
    "add_worktree",
    "extract_clonepack_streaming",
    "verify_archive",
    "fetch_artifact_with_url_cache",
];

#[test]
fn deleted_client_entry_points_have_no_definition_or_caller() {
    let mut hits: Vec<String> = Vec::new();
    for file in rust_sources() {
        let text = std::fs::read_to_string(&file).expect("read source");
        for (lineno, line) in text.lines().enumerate() {
            for item in DELETED_CLIENT_ITEMS {
                // `install_repo_with_mode_at` is the retained entry point, so
                // match the identifier exactly rather than as a prefix.
                if line.match_indices(item).any(|(idx, _)| {
                    let after = line[idx + item.len()..].chars().next();
                    let before = line[..idx].chars().next_back();
                    !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
                        && !before.is_some_and(|c| c.is_alphanumeric() || c == '_')
                }) {
                    hits.push(format!(
                        "{}:{}: {}",
                        file.display(),
                        lineno + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "deleted client entry points reappeared in src/:\n  {}",
        hits.join("\n  ")
    );
}

#[test]
fn extract_makes_no_network_request() {
    let text = std::fs::read_to_string(src_dir().join("extract.rs")).expect("read extract.rs");
    for needle in ["reqwest", "http://", "https://", "TcpStream"] {
        assert!(
            !text.contains(needle),
            "extract.rs must perform no network request, found {needle:?}"
        );
    }
}

/// The streamed-file download exists so a large pack never lands in memory. If
/// a history-only pack ever routed through the buffered helper, it would be held
/// whole and then copied again on the way to `.git/objects/pack/`.
#[test]
fn history_packs_stream_instead_of_buffering() {
    let text = std::fs::read_to_string(src_dir().join("client.rs")).expect("read client.rs");
    assert!(
        text.contains("async fn fetch_artifact_to_temp("),
        "the streamed-file download helper must exist"
    );
    let branch = text
        .split_once("let pack_body = if history_only {")
        .expect("editable install must branch on history_only")
        .1
        .split_once("} else {")
        .expect("history_only branch must have a buffered sibling")
        .0;
    assert!(
        branch.contains("fetch_chunk_ref_to_temp("),
        "history-only packs must stream to a temporary file: {branch}"
    );
}
