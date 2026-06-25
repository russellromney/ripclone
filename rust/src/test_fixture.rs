//! Test helpers for creating git repos and commits without `git2`.
//!
//! These are intentionally simple: bare repos, flat or nested file commits,
//! and optional submodule/commit entries. They exist only for unit tests.

use anyhow::{Context, Result};
use gix::objs::{Blob, Commit, Tree, tree::Entry};
use gix::refs::transaction::PreviousValue;
use std::collections::BTreeMap;
use std::path::Path;

const TEST_AUTHOR: &str = "test";
const TEST_EMAIL: &str = "test@example.com";

fn signature() -> gix::actor::Signature {
    gix::actor::Signature {
        name: TEST_AUTHOR.into(),
        email: TEST_EMAIL.into(),
        time: gix::date::Time {
            seconds: 0,
            offset: 0,
        },
    }
}

/// Initialize a bare repo at `path`.
pub fn init_bare(path: &Path) -> gix::Repository {
    gix::init_bare(path).expect("init bare repo")
}

/// Create a commit containing `files` (regular blobs, mode 0o100644) and return
/// its hex SHA.
pub fn commit(repo: &gix::Repository, files: &[(&str, &[u8])]) -> String {
    let with_mode: Vec<(&str, u32, &[u8])> =
        files.iter().map(|(p, c)| (*p, 0o100644, *c)).collect();
    commit_with_modes(repo, &with_mode)
}

/// Create a commit with per-file modes and return its hex SHA.
///
/// For `mode == 0o160000`, `content` is interpreted as the hex SHA of a commit
/// (used for submodule entries). For all other modes, `content` is the blob
/// bytes.
pub fn commit_with_modes(repo: &gix::Repository, files: &[(&str, u32, &[u8])]) -> String {
    let files: Vec<(&[u8], u32, &[u8])> = files
        .iter()
        .map(|(p, m, c)| (p.as_bytes(), *m, *c))
        .collect();
    commit_bytes_with_modes(repo, &files)
}

/// Like `commit_with_modes`, but paths are raw byte slices (allows non-UTF-8
/// filenames).
pub fn commit_bytes(repo: &gix::Repository, files: &[(&[u8], &[u8])]) -> String {
    let files: Vec<(&[u8], u32, &[u8])> = files.iter().map(|(p, c)| (*p, 0o100644, *c)).collect();
    commit_bytes_with_modes(repo, &files)
}

fn ensure_committer_env() {
    // gix needs a committer identity to write the reflog. CI runners don't always
    // have git user config, so fall back to the test identity.
    if std::env::var_os("GIT_COMMITTER_NAME").is_none() {
        unsafe { std::env::set_var("GIT_COMMITTER_NAME", TEST_AUTHOR) };
    }
    if std::env::var_os("GIT_COMMITTER_EMAIL").is_none() {
        unsafe { std::env::set_var("GIT_COMMITTER_EMAIL", TEST_EMAIL) };
    }
}

fn commit_bytes_with_modes(repo: &gix::Repository, files: &[(&[u8], u32, &[u8])]) -> String {
    ensure_committer_env();
    let parents = head_commit_ids(repo);
    let tree_id = build_tree(repo, files).expect("build tree");

    let commit_id = repo
        .write_object(Commit {
            tree: tree_id,
            parents: parents.into(),
            author: signature(),
            committer: signature(),
            encoding: None,
            message: "test commit".into(),
            extra_headers: vec![],
        })
        .expect("write commit")
        .detach();

    repo.reference("refs/heads/main", commit_id, PreviousValue::Any, "commit")
        .expect("update main");

    commit_id.to_string()
}

fn head_commit_ids(repo: &gix::Repository) -> Vec<gix::hash::ObjectId> {
    repo.head_ref()
        .ok()
        .flatten()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(|id| id.detach())
        .into_iter()
        .collect()
}

/// Build a tree (with nested directories) from a flat list of byte paths.
fn build_tree(
    repo: &gix::Repository,
    files: &[(&[u8], u32, &[u8])],
) -> Result<gix::hash::ObjectId> {
    // directory components -> entries
    let mut dirs: BTreeMap<Vec<Vec<u8>>, Vec<Entry>> = BTreeMap::new();

    for (path, mode, content) in files {
        let mode = gix::objs::tree::EntryMode::try_from(*mode)
            .map_err(|m| anyhow::anyhow!("invalid mode {m:#o}"))?;
        let oid = if mode.value() == 0o160000 {
            // submodule entry: content is the hex commit id
            std::str::from_utf8(content)
                .context("submodule commit id is not utf-8")?
                .parse::<gix::hash::ObjectId>()
                .context("parse submodule id")?
        } else {
            repo.write_object(Blob {
                data: content.to_vec(),
            })
            .context("write blob")?
            .detach()
        };

        let parts: Vec<Vec<u8>> = path.split(|&b| b == b'/').map(|s| s.to_vec()).collect();
        if parts.is_empty() || parts.last().unwrap().is_empty() {
            anyhow::bail!("invalid empty path");
        }
        let dir = parts[..parts.len() - 1].to_vec();
        let name: gix::bstr::BString = parts.last().unwrap().clone().into();

        dirs.entry(dir).or_default().push(Entry {
            mode,
            filename: name,
            oid,
        });
    }

    // Collect every directory that contains entries or has children, including
    // intermediate directories that only hold subdirectories.
    let all_dirs: Vec<Vec<Vec<u8>>> = {
        let mut s = std::collections::HashSet::new();
        for dir in dirs.keys() {
            s.insert(dir.clone());
            for i in 0..dir.len() {
                s.insert(dir[..i].to_vec());
            }
        }
        let mut v: Vec<_> = s.into_iter().collect();
        v.sort_by_key(|d| d.len());
        v
    };

    // Build from leaves up to root so children have ids before parents.
    let mut written: BTreeMap<Vec<Vec<u8>>, gix::hash::ObjectId> = BTreeMap::new();
    for dir in all_dirs.into_iter().rev() {
        let mut entries = dirs.remove(dir.as_slice()).unwrap_or_default();
        // Add child trees that belong to this directory.
        for (child_dir, child_id) in &written {
            if child_dir.len() == dir.len() + 1 && child_dir.starts_with(dir.as_slice()) {
                let child_name: gix::bstr::BString = child_dir.last().unwrap().clone().into();
                entries.push(Entry {
                    mode: gix::objs::tree::EntryMode::try_from(0o040000u32).unwrap(),
                    filename: child_name,
                    oid: *child_id,
                });
            }
        }
        entries.sort_by(|a, b| a.filename.cmp(&b.filename));
        let tree_id = repo
            .write_object(Tree { entries })
            .with_context(|| format!("write tree for {:?}", dir))?
            .detach();
        written.insert(dir, tree_id);
    }

    match written.remove(&[][..]) {
        Some(root) => Ok(root),
        None => {
            // Empty tree (no files).
            Ok(repo
                .write_object(Tree { entries: vec![] })
                .context("write empty tree")?
                .detach())
        }
    }
}
