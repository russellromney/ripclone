use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::cas::{hash, Cas};
use crate::git;

/// Build a skeleton pack for a commit: commit object + all reachable trees.
pub fn make_skeleton_pack<P: AsRef<Path>, Q: AsRef<Path>>(
    bare_repo: P,
    commit: &str,
    output: Q,
) -> Result<usize> {
    let resolved = git::resolve_commit(&bare_repo, commit)?;
    let objects = git::list_objects(&bare_repo, &resolved)?;

    let mut skeleton_shas: Vec<String> = Vec::new();
    for (sha, obj_type) in objects {
        if obj_type == "commit" || obj_type == "tree" {
            skeleton_shas.push(sha);
        }
    }

    if skeleton_shas.is_empty() {
        anyhow::bail!("no commit/tree objects found for {}", resolved);
    }

    git::pack_objects(&bare_repo, &skeleton_shas, output.as_ref())?;
    let size = std::fs::metadata(output.as_ref())?.len() as usize;
    Ok(size)
}

/// Build a skeleton pack and store it in the CAS.
pub fn make_skeleton_pack_in_cas<P: AsRef<Path>>(
    bare_repo: P,
    commit: &str,
    cas: &Cas,
) -> Result<(String, usize, usize)> {
    let tmp = tempfile::NamedTempFile::new()?;
    let size = make_skeleton_pack(&bare_repo, commit, tmp.path())?;
    let data = std::fs::read(tmp.path())?;
    let pack_hash = cas.put(&data)?;
    let object_count = data.len(); // placeholder; will compute properly below
    Ok((pack_hash, size, object_count))
}

/// Analyze skeleton packs for the last N commits.
pub fn analyze_skeletons<P: AsRef<Path>>(bare_repo: P, count: usize) -> Result<()> {
    let branch = git::default_branch(&bare_repo)?;
    let commits = git::last_commits(&bare_repo, &branch, count)?;
    println!(
        "Analyzing skeletons for last {} commits on {}...",
        commits.len(),
        branch
    );

    // Phase 1: collect all reachable SHAs per commit and all unique SHAs.
    let mut per_commit_shas: Vec<Vec<String>> = Vec::new();
    let mut all_shas: HashSet<String> = HashSet::new();
    println!("  listing objects per commit...");
    for commit in &commits {
        let shas = git::list_object_shas(&bare_repo, commit)?;
        all_shas.extend(shas.iter().cloned());
        per_commit_shas.push(shas);
    }
    println!("  {} unique objects to classify", all_shas.len());

    // Phase 2: batch classify all unique objects by type.
    let types = git::classify_objects(&bare_repo, &all_shas)?;

    // Phase 3: compute per-commit skeletons and sizes.
    let mut per_commit: Vec<(String, usize, usize, usize)> = Vec::new();
    let mut all_unique_objects: HashSet<String> = HashSet::new();
    let mut prev_objects: Option<HashSet<String>> = None;
    let mut total_shared_with_parent = 0usize;

    for (i, (commit, shas)) in commits.iter().zip(per_commit_shas.iter()).enumerate() {
        let start = Instant::now();
        let mut skeleton_shas: Vec<String> = Vec::new();
        let mut commit_count = 0usize;
        let mut tree_count = 0usize;
        for sha in shas {
            if let Some(obj_type) = types.get(sha) {
                if obj_type == "commit" || obj_type == "tree" {
                    skeleton_shas.push(sha.clone());
                    if obj_type == "commit" {
                        commit_count += 1;
                    } else {
                        tree_count += 1;
                    }
                }
            }
            all_unique_objects.insert(sha.clone());
        }

        let current_set: HashSet<String> = skeleton_shas.iter().cloned().collect();
        let new_vs_parent = if let Some(prev) = &prev_objects {
            current_set.difference(prev).count()
        } else {
            current_set.len()
        };
        total_shared_with_parent += current_set.len() - new_vs_parent;

        // Build pack to get size.
        let tmp = tempfile::NamedTempFile::new()?;
        git::pack_objects(&bare_repo, &skeleton_shas, tmp.path())?;
        let size = std::fs::metadata(tmp.path())?.len() as usize;
        let elapsed = start.elapsed();

        println!(
            "commit {:>3}/{} {}: pack={:>7} bytes, {} commits + {} trees, new-vs-parent={}, took {:?}",
            i + 1,
            commits.len(),
            &commit[..7],
            size,
            commit_count,
            tree_count,
            new_vs_parent,
            elapsed
        );

        per_commit.push((commit.clone(), size, skeleton_shas.len(), new_vs_parent));
        prev_objects = Some(current_set);
    }

    let total_full_duplication: usize = per_commit.iter().map(|(_, size, _, _)| size).sum();
    let total_objects_duplicated: usize = per_commit.iter().map(|(_, _, count, _)| count).sum();

    println!("\nSummary:");
    println!("  commits analyzed: {}", per_commit.len());
    println!("  total unique objects across all skeletons: {}", all_unique_objects.len());
    println!("  sum of per-commit skeleton sizes: {} bytes ({:.2} MB)",
        total_full_duplication,
        total_full_duplication as f64 / 1_048_576.0
    );
    println!("  sum of per-commit object counts: {}", total_objects_duplicated);
    println!("  average skeleton size: {:.2} KB",
        total_full_duplication as f64 / per_commit.len() as f64 / 1024.0
    );
    if per_commit.len() > 1 {
        println!("  objects shared with parent on average: {:.1}",
            total_shared_with_parent as f64 / (per_commit.len() - 1) as f64
        );
    }

    Ok(())
}
