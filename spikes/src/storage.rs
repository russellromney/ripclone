use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use crate::git;

/// Analyze storage models for the last N commits.
pub fn analyze<P: AsRef<Path>>(bare_repo: P, count: usize) -> Result<()> {
    let branch = git::default_branch(&bare_repo)?;
    let commits = git::last_commits(&bare_repo, &branch, count)?;
    println!(
        "Analyzing storage models for last {} commits on {}...",
        commits.len(),
        branch
    );

    let start = Instant::now();

    // Model 1: perfect sharing — unique raw objects across all N commits.
    let mut all_objects: HashSet<String> = HashSet::new();
    let mut commit_only_objects: HashSet<String> = HashSet::new();
    let mut tree_only_objects: HashSet<String> = HashSet::new();

    // Model 2 & 3: per-commit skeletons.
    let mut full_skeleton_sizes: Vec<usize> = Vec::new();
    let mut delta_skeleton_sizes: Vec<usize> = Vec::new();

    // Model 4: full working-tree pack per commit.
    let mut full_head_sizes: Vec<usize> = Vec::new();

    let mut prev_skeleton: Option<HashSet<String>> = None;

    for (i, commit) in commits.iter().enumerate() {
        let objects = git::list_objects(&bare_repo, commit)?;
        let mut skeleton_shas: Vec<String> = Vec::new();
        let mut head_shas: Vec<String> = Vec::new();

        for (sha, obj_type) in &objects {
            all_objects.insert(sha.clone());
            head_shas.push(sha.clone());
            if obj_type == "commit" || obj_type == "tree" {
                skeleton_shas.push(sha.clone());
                if obj_type == "commit" {
                    commit_only_objects.insert(sha.clone());
                } else {
                    tree_only_objects.insert(sha.clone());
                }
            }
        }

        // Full skeleton pack.
        let tmp = tempfile::NamedTempFile::new()?;
        git::pack_objects(&bare_repo, &skeleton_shas, tmp.path())?;
        full_skeleton_sizes.push(std::fs::metadata(tmp.path())?.len() as usize);

        // Full HEAD pack (skeleton + blobs).
        let tmp2 = tempfile::NamedTempFile::new()?;
        git::pack_objects(&bare_repo, &head_shas, tmp2.path())?;
        full_head_sizes.push(std::fs::metadata(tmp2.path())?.len() as usize);

        // Delta skeleton pack: objects not in parent skeleton.
        let current_skeleton: HashSet<String> = skeleton_shas.iter().cloned().collect();
        if let Some(prev) = &prev_skeleton {
            let delta: Vec<String> = current_skeleton
                .difference(prev)
                .cloned()
                .collect();
            if !delta.is_empty() {
                let tmp3 = tempfile::NamedTempFile::new()?;
                git::pack_objects(&bare_repo, &delta, tmp3.path())?;
                delta_skeleton_sizes.push(std::fs::metadata(tmp3.path())?.len() as usize);
            } else {
                delta_skeleton_sizes.push(0);
            }
        } else {
            delta_skeleton_sizes.push(full_skeleton_sizes.last().copied().unwrap_or(0));
        }

        prev_skeleton = Some(current_skeleton);

        if (i + 1) % 10 == 0 {
            println!("  processed {}/{}", i + 1, commits.len());
        }
    }

    // Model 1 size: pack all unique objects into one pack.
    let unique_objects: Vec<String> = all_objects.into_iter().collect();
    let tmp = tempfile::NamedTempFile::new()?;
    git::pack_objects(&bare_repo, &unique_objects, tmp.path())?;
    let perfect_size = std::fs::metadata(tmp.path())?.len() as usize;

    let full_skeleton_total: usize = full_skeleton_sizes.iter().sum();
    let delta_skeleton_total: usize = delta_skeleton_sizes.iter().sum();
    let full_head_total: usize = full_head_sizes.iter().sum();

    println!("\nStorage model comparison:");
    println!(
        "  (a) Perfect sharing (all unique objects, one pack): {:.2} MB",
        perfect_size as f64 / 1_048_576.0
    );
    println!(
        "  (b) Full skeleton per commit:          {:.2} MB ({}× perfect)",
        full_skeleton_total as f64 / 1_048_576.0,
        full_skeleton_total as f64 / perfect_size as f64
    );
    println!(
        "  (c) Delta skeleton per commit:         {:.2} MB ({}× perfect)",
        delta_skeleton_total as f64 / 1_048_576.0,
        delta_skeleton_total as f64 / perfect_size as f64
    );
    println!(
        "  (d) Full working tree pack per commit: {:.2} MB ({}× perfect)",
        full_head_total as f64 / 1_048_576.0,
        full_head_total as f64 / perfect_size as f64
    );
    println!(
        "  Unique commit objects: {}, unique tree objects: {}",
        commit_only_objects.len(),
        tree_only_objects.len()
    );
    println!("Analysis took {:?}", start.elapsed());

    Ok(())
}
