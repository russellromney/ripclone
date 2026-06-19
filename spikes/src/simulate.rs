use anyhow::{Context, Result};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::cas::Cas;
use crate::git;

/// Simulate a lazy-blob clone.
/// Steps:
/// 1. Resolve commit and build skeleton pack.
/// 2. Store skeleton pack and all reachable blobs in CAS.
/// 3. Simulate client: fetch skeleton, unpack, fetch blobs for each file in list.
pub async fn run<P: AsRef<Path>>(
    bare_repo: P,
    branch: &str,
    file_list: P,
    cas_dir: P,
) -> Result<()> {
    let commit = git::resolve_commit(&bare_repo, branch)?;
    println!("Simulating clone for {} at {}", branch, &commit[..7]);

    let cas = Cas::new(&cas_dir)?;

    // --- Server side: populate CAS ---
    let server_start = Instant::now();

    // Build skeleton pack and store in CAS.
    let skeleton_tmp = tempfile::NamedTempFile::new()?;
    let skeleton_size = crate::skeleton::make_skeleton_pack(&bare_repo, &commit, skeleton_tmp.path())?;
    let skeleton_data = std::fs::read(skeleton_tmp.path())?;
    let skeleton_hash = cas.put(&skeleton_data)?;
    println!(
        "Server: skeleton pack stored in CAS: {} ({} bytes)",
        skeleton_hash, skeleton_size
    );

    // List all blobs and store in CAS.
    let objects = git::list_objects(&bare_repo, &commit)?;
    let mut blob_shas: Vec<String> = Vec::new();
    let mut blob_sizes: Vec<usize> = Vec::new();
    for (sha, obj_type) in objects {
        if obj_type == "blob" {
            let content = git::object_content(&bare_repo, &sha)?;
            cas.put_with_hash(&sha, &content)?;
            blob_shas.push(sha);
            blob_sizes.push(content.len());
        }
    }
    let total_blob_bytes: usize = blob_sizes.iter().sum();
    println!(
        "Server: stored {} blobs ({:.2} MB) in CAS",
        blob_shas.len(),
        total_blob_bytes as f64 / 1_048_576.0
    );
    println!("Server population took {:?}", server_start.elapsed());

    // --- Client side: fetch skeleton ---
    let client_start = Instant::now();

    let tmp_git_dir = tempfile::tempdir()?;
    std::fs::create_dir_all(tmp_git_dir.path().join("objects"))?;
    std::fs::create_dir_all(tmp_git_dir.path().join("refs"))?;

    let skeleton_fetch_start = Instant::now();
    let skeleton_data = cas.get(&skeleton_hash)?;
    let skeleton_fetch_time = skeleton_fetch_start.elapsed();

    git::unpack_pack(tmp_git_dir.path(), skeleton_tmp.path())?;

    // Set HEAD.
    std::fs::write(tmp_git_dir.path().join("HEAD"), format!("{}\n", commit))?;

    println!(
        "Client: fetched skeleton ({} bytes) in {:?}",
        skeleton_data.len(),
        skeleton_fetch_time
    );

    // --- Client side: fetch blobs for file list ---
    let file_list_file = std::fs::File::open(file_list.as_ref())?;
    let files: Vec<String> = std::io::BufReader::new(file_list_file)
        .lines()
        .filter_map(|l| l.ok())
        .filter(|l| !l.trim().is_empty())
        .collect();

    println!("Agent wants to read {} files", files.len());

    let blob_fetch_start = Instant::now();
    let mut fetched_blobs = 0usize;
    let mut fetched_blob_bytes = 0usize;
    let mut missing = 0usize;

    for path in &files {
        match git::tree_entry(&bare_repo, &commit, path) {
            Ok(Some((_mode, sha))) => {
                if cas.has(&sha) {
                    let content = cas.get(&sha)?;
                    fetched_blob_bytes += content.len();
                    fetched_blobs += 1;
                } else {
                    missing += 1;
                }
            }
            Ok(None) => {
                missing += 1;
            }
            Err(e) => {
                eprintln!("warning: could not resolve {}: {}", path, e);
                missing += 1;
            }
        }
    }
    let blob_fetch_time = blob_fetch_start.elapsed();

    println!(
        "Client: fetched {} blobs ({} missing, {:.2} MB) in {:?}",
        fetched_blobs,
        missing,
        fetched_blob_bytes as f64 / 1_048_576.0,
        blob_fetch_time
    );

    let total_client_time = client_start.elapsed();
    println!(
        "Client total: {:?}, bytes transferred: {} (skeleton) + {} (blobs) = {} bytes ({:.2} MB)",
        total_client_time,
        skeleton_size,
        fetched_blob_bytes,
        skeleton_size + fetched_blob_bytes,
        (skeleton_size + fetched_blob_bytes) as f64 / 1_048_576.0
    );

    // Compare to a shallow git clone with blob:none.
    println!("\nComparison baseline: git clone --depth=1 --filter=blob:none");
    let baseline_start = Instant::now();
    let baseline_dir = tempfile::tempdir()?;
    let status = tokio::process::Command::new("git")
        .args([
            "clone",
            "--depth=1",
            "--filter=blob:none",
            "--single-branch",
            "--branch",
            &git::default_branch(&bare_repo)?,
            &format!("file://{}", bare_repo.as_ref().canonicalize()?.display()),
            &baseline_dir.path().to_string_lossy(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .context("git clone baseline")?;
    if !status.success() {
        eprintln!("warning: baseline git clone failed");
    }
    let baseline_time = baseline_start.elapsed();
    println!("Baseline shallow clone (blob:none) took {:?}", baseline_time);

    Ok(())
}
