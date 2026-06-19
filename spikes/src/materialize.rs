use anyhow::{Context, Result};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::cas::Cas;
use crate::git;

/// Materialize a working tree from a skeleton pack + CAS.
pub fn run<P: AsRef<Path>>(
    skeleton_pack: P,
    cas_dir: P,
    file_list: P,
    output: P,
) -> Result<()> {
    let cas = Cas::new(&cas_dir)?;
    let output = output.as_ref();
    std::fs::create_dir_all(output)?;

    let start = Instant::now();

    // Set up minimal git dir.
    let git_dir = output.join(".git");
    std::fs::create_dir_all(git_dir.join("objects"))?;
    std::fs::create_dir_all(git_dir.join("refs"))?;

    // Unpack skeleton.
    git::unpack_pack(&git_dir, skeleton_pack.as_ref())?;

    // Read HEAD commit from skeleton? We need to know it. The skeleton pack contains
    // commits; find the first commit object.
    let commit = find_commit_in_pack(skeleton_pack.as_ref(), &git_dir)?;
    std::fs::write(git_dir.join("HEAD"), format!("{}\n", commit))?;

    println!("Unpacked skeleton for commit {} in {:?}", &commit[..7], start.elapsed());

    // Read file list.
    let file_list_file = std::fs::File::open(file_list.as_ref())?;
    let files: Vec<String> = std::io::BufReader::new(file_list_file)
        .lines()
        .filter_map(|l| l.ok())
        .filter(|l| !l.trim().is_empty())
        .collect();

    // Materialize files.
    let mat_start = Instant::now();
    let mut written = 0usize;
    let mut written_bytes = 0usize;

    for path in &files {
        match git::tree_entry(&git_dir, &commit, path) {
            Ok(Some((mode, sha))) => {
                let content = cas.get(&sha)?;
                let target = output.join(path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&target, &content)?;
                // Set executable bit if mode indicates.
                if mode == "100755" {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mut perms = std::fs::metadata(&target)?.permissions();
                        perms.set_mode(0o755);
                        std::fs::set_permissions(&target, perms)?;
                    }
                }
                written += 1;
                written_bytes += content.len();
            }
            Ok(None) => {
                eprintln!("warning: {} not in tree", path);
            }
            Err(e) => {
                eprintln!("warning: could not resolve {}: {}", path, e);
            }
        }
    }

    let mat_time = mat_start.elapsed();
    println!(
        "Materialized {} files ({:.2} MB) in {:?}",
        written,
        written_bytes as f64 / 1_048_576.0,
        mat_time
    );
    println!("Total time: {:?}", start.elapsed());

    Ok(())
}

fn find_commit_in_pack<P: AsRef<Path>>(_pack: P, git_dir: P) -> Result<String> {
    // List all commit objects in the git dir and return the first one.
    let objects_dir = git_dir.as_ref().join("objects");
    for entry in walkdir::WalkDir::new(&objects_dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            let rel = path.strip_prefix(&objects_dir)?;
            let sha = rel
                .to_string_lossy()
                .replace(&std::path::MAIN_SEPARATOR.to_string(), "");
            if sha.len() == 40 {
                match git::object_type(&git_dir, &sha) {
                    Ok(t) if t == "commit" => return Ok(sha),
                    _ => {}
                }
            }
        }
    }
    anyhow::bail!("no commit object found in skeleton pack")
}
