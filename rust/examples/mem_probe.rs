//! Peak-RSS probe for the phase-2 archive build (`build_into_cas_incremental`).
//!
//! Builds a synthetic many-file repo and runs the streaming archive build, then
//! reports peak resident set size and the total compressed archive bytes. The
//! pre-fix build held that full compressed total resident at once (every frame's
//! `Vec<u8>` accumulated before the CAS puts); the streaming build keeps only
//! ~one batch resident. So the memory saved is ~`total_compressed_bytes`.
//!
//! Run: `cargo run --release --example mem_probe -- <files> <bytes_per_file>`
//! e.g. `cargo run --release --example mem_probe -- 4000 100000`

use ripclone::archive::ArchiveBuilder;
use ripclone::cas::Cas;
use std::collections::HashMap;
use std::process::Command;

fn peak_rss_bytes() -> u64 {
    // getrusage(RUSAGE_SELF).ru_maxrss — bytes on macOS, kibibytes on Linux.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return 0;
    }
    let raw = usage.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        raw
    } else {
        raw * 1024
    }
}

fn xorshift_fill(seed: u64, len: usize) -> Vec<u8> {
    let mut s = seed ^ 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 33) as u8
        })
        .collect()
}

fn git(dir: &std::path::Path, args: &[&str]) {
    let status = Command::new("git")
        .current_dir(dir)
        .args(args)
        .status()
        .expect("run git");
    assert!(status.success(), "git {:?} failed", args);
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n_files: usize = a.next().map(|s| s.parse().unwrap()).unwrap_or(4000);
    let bytes: usize = a.next().map(|s| s.parse().unwrap()).unwrap_or(100_000);

    let work = tempfile::tempdir().unwrap();
    git(work.path(), &["init", "-q"]);
    git(work.path(), &["config", "user.email", "p@p"]);
    git(work.path(), &["config", "user.name", "p"]);
    // High-entropy content so CDC cuts real frames and compression can't collapse
    // the archive to near-nothing (keeps the measurement honest).
    for i in 0..n_files {
        let sub = work.path().join(format!("d{:03}", i % 256));
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(
            sub.join(format!("f{i}.bin")),
            xorshift_fill(i as u64, bytes),
        )
        .unwrap();
    }
    git(work.path(), &["add", "-A"]);
    git(work.path(), &["commit", "-q", "-m", "big"]);
    let out = Command::new("git")
        .current_dir(work.path())
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    let commit = String::from_utf8(out.stdout).unwrap().trim().to_string();

    let raw_total = (n_files * bytes) as f64 / (1024.0 * 1024.0);
    let cas_dir = tempfile::tempdir().unwrap();
    let cas = Cas::new(cas_dir.path()).unwrap();
    let builder = ArchiveBuilder::new(work.path());
    let empty: HashMap<String, (String, u64)> = HashMap::new();

    let t = std::time::Instant::now();
    let result = builder
        .build_into_cas_incremental(&commit, &cas, None, 3, None, &empty, 4 * 1024 * 1024)
        .expect("archive build");
    let elapsed = t.elapsed();

    let total_compressed: u64 = result.archive_frames.iter().map(|f| f.compressed_len).sum();
    let peak = peak_rss_bytes();
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    println!("files={n_files} bytes_per_file={bytes} raw_worktree_MiB={raw_total:.1}");
    println!("frames={}", result.archive_frames.len());
    println!("total_compressed_MiB={:.1}", mib(total_compressed));
    println!("build_time={elapsed:?}");
    println!("PEAK_RSS_MiB={:.1}", mib(peak));
    println!(
        "pre-fix peak would additionally hold ~total_compressed = {:.1} MiB resident",
        mib(total_compressed)
    );
}
