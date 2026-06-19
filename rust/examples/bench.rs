use ripclone::cas::{Cas, hash as cas_hash};
use ripclone::manifest::MetadataChunk;
use ripclone::server::RateLimiter;
use ripclone::validation;
use std::path::Path;
use std::time::Instant;

fn now() -> Instant {
    Instant::now()
}

fn throughput(bytes: u64, elapsed: std::time::Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        0.0
    } else {
        (bytes as f64 / 1_048_576.0) / secs
    }
}

fn bench_cas(size: usize) {
    let tmp = tempfile::tempdir().unwrap();
    let cas = Cas::new(tmp.path()).unwrap();
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let hash = cas_hash(&data);

    // Warm up + one measured write.
    let start = now();
    cas.put_with_hash(&hash, &data).unwrap();
    let write_elapsed = start.elapsed();

    // Read through the CAS (validates SHA-256 of the whole object).
    let start = now();
    let read = cas.get(&hash).unwrap();
    assert_eq!(read.len(), data.len());
    let get_elapsed = start.elapsed();

    // Raw filesystem read of the same bytes (no hash).
    let path = cas.path(&hash);
    let start = now();
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(raw.len(), data.len());
    let raw_elapsed = start.elapsed();

    println!(
        "CAS {:>6}  write {:>8.2} ms ({:>7.1} MB/s)  get {:>8.2} ms ({:>7.1} MB/s)  raw read {:>8.2} ms ({:>7.1} MB/s)  hash overhead ~{:.1}%",
        human_size(size as u64),
        write_elapsed.as_secs_f64() * 1000.0,
        throughput(size as u64, write_elapsed),
        get_elapsed.as_secs_f64() * 1000.0,
        throughput(size as u64, get_elapsed),
        raw_elapsed.as_secs_f64() * 1000.0,
        throughput(size as u64, raw_elapsed),
        if get_elapsed.as_secs_f64() > 0.0 {
            ((get_elapsed.as_secs_f64() - raw_elapsed.as_secs_f64()) / get_elapsed.as_secs_f64()) * 100.0
        } else {
            0.0
        }
    );
}

fn human_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn bench_manifest_validation(file_count: usize) {
    let mut manifest = MetadataChunk::new();
    let frame = ripclone::clonepack::FrameInfo {
        chunk_index: 0,
        chunk_offset: 0,
        compressed_len: 1024,
        raw_len: 1024,
    };
    manifest.frames.push(frame);

    let sha1 = [0u8; 20];
    for i in 0..file_count {
        let path = format!("src/components/widget-{}/deep/nested/file.txt", i);
        manifest.files.push(ripclone::clonepack::FileEntry {
            path: path.into_bytes(),
            mode: 0o100644,
            blob_sha1: sha1.to_vec(),
            fragments: vec![ripclone::clonepack::Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 1024,
            }],
        });
    }

    let iters = 10;
    let start = now();
    for _ in 0..iters {
        manifest.validate_geometry().unwrap();
    }
    let elapsed = start.elapsed();
    println!(
        "manifest geometry validation  files={:<8}  {:>8.2} us/iter  ({:.1} files/us)",
        file_count,
        elapsed.as_micros() as f64 / iters as f64,
        file_count as f64 / (elapsed.as_micros() as f64 / iters as f64)
    );
}

fn bench_path_validation(count: usize) {
    let paths: Vec<&Path> = (0..count)
        .map(|i| {
            let s = format!("src/foo-{}/bar/baz.txt", i % 1000);
            Path::new(Box::leak(s.into_boxed_str()))
        })
        .collect();
    let start = now();
    for p in &paths {
        ripclone::extract::validate_relative_path(p).unwrap();
    }
    let elapsed = start.elapsed();
    println!(
        "relative path validation      count={:<8}  {:>8.2} ns/op",
        count,
        elapsed.as_nanos() as f64 / count as f64
    );
}

fn bench_artifact_id_validation(count: usize) {
    let id = "a".repeat(64);
    let start = now();
    for _ in 0..count {
        Cas::validate_artifact_id(&id).unwrap();
    }
    let elapsed = start.elapsed();
    println!(
        "artifact id validation        count={:<8}  {:>8.2} ns/op",
        count,
        elapsed.as_nanos() as f64 / count as f64
    );
}

fn bench_git_rev_validation(count: usize) {
    let rev = "refs/heads/main";
    let start = now();
    for _ in 0..count {
        validation::validate_git_rev(rev).unwrap();
    }
    let elapsed = start.elapsed();
    println!(
        "git ref validation            count={:<8}  {:>8.2} ns/op",
        count,
        elapsed.as_nanos() as f64 / count as f64
    );
}

fn bench_rate_limiter(count: usize) {
    let limiter = RateLimiter::new(count as u32 + 1_000, 1_000_000.0);
    let key = "192.0.2.1";
    let start = now();
    for _ in 0..count {
        assert!(limiter.check(key));
    }
    let elapsed = start.elapsed();
    println!(
        "rate limiter check            count={:<8}  {:>8.2} ns/op",
        count,
        elapsed.as_nanos() as f64 / count as f64
    );
}

fn main() {
    println!("Ripclone security-fix performance micro-benchmarks\n");

    println!("--- CAS integrity overhead (atomic write + read hashing) ---");
    for size in &[1024, 1024 * 1024, 64 * 1024 * 1024, 256 * 1024 * 1024] {
        bench_cas(*size);
    }

    println!("\n--- Request/path validation overhead ---");
    bench_artifact_id_validation(1_000_000);
    bench_git_rev_validation(1_000_000);
    bench_path_validation(1_000_000);
    bench_rate_limiter(1_000_000);

    println!("\n--- Manifest geometry validation overhead ---");
    bench_manifest_validation(10_000);
    bench_manifest_validation(100_000);
}
