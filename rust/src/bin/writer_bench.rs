//! Synthetic worktree-writer micro-benchmark.
//!
//! This isolates the *pure file-writing* cost that the per-clone benchmark
//! (`write_ms`) buries underneath archive download, zstd decompression, SHA-1
//! verification and the local blob-pack build. Here there is no network, no
//! decompression and no hashing: file contents are prepared in memory up front,
//! then handed to the real `WorktreeWriter::write_owned_entries` path — exactly
//! what the extractor calls per frame.
//!
//! It runs the POSIX backend and (on Linux, when available) the io_uring
//! backend against separate temp directories with an identical workload and
//! prints files/sec, MiB/sec and the prep / io / mtime breakdown collected by
//! `worktree_writer::take_write_timing`.
//!
//! Example:
//!   cargo run --release --bin writer_bench -- \
//!     --small 20000 --small-size 2048 --large 64 --large-size 1048576 \
//!     --threads 7 --batch 256 --backend both
//!
//! `--dir` selects the parent directory for the temp trees (default: the system
//! temp dir). Point it at the target filesystem (e.g. a Fly `/data` volume) to
//! measure the device that matters.

use anyhow::{Context, Result};
use ripclone::manifest::FileEntry;
use ripclone::worktree_writer::{
    OwnedFileWrite, SchedulerConfig, WorktreeWriteScheduler, WorktreeWriter, WriteOptions,
    take_write_timing,
};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Clone)]
struct Config {
    small: usize,
    small_size: usize,
    large: usize,
    large_size: usize,
    threads: usize,
    batch: usize,
    dirs: usize,
    backend: Backend,
    parent: PathBuf,
    runs: usize,
    stamp_mtime: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Backend {
    Posix,
    IoUring,
    Both,
    /// The shared write scheduler (submitter pool). Tuning knobs come from the
    /// `RIPCLONE_IO_URING_*` env vars, exactly as in a real clone.
    Scheduler,
}

fn main() -> Result<()> {
    let cfg = parse_args()?;

    let default_threads = std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).max(1))
        .unwrap_or(4);
    let threads = if cfg.threads == 0 {
        default_threads
    } else {
        cfg.threads
    };
    let cfg = Config { threads, ..cfg };

    println!(
        "workload: {} small files x {} B + {} large files x {} B across {} dirs\n\
         runner:   {} writer threads, batch {} files/call, {} run(s) per backend, stamp_mtime={}\n\
         target:   {}",
        cfg.small,
        cfg.small_size,
        cfg.large,
        cfg.large_size,
        cfg.dirs,
        cfg.threads,
        cfg.batch,
        cfg.runs,
        cfg.stamp_mtime,
        cfg.parent.display(),
    );

    // Build the workload once: (relative path bytes, mode, content). Content is
    // a deterministic fill so it compresses/writes like real bytes.
    let workload = build_workload(&cfg);
    let total_files = workload.len();
    let total_bytes: u64 = workload.iter().map(|w| w.2.len() as u64).sum();
    println!(
        "prepared {} files, {:.1} MiB total content\n",
        total_files,
        total_bytes as f64 / (1024.0 * 1024.0)
    );
    match cfg.backend {
        Backend::Posix => {
            run_backend("posix", &cfg, &workload, || Ok(WorktreeWriter::posix()))?;
        }
        Backend::IoUring => {
            run_backend("io_uring", &cfg, &workload, WorktreeWriter::io_uring)?;
        }
        Backend::Both => {
            run_backend("posix", &cfg, &workload, || Ok(WorktreeWriter::posix()))?;
            match run_backend("io_uring", &cfg, &workload, WorktreeWriter::io_uring) {
                Ok(()) => {}
                Err(e) => println!("io_uring backend unavailable, skipped: {e:#}"),
            }
        }
        Backend::Scheduler => {
            run_scheduler_backend(&cfg, &workload)?;
        }
    }

    Ok(())
}

type Workload = Vec<(Vec<u8>, u32, Vec<u8>)>;

fn build_workload(cfg: &Config) -> Workload {
    let mut out: Workload = Vec::with_capacity(cfg.small + cfg.large);
    let dirs = cfg.dirs.max(1);
    for i in 0..cfg.small {
        let path = format!("d{:03}/small_{:06}.txt", i % dirs, i);
        out.push((path.into_bytes(), 0o644, filled(cfg.small_size, i as u8)));
    }
    for i in 0..cfg.large {
        let path = format!("big/large_{:04}.bin", i);
        out.push((path.into_bytes(), 0o644, filled(cfg.large_size, i as u8)));
    }
    out
}

fn filled(len: usize, seed: u8) -> Vec<u8> {
    let mut v = vec![0u8; len];
    for (i, b) in v.iter_mut().enumerate() {
        *b = seed.wrapping_add((i & 0xff) as u8);
    }
    v
}

fn run_backend(
    label: &str,
    cfg: &Config,
    workload: &Workload,
    make_writer: impl Fn() -> Result<WorktreeWriter>,
) -> Result<()> {
    // Construct once up front so an unsupported backend fails before timing.
    let _probe = make_writer().with_context(|| format!("init {label} writer"))?;

    let total_files = workload.len();
    let total_bytes: u64 = workload.iter().map(|w| w.2.len() as u64).sum();

    let mut wall_ms = Vec::with_capacity(cfg.runs);
    let mut timings = Vec::with_capacity(cfg.runs);

    for run in 0..cfg.runs {
        let target = cfg
            .parent
            .join(format!("ripclone-writer-bench-{label}-{run}"));
        let _ = std::fs::remove_dir_all(&target);
        std::fs::create_dir_all(&target)
            .with_context(|| format!("create target {}", target.display()))?;

        // Pre-create every parent directory, exactly as the extractor does
        // up front, so this cost is excluded from the writer measurement.
        precreate_dirs(&target, workload)?;

        // Prepare all owned file buffers outside the timed region. The real
        // extractor currently pays this `to_vec` cost before calling the
        // writer, but this benchmark is intentionally isolating writer backend
        // behavior rather than buffer ownership.
        let batches = prepare_batches(workload, cfg);
        let writer = make_writer()?;
        // Reset counters just before the timed region.
        let _ = take_write_timing();

        let start = Instant::now();
        run_writers(&writer, &target, batches, cfg.stamp_mtime)?;
        let elapsed = start.elapsed();

        wall_ms.push(elapsed.as_secs_f64() * 1000.0);
        timings.push(take_write_timing());

        let _ = std::fs::remove_dir_all(&target);
    }

    let median_idx = median_index(&wall_ms);
    let wall = wall_ms.get(median_idx).copied().unwrap_or(0.0);
    let secs = wall / 1000.0;
    let mib = total_bytes as f64 / (1024.0 * 1024.0);

    // Sum the breakdown across all writer threads of the median wall-time run.
    let t = timings.get(median_idx).copied().unwrap_or_default();
    let prep_ms = t.prep_ns as f64 / 1e6;
    let io_ms = t.io_ns as f64 / 1e6;
    let mtime_ms = t.mtime_ns as f64 / 1e6;
    let thread_total = prep_ms + io_ms + mtime_ms;

    println!("=== {label} ===");
    println!(
        "wall {:.1} ms  |  {:.0} files/s  |  {:.1} MiB/s",
        wall,
        total_files as f64 / secs,
        mib / secs,
    );
    println!(
        "thread-time breakdown (summed over {} threads, so > wall):",
        cfg.threads
    );
    println!(
        "  prep   {:>8.1} ms  ({:>4.1}%)  — validate + dir + symlink/exists probes",
        prep_ms,
        pct(prep_ms, thread_total),
    );
    println!(
        "  io     {:>8.1} ms  ({:>4.1}%)  — open/write/close only",
        io_ms,
        pct(io_ms, thread_total),
    );
    println!(
        "  mtime  {:>8.1} ms  ({:>4.1}%)  — serial utimensat per file",
        mtime_ms,
        pct(mtime_ms, thread_total),
    );
    println!();
    Ok(())
}

/// Drive the shared write scheduler the way the extractor does: one scheduler,
/// `threads` producer threads each preparing work and calling `submit`, then a
/// single `flush`. Tuning comes from `SchedulerConfig::from_env`.
fn run_scheduler_backend(cfg: &Config, workload: &Workload) -> Result<()> {
    let sched_cfg = SchedulerConfig::from_env();
    let total_files = workload.len();
    let total_bytes: u64 = workload.iter().map(|w| w.2.len() as u64).sum();

    let mut wall_ms = Vec::with_capacity(cfg.runs);
    let mut timings = Vec::with_capacity(cfg.runs);

    for run in 0..cfg.runs {
        let target = cfg
            .parent
            .join(format!("ripclone-writer-bench-scheduler-{run}"));
        let _ = std::fs::remove_dir_all(&target);
        std::fs::create_dir_all(&target)
            .with_context(|| format!("create target {}", target.display()))?;
        precreate_dirs(&target, workload)?;

        let batches = prepare_batches(workload, cfg);
        let options = WriteOptions {
            parents_prepared: true,
            stamp_mtime: cfg.stamp_mtime,
            fresh_target: false,
        };
        let scheduler = WorktreeWriteScheduler::with_config(target.clone(), options, sched_cfg)?;
        let _ = take_write_timing();

        let start = Instant::now();
        std::thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            for thread_batches in batches {
                let scheduler = &scheduler;
                handles.push(scope.spawn(move || -> Result<()> {
                    for writes in thread_batches {
                        scheduler.submit(writes)?;
                    }
                    Ok(())
                }));
            }
            for h in handles {
                h.join().expect("producer thread panicked")?;
            }
            Ok(())
        })?;
        scheduler.flush()?;
        let elapsed = start.elapsed();

        wall_ms.push(elapsed.as_secs_f64() * 1000.0);
        timings.push(take_write_timing());
        let _ = std::fs::remove_dir_all(&target);
    }

    let median_idx = median_index(&wall_ms);
    let wall = wall_ms.get(median_idx).copied().unwrap_or(0.0);
    let secs = wall / 1000.0;
    let mib = total_bytes as f64 / (1024.0 * 1024.0);
    let t = timings.get(median_idx).copied().unwrap_or_default();

    println!("=== scheduler ===");
    println!(
        "config:   submitters={}, inflight={}, batch_files={}, byte_cap={} B, flush={:?}",
        sched_cfg.submitters,
        sched_cfg.inflight,
        sched_cfg.batch_files,
        sched_cfg.byte_cap,
        sched_cfg.flush_timeout,
    );
    println!(
        "wall {:.1} ms  |  {:.0} files/s  |  {:.1} MiB/s",
        wall,
        total_files as f64 / secs,
        mib / secs,
    );
    println!(
        "io thread-time {:.1} ms across submitters (prep not separately tracked here)\n",
        t.io_ns as f64 / 1e6,
    );
    Ok(())
}

fn precreate_dirs(target: &Path, workload: &Workload) -> Result<()> {
    use std::collections::HashSet;
    let mut seen: HashSet<&[u8]> = HashSet::new();
    for (path, _, _) in workload.iter() {
        if let Some(slash) = path.iter().rposition(|&b| b == b'/') {
            let dir = &path[..slash];
            if seen.insert(dir) {
                let rel = ripclone::worktree_writer::path_from_bytes(dir);
                ripclone::worktree_writer::safe_create_dir_all(target, rel)?;
            }
        }
    }
    Ok(())
}

fn prepare_batches(workload: &Workload, cfg: &Config) -> Vec<Vec<Vec<OwnedFileWrite>>> {
    let n = workload.len();
    let threads = cfg.threads.min(n.max(1));
    let per = n.div_ceil(threads);
    let batch = cfg.batch.max(1);

    let mut out = Vec::with_capacity(threads);
    for t in 0..threads {
        let lo = t * per;
        if lo >= n {
            break;
        }
        let hi = (lo + per).min(n);
        let mut thread_batches = Vec::new();
        let mut i = lo;
        while i < hi {
            let end = (i + batch).min(hi);
            let mut writes = Vec::with_capacity(end - i);
            for (path, mode, content) in &workload[i..end] {
                writes.push(OwnedFileWrite {
                    entry: FileEntry {
                        path: path.clone(),
                        mode: 0o100000 | *mode,
                        blob_sha1: Vec::new(),
                        fragments: Vec::new(),
                    },
                    content: content.clone().into(),
                });
            }
            thread_batches.push(writes);
            i = end;
        }
        out.push(thread_batches);
    }
    out
}

fn run_writers(
    writer: &WorktreeWriter,
    target: &Path,
    batches: Vec<Vec<Vec<OwnedFileWrite>>>,
    stamp_mtime: bool,
) -> Result<()> {
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for thread_batches in batches {
            let target = target.to_path_buf();
            handles.push(scope.spawn(move || -> Result<()> {
                for writes in thread_batches {
                    writer.write_owned_entries_with_options(
                        &target,
                        writes,
                        WriteOptions {
                            parents_prepared: true,
                            stamp_mtime,
                            ..WriteOptions::default()
                        },
                    )?;
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join().expect("writer thread panicked")?;
        }
        Ok(())
    })
}

fn median_index(xs: &[f64]) -> usize {
    if xs.is_empty() {
        return 0;
    }
    let mut idxs: Vec<_> = (0..xs.len()).collect();
    idxs.sort_by(|&a, &b| xs[a].partial_cmp(&xs[b]).unwrap());
    idxs[idxs.len() / 2]
}

fn pct(part: f64, total: f64) -> f64 {
    if total <= 0.0 {
        0.0
    } else {
        part / total * 100.0
    }
}

fn parse_args() -> Result<Config> {
    let mut cfg = Config {
        small: 20_000,
        small_size: 2048,
        large: 64,
        large_size: 1024 * 1024,
        threads: 0,
        batch: 256,
        dirs: 200,
        backend: Backend::Both,
        parent: std::env::temp_dir(),
        runs: 3,
        stamp_mtime: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || -> Result<String> {
            args.next()
                .ok_or_else(|| anyhow::anyhow!("missing value for {arg}"))
        };
        match arg.as_str() {
            "--small" => cfg.small = value()?.parse()?,
            "--small-size" => cfg.small_size = value()?.parse()?,
            "--large" => cfg.large = value()?.parse()?,
            "--large-size" => cfg.large_size = value()?.parse()?,
            "--threads" => cfg.threads = value()?.parse()?,
            "--batch" => cfg.batch = value()?.parse()?,
            "--dirs" => cfg.dirs = value()?.parse()?,
            "--runs" => cfg.runs = value()?.parse::<usize>()?.max(1),
            "--stamp-mtime" => cfg.stamp_mtime = true,
            "--dir" => cfg.parent = PathBuf::from(value()?),
            "--backend" => {
                cfg.backend = match value()?.as_str() {
                    "posix" => Backend::Posix,
                    "iouring" | "io_uring" => Backend::IoUring,
                    "both" => Backend::Both,
                    "scheduler" | "sched" => Backend::Scheduler,
                    other => anyhow::bail!("unknown backend {other}"),
                }
            }
            "-h" | "--help" => {
                println!(
                    "writer_bench [--small N] [--small-size B] [--large N] [--large-size B]\n\
                     [--threads T] [--batch N] [--dirs N] [--runs N] [--dir PATH]\n\
                     [--stamp-mtime]\n\
                     [--backend posix|iouring|both]"
                );
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument {other}"),
        }
    }
    Ok(cfg)
}
