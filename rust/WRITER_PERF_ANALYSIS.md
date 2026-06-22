# Worktree writer performance: io_uring vs POSIX

Adversarial analysis of why io_uring does not clearly beat POSIX in the clone
benchmark, with code references, isolation tooling, and ranked next steps.

## TL;DR

1. The clone benchmark **cannot** show a writer win. `write_ms` measures the
   whole extract pipeline (decompress + SHA-1 + blob-pack + writes), and pure
   file I/O is a minority of it. POSIX and io_uring differ only in the I/O slice.
2. The optimized archive path now removes the biggest shared floors from the
   writer: regular-file `utimensat` is replaced by index stat-cache refresh,
   parent directory walks are skipped after the pre-create pass, and
   single-fragment files borrow frame slices instead of copying to a new `Vec`.
3. The io_uring backend now has a conservative adaptive policy: batches of one
   regular file fall back to the POSIX writer, while normal archive batches keep
   the batched ring path.
4. New tool `writer_bench` isolates pure writer time and prints a
   prep / io / mtime split per backend. On Fly `/data`, io_uring is a clear win
   in the isolated single-thread batched small-file case, but the win is mostly
   hidden in the real clone benchmark by prep, mtime, decompression, hashing, and
   pack work.

## 1. The benchmark conflates writing with CPU work

`bench.rs:12` defines `write_ms` as "working-tree extraction + local blob pack
build", set by `mark_write()` (`bench.rs:101`). Everything the writer threads do
in `extract.rs` lands in that window, per frame
(`extract.rs:384-520`):

- zstd decompression — `extract.rs:406` `zstd::decode_all`
- SHA-1 verify of every blob — `extract.rs:440` `Sha1::digest`
- blob-pack send; the builder thread then zlib-compresses every blob into the
  packfile (`extract.rs:447-456`), contending for the same cores
- `entry.clone()` + `content.to_vec()` — `extract.rs:458-461`
- only then the actual writer — `write_owned_entries`

So comparing backends through `write_ms` mostly compares shared zstd/SHA-1/zlib
CPU work. The observed Fly deltas (extractor 542 vs 573 ms; bench-write 755 vs
722 ms) are within the noise of that shared work. That is the root cause of "io_uring
is not clearly beating POSIX": **the writer is not the bottleneck this number
measures.**

## 2. Logic differences between POSIX and io_uring

Shared, identical for both backends (`worktree_writer.rs:198-264`):

- path validation — `:211`
- per-file `safe_create_dir_all` is skipped in archive extraction via
  `write_owned_entries_for_archive`; `extract.rs` already pre-creates every
  parent directory with `safe_create_dir_all`.
- per-file `target.is_symlink()` probe — `:226`
- symlink write, mode mapping — identical

Differences (`write_regular_batch`, `worktree_writer.rs:326-365`):

| Step | POSIX | io_uring |
|------|-------|----------|
| open/write/close | std `OpenOptions`+`write_all`, `O_NOFOLLOW`, per file (`:393`) | batched `openat→write→close` chains, fixed-file or normal fd |
| extra `exists()` | YES — `write_regular_posix:394` re-stats after `:237` already removed | NO — relies on `O_TRUNC` |
| mtime | skipped in archive extraction; old behavior available in `writer_bench --stamp-mtime` | skipped in archive extraction; old behavior available in `writer_bench --stamp-mtime` |

The old mtime loop was the largest avoidable floor for io_uring: the ring
collapsed open+write+close for a whole 256-file window, then issued **N serial
`utimensat` syscalls**. Archive extraction now skips that loop and refreshes the
git index stat cache while clearing skip-worktree.

## 3. io_uring implementation review

- **Concurrency within a window:** correct. Each file is its own
  `IO_LINK`/`IO_HARDLINK` chain (open→write→close); different files' chains are
  independent so the kernel runs them in parallel. One submit per window
  (`:774`, `:642/684`).
- **Batch size:** window cap is 256 (`MAX_BATCH_FILES`, `:434`), but the input
  batch is "files in one decompressed frame" — `extract.rs` calls
  `write_owned_entries` once per frame. Frames are ~6 MiB raw
  (`archive.rs:11` `FRAME_TARGET`). Small-file repos → hundreds of files/frame
  (good). Files > 6 MiB are split one-per-frame (`archive.rs:194`) → **batch of
  1**, where io_uring is pure overhead vs a plain write. The io_uring backend
  now routes these one-file batches through the POSIX writer.
- **Waiting:** `collect_completions` drains with `submit_and_wait(1)` (`:845`) —
  fine, not over-waiting.
- **Registration cost:** `register_files_sparse(256)` runs once per writer
  thread (thread-local `RawUringWriter`, `:471`), not per batch. Good.
- **Close serialization:** closes ride in the same batch; not serialized.
- **O_CLOEXEC stripped on direct open** (`:745`): a necessary Fly workaround;
  fd is closed in the same chain, so it is safe.

No correctness or obvious concurrency bug in the ring itself. The problem is
amortization: per-file mtime + redundant prep stats dominate once the batched
I/O is fast.

## 4. Isolation tooling added

- `worktree_writer::take_write_timing()` + process-wide counters split writer
  cost into **prep / io / mtime** (`worktree_writer.rs`, top). Recording is a few
  atomic adds per batch (never per file), always on.
- `src/bin/writer_bench.rs`: synthetic, no network/decompress/hash. Prepares file
  contents in memory, runs POSIX and io_uring against separate temp dirs with an
  identical workload (many small files + a few large), reports files/s, MiB/s and
  the prep/io/mtime split. Default matches the optimized archive path
  (`stamp_mtime=false`); pass `--stamp-mtime` to measure the old mtime loop.

Run on the device that matters:

```
cargo build --release --bin writer_bench
./target/release/writer_bench --dir /data \
  --small 20000 --small-size 2048 --large 64 --large-size 1048576 \
  --threads 7 --batch 256 --backend both
```

### Current optimized path: Fly `/data` ext4 volume, 8 performance CPUs, 16 GB

Optimizations included:

- skip per-file parent-dir walks after the safe pre-create pass
- skip regular-file `utimensat`; refresh index stat cache while clearing
  skip-worktree
- avoid copying single-fragment files out of the decompressed frame

Mixed Bun-ish workload: 20000x2KB + 64x1MiB, 7 threads, batch 256, 5 runs:

```
posix:    wall 161.7 ms | 124082 files/s |  637.4 MiB/s
  prep  95.6 ms | io 488.8 ms | mtime 0.0 ms

io_uring: wall  97.4 ms | 205968 files/s | 1058.0 MiB/s
  prep 148.7 ms | io 432.2 ms | mtime 0.0 ms
```

io_uring is ~40% faster wall-clock than POSIX on the optimized mixed workload.

Small files only: 20000x2KB, 7 threads, batch 256, 5 runs:

```
posix:    wall 104.3 ms | io 479.6 ms
io_uring: wall 108.5 ms | io 459.9 ms
```

At high parallelism, small-file-only remains effectively tied. The mixed
workload benefits more because io_uring handles the small-file syscall pressure
while still moving larger buffered writes efficiently.

Old mtime path comparison, same mixed workload with `--stamp-mtime`:

```
posix:    wall 172.2 ms | mtime 81.6 ms
io_uring: wall 133.5 ms | mtime 89.7 ms
```

Removing the regular-file mtime loop improved this Fly run from 133.5 ms to
97.4 ms for io_uring.

### Rebased main: editable pack materialization

After rebasing onto `main`, the default editable clone path no longer rebuilds
blob packs locally. It downloads prebuilt editable packs, installs them into
`.git/objects`, and materializes the working tree by parsing HEAD packs. The
pack parser now batches file writes through `WorktreeWriter`, so the io_uring
backend is exercised on the default editable path.

Target: `oven-sh/bun` at
`88417471cb28aab8943eb6227c014ac3f1c50cbc`, synced immediately before the run.
Client ran on a one-off Fly machine with 8 performance CPUs, 16 GB RAM,
`--rm --restart no --autostop=stop --autostart=false`, and a fresh ext4 volume
mounted at `/data`. Overlay staging was disabled with `RIPCLONE_NO_OVERLAY=1`.

Command shape:

```
RIPCLONE_NO_OVERLAY=1 RIPCLONE_BENCH=1 \
RIPCLONE_FETCH_CONCURRENCY=<n> RIPCLONE_WRITE_THREADS=<n> \
RIPCLONE_IO_URING=<0|1> \
  ripclone --server https://ripclone.fly.dev clone oven-sh/bun \
  --dir /data/bun-<label> --mode editable
```

Results:

```
posix  fetch=16 write=16: total 2344 ms | write 1387 ms | real 2.40 s
uring  fetch=16 write=16: total 1368 ms | write 1187 ms | real 1.38 s

posix  fetch=8  write=8:  total 1667 ms | write 1494 ms | real 1.68 s
uring  fetch=8  write=8:  total 1518 ms | write 1328 ms | real 1.53 s

posix  fetch=4  write=4:  total 1729 ms | write 1549 ms | real 1.74 s
uring  fetch=4  write=4:  total 1574 ms | write 1437 ms | real 1.59 s

posix  fetch=8  write=4:  total 1624 ms | write 1370 ms | real 1.64 s
uring  fetch=8  write=4:  total 1481 ms | write 1323 ms | real 1.50 s
```

All runs had a clean `git status` and installed/extracted 33 editable packs
(68.6 MB downloaded). io_uring direct descriptors were enabled in every io_uring
run. In this rebased path, io_uring won every matched concurrency point on
`write_ms`; the best observed setting was the default 16/16 io_uring run at
`1187 ms` write time.

### Full Bun clone after optimized writer changes

Target: `oven-sh/bun` at
`88417471cb28aab8943eb6227c014ac3f1c50cbc`, synced to the Fly server before
the run. Client ran on a one-off Fly machine with 8 performance CPUs, 16 GB RAM,
`--rm --restart no --autostop=stop --autostart=false`, and a fresh ext4 volume
mounted at `/data`. Overlay staging was disabled with `RIPCLONE_NO_OVERLAY=1`,
so worktree and local blob-pack writes went directly to `/data`.

Command shape:

```
RIPCLONE_NO_OVERLAY=1 RIPCLONE_BENCH=1 RIPCLONE_IO_URING=<0|1> \
  ripclone --server https://ripclone.fly.dev clone oven-sh/bun \
  --dir /data/bun-<label> --mode full
```

Results, alternating POSIX/io_uring in the same machine:

```
posix-1:    total 3533 ms | write 2649 ms | real 3.59 s
io_uring-1: total 5265 ms | write 2169 ms | real 5.29 s
posix-2:    total 2335 ms | write 2190 ms | real 2.36 s
io_uring-2: total 4061 ms | write 2098 ms | real 4.09 s
```

All runs had a clean `git status` and materialized a 304 MiB checkout. The warm
write-window comparison is `2190 ms` POSIX vs `2098 ms` io_uring, about a 4%
improvement. Total wall time was dominated by manifest/archive download
variance (`manifest_ms` was 93 ms for warm POSIX vs 1937 ms for warm io_uring),
so `total_ms` is not useful for backend comparison in this run.

Follow-up run, same machine shape and direct `/data` settings, mixed order with
five runs per backend:

```
io_uring-1: total 4055 ms | write 2334 ms | archive 3004 ms | real 4.11 s
posix-1:    total 2394 ms | write 2219 ms | archive 2358 ms | real 2.42 s
posix-2:    total 2818 ms | write 2470 ms | archive 2752 ms | real 2.85 s
io_uring-2: total 4563 ms | write 4426 ms | archive 4540 ms | real 4.59 s
io_uring-3: total 2340 ms | write 2177 ms | archive 2315 ms | real 2.37 s
posix-3:    total 3564 ms | write 3444 ms | archive 3528 ms | real 3.59 s
posix-4:    total 3686 ms | write 3554 ms | archive 3663 ms | real 3.71 s
io_uring-4: total 4170 ms | write 3853 ms | archive 4146 ms | real 4.20 s
io_uring-5: total 3196 ms | write 3060 ms | archive 3158 ms | real 3.22 s
posix-5:    total 3337 ms | write 3202 ms | archive 3304 ms | real 3.36 s
```

Summary:

```
posix:    total avg 3160 ms, median 3337 ms | write avg 2978 ms, median 3202 ms
io_uring: total avg 3665 ms, median 4055 ms | write avg 3170 ms, median 3060 ms
```

The best full-clone run was io_uring (`2340 ms` total, `2177 ms` write), and the
worst was also io_uring (`4563 ms` total, `4426 ms` write). The wide swing tracks
`archive_download_ms`, not just backend choice, so the full clone benchmark still
does not isolate writer speed well enough to explain the lower-level writer
result.

### Historical: Fly `/data` ext4 volume, 8 shared CPUs, 2 GB

Corrected harness: file contents are cloned into owned buffers before the timed
region, so wall time covers `WorktreeWriter::write_owned_entries` only.

Mixed Bun-ish workload: 20000x2KB + 64x1MiB, 7 threads, batch 256, 5 runs:

```
posix:    wall 254.6 ms | 78799 files/s | 404.8 MiB/s
  prep 307.6 ms | io 634.7 ms | mtime 140.7 ms

io_uring: wall 229.4 ms | 87448 files/s | 449.2 MiB/s
  prep 372.2 ms | io 680.5 ms | mtime 153.6 ms
```

io_uring is ~10% faster wall-clock here, but the summed thread-time buckets are
noisy on shared CPUs. The important point is that wall-clock writer isolation
does show a win, while the full clone benchmark mostly does not.

Small files only: 20000x2KB, 7 threads, batch 256, 5 runs:

```
posix:    wall 183.0 ms | io 615.6 ms
io_uring: wall 181.6 ms | io 610.8 ms
```

At high parallelism this is effectively tied: prep and mtime are the floor.

Small files only: 20000x2KB, **1 thread**, batch 256, 5 runs:

```
posix:    wall 940.8 ms | io 531.3 ms
io_uring: wall 574.6 ms | io 171.7 ms
```

This is the clearest proof that the ring path works: batched io_uring cuts
single-thread small-file open/write/close time by ~68% and wall time by ~39%.

Small files only: 20000x2KB, 7 threads, **batch 1**, 5 runs:

```
posix:    wall 163.2 ms | io 618.8 ms
io_uring: wall 281.7 ms | io 1191.1 ms
```

Batching is mandatory. One file per ring submission is substantially worse than
POSIX.

Large files only: 512x1MiB, 7 threads, batch 256, 3 runs:

```
posix:    wall 10666.1 ms | io 70601.6 ms
io_uring: wall 11619.1 ms | io 70566.4 ms
```

For large writes on this Fly volume, io_uring does not help. The I/O thread-time
is identical and wall is slightly worse, so batch-of-few/large-file work should
probably stay on POSIX.

### Measured: Fly `/data` ext4 volume, 8 performance CPUs, 16 GB

Same corrected harness, but on Fly performance CPUs. One-off benchmark machines
used `--rm --restart no --autostop=stop --autostart=false`.

Mixed Bun-ish workload: 20000x2KB + 64x1MiB, 7 threads, batch 256, 5 runs:

```
posix:    wall 218.5 ms |  91814 files/s | 471.6 MiB/s
  prep 199.9 ms | io 537.2 ms | mtime  81.2 ms

io_uring: wall 155.9 ms | 128739 files/s | 661.3 MiB/s
  prep 271.6 ms | io 564.3 ms | mtime 101.9 ms
```

io_uring is ~29% faster wall-clock on this laptop-like CPU shape, even though
the summed thread-time buckets remain noisy under parallel scheduling.

Small files only: 20000x2KB, 7 threads, batch 256, 5 runs:

```
posix:    wall 130.1 ms | io 460.6 ms
io_uring: wall 133.2 ms | io 494.5 ms
```

At high parallelism, small-file wall time is still effectively tied because prep
and mtime are now a large floor.

Small files only: 20000x2KB, **1 thread**, batch 256, 5 runs:

```
posix:    wall 552.3 ms | io 351.0 ms
io_uring: wall 338.6 ms | io 138.7 ms
```

This is the cleanest performance-CPU proof: batched io_uring cuts single-thread
small-file open/write/close time by ~60% and wall time by ~39%.

Large files only: 512x1MiB, 7 threads, batch 256, 3 runs:

```
posix:    wall 67.7 ms | io 342.3 ms
io_uring: wall 57.4 ms | io 251.0 ms
```

Unlike the shared-CPU run, large writes also improved on performance CPUs. This
benchmark does not fsync, so it is measuring buffered materialization latency,
not durable writeback.

Small files only: 20000x2KB, 7 threads, **batch 1**, 5 runs:

```
posix:    wall 118.7 ms | io 451.1 ms
io_uring: wall 203.7 ms | io 923.4 ms
```

Batching remains mandatory. One file per ring submit is substantially worse than
POSIX even on performance CPUs.

### Measured: local macOS / APFS, 20000x2KB + 64x1MiB, 7 threads, batch 256

```
posix: wall 2402 ms | 8352 files/s | 42.9 MiB/s
  prep   5.1%  | io 77.8% | mtime 17.1%
```

This local POSIX-only split was the first hint that **mtime 17% + prep 5% = 22%
that io_uring cannot reduce**. The Fly measurements above confirm the shape:
once batched io_uring shrinks the I/O slice, mtime+prep become the floor, so
removing mtime likely beats further ring tuning.

## 5. Ranked optimizations

1. **Eliminate the per-file mtime syscall (biggest win).** Files are written with
   `INDEX_MTIME = (1,0)` so git's index matches. Instead of `utimensat` per file
   on the FS (then git re-`lstat`s during skip-worktree clear at
   `extract.rs:583`), write the stat cache into the index directly. Removes N
   `utimensat` + N `lstat`. There is no `utimensat`/`futimens` io_uring opcode in
   mainline pre-6.x kernels, so batching it into the ring is not available — the
   index approach is the real fix.
2. **Skip redundant per-file `safe_create_dir_all`** (`worktree_writer.rs:217`).
   Dirs are pre-created in `extract.rs:207-224`. Pass a `dirs_precreated` flag or
   keep a thread-local "created dirs" set so the hot path is one hashset hit, not
   2-3 stats per path component.
3. **Drop the double `exists()` in POSIX** (`write_regular_posix:394`); rely on
   `O_CREAT|O_TRUNC`. POSIX-only, minor.
4. **Avoid `content.to_vec()` for POSIX** (`extract.rs:461`); POSIX can write the
   borrowed frame slice. Only io_uring needs an owned buffer for submission
   lifetime.
5. **Bypass io_uring for batch-of-1 large files**; use plain `pwrite` where there
   is no batching benefit to amortize ring overhead.

Items 1 and 2 shrink the shared "extractor" time the clone benchmark actually
reports, independent of backend — likely a larger real-world win than the
io_uring port by itself.
