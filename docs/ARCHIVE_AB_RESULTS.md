# Archive-chunk extraction A/B results

Date: 2026-06-19
Server: `ripclone.fly.dev` (Fly `shared-cpu-2x`, 1 GB, iad, local storage)
Client versions: current `main` with protobuf clonepack + chunked archive support

## Test setup

- `default` path: fetch clonepack manifest + metadata chunk, install `.git` artifacts, then `git checkout-index` from the prebuilt head-blobs pack.
- `archive` path: same fetch, then stream-decompress archive chunks and write files directly (`RIPCLONE_EXTRACT_ARCHIVE=1`).
- Both paths use overlay staging on Linux when available.
- Each cell shows wall time, user+sys CPU time, and post-clone `git status` time.

## Local macOS (server on same machine)

| repo | default | archive | winner |
|---|---|---|---|
| `pandas-dev/pandas` | 574 ms (CPU 0.52 s, status 441 ms) | **473 ms** (CPU 1.10 s, status 33 ms) | archive |
| `oven-sh/bun` | 2984 ms (CPU 2.64 s, status 1569 ms) | **2279 ms** (CPU 5.76 s, status 113 ms) | archive |

Archive extraction is faster when network/download cost is negligible. It uses more CPU (zstd decompression) but avoids the `git checkout-index` metadata/stat storm, so `git status` is dramatically faster.

## macOS → Fly server

| repo | default | archive | winner |
|---|---|---|---|
| `pandas-dev/pandas` | **3359–6008 ms** (CPU ~0.6 s) | 6699–9025 ms (CPU ~1.1 s) | default |
| `oven-sh/bun` | **12.9–18.2 s** (CPU ~2.9 s) | 20.7–24.8 s (CPU ~5.7 s) | default |

Over the public internet the checkout-index path wins because the metadata chunk already contains the head-blobs pack (~65 MB for bun). The archive path downloads the same metadata chunk *plus* the archive chunks (~58 MB more for bun), so it moves significantly more bytes.

## Fly client (ewr) → Fly server (iad)

| repo | default | archive | winner |
|---|---|---|---|
| `oven-sh/bun` overlay | **6047 ms** (CPU 2.40 s, status 1368 ms) | 10 541 ms (CPU 3.10 s, status 66 ms) | default |
| `oven-sh/bun` rootfs | 24 416 ms (CPU 2.74 s, status 2708 ms) | — | — |

With overlay staging available, the archive path is about **1.7× slower** than `git checkout-index` (10.5 s vs 6.0 s). The extra time is almost entirely the ~58 MB of archive chunks that archive clients download on top of the metadata chunk. The actual extraction pipeline is fast: the 58 MB downloads in ~1 s and decompression/writing of 14 695 files to tmpfs takes ~3 s.

The earlier ~48 s archive result was caused by the benchmark script leaving overlay staging directories in `/dev/shm`, which made the archive run fall back to the slow rootfs (~8 MB/s). After cleaning `/dev/shm` between runs, archive extraction uses overlay staging and is competitive.

## Decision

Keep **`git checkout-index` as the default** materialization path. Archive-chunk extraction stays opt-in via `RIPCLONE_EXTRACT_ARCHIVE=1`.

Archive extraction can become the default only after at least one of these is true:

1. The server emits a **slim metadata chunk** that omits the head-blobs pack/idx for archive-only clients, so archive extraction does not download the same blobs twice.
2. The extraction pipeline is profiled and the Fly/inter-region overhead is eliminated.
3. The workload is known to be local or on a very fast link where the extra CPU is cheaper than `git checkout-index` metadata ops.

## Follow-ups

- Consider splitting the metadata chunk into a `.git` metadata variant and an archive-only manifest, or making head-blobs pack optional in the metadata chunk. This would remove the redundant head-blobs download for archive-only clients and likely make archive extraction faster than `git checkout-index` on fast links.
- Ensure overlay staging directories are cleaned up after installs so subsequent clones do not fall back to the slow rootfs.
