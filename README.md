<p align="center">
  <img src="assets/logo.png" alt="ripclone logo" width="200">
</p>

# ripclone

ripclone is the fastest way to clone git repos. Large repos see 5x-10x speedup; small repos are also a bit faster.

ripclone pre-builds git artifacts for every pushed commit so that agents, CI systems, and humans can clone a repo and start working in seconds instead of waiting for a full `git clone`. It is **read-only** and **clone-only**: it does not proxy commits or pushes. You use normal git with your own GitHub tokens for writes.

It is designed to be self-hosted and works for private or public repos. For the easiest experience, sign up for free (for public repos) at [Ripclone Cloud](https://ripclone.com).

ripclone started from a simple question asked by [Jarred Sumner](https://x.com/jarredsumner/status/2066420871753838913): 

> *"It's hard to imagine why cloning a git repo should be much slower than downloading an equivalent-sized file. Where are the experiments with custom git clients that clone faster?"* 

ripclone is one answer.

## How it works

A normal `git clone` is slow because it interleaves several expensive steps. The client and server ***negotiate*** which objects to send; the client ***indexes*** the pack; git builds the index; then it checks out every file. Each step is fine on its own, but together they create a long chain of round trips and disk operations that are hard to parallelize.

ripclone unbundles that chain. The server runs the ***negotiation***, ***indexing***, and ***tree walking*** once per push and stores the results. The client only downloads precomputed artifacts and writes them to disk.

On every push, ripclone mirrors the repo and builds a **clonepack** for `HEAD`. A clonepack has three pieces:

- **Manifest.** A small file that lists the hashes of the metadata chunk and every content chunk. The client downloads this first to know what to fetch.
- **Metadata chunk.** Contains a skeleton pack and index, a prebuilt `.git/index`, and tables that map every file path to its mode, blob hash, and byte location in the archive. The client uses this to assemble `.git/` without running any git commands.
- **Content chunks.** The actual file bytes for `HEAD`.

### The skeleton

The skeleton is a git packfile containing the `HEAD` commit object and every tree reachable from it, but no blobs. It is enough for git to understand the shape of the repo — every directory, file path, mode, and blob hash — without the file contents. The client drops the skeleton pack into `.git/objects/pack/` alongside the prebuilt index, so commands like `git ls-tree`, `git log`, and `git status` work immediately.

### Getting the file bytes

ripclone stores the same `HEAD` file bytes in two formats so you can choose the tradeoff.

***Head-blobs pack.*** A normal git packfile containing every blob reachable from `HEAD`. When the client installs this pack next to the skeleton, `git diff`, `git show`, and `git checkout-index` all behave exactly like a regular `git clone --depth=1`.

***Archive chunks.*** The same blob bytes grouped into zstd-compressed chunks. Each chunk is made of independent ***zstd frames***, so the client can fetch many chunks in parallel and start decompressing and writing files as soon as the first bytes arrive, while later chunks are still downloading. The frame-level split also means a point lookup can fetch just the frame it needs instead of the whole chunk. This is faster than a git pack for pure materialization, but it leaves `.git/objects` without blobs, so raw git content commands do not work.

### Clone modes

`--mode=full` (the default) downloads the metadata chunk and the archive chunks, writes the working tree directly from the zstd frames, and builds a local HEAD-blobs pack from those bytes so `git diff`, `git show`, and `git checkout-index` all work. The result is indistinguishable from `git clone --depth=1`.

`--mode=fast` downloads the metadata chunk and the archive chunks, then writes files directly from the zstd frames without building a blob pack. This is the fastest way to get a working tree for agents that only edit and commit; `git diff`/`git show` do not work.

`--mode=hybrid` downloads the pre-built HEAD-blobs pack in parallel with the archive chunks and writes the working tree from the archive. Faster than `full` when bandwidth is plentiful because it avoids the local pack-build CPU cost; slower on constrained links because it downloads extra bytes.

`--mode=skeleton` downloads only the metadata chunk. It gives you a valid `.git/` with history and tree structure but no working tree and no blob objects.

### Performance

ripclone pre-builds git artifacts so clones are faster than `git clone` across every bandwidth we tested. On a 1000 Mbps link the wins are largest; as bandwidth drops the download itself dominates and the gap narrows.

At 1000 Mbps, measured speedups over native `git clone` are:

- **`oven-sh/bun`**: full clone **7.0×**, depth-1 **5.9×**, files **10.1×**.
- **`pandas-dev/pandas`**: full clone **4.5×**, depth-1 **3.8×**, files **4.6×**.
- **`torvalds/linux`** (1000 Mbps only): full clone **5.5×**, depth-1 **7.6×**, files **11.2×**.
- **`facebook/react`** was not included in this shaped sweep; earlier warm-cache measurements showed a depth-1 speedup of about **3.8×**.

The full-clone win is smaller on Linux than on bun because the full pack is so large that the transfer dominates; depth-1 and `files` mode avoid most of that transfer, so they stay well ahead even on huge repos.

#### Shaped bandwidth benchmark

We ran `ripclone` against native `git clone` on a Fly.io `performance-8x` client talking to a `ripclone-server` over shaped links from 50 Mbps to 1000 Mbps. Each cell is a single run (n=1). `oven-sh/bun` and `pandas-dev/pandas` were measured across all five bandwidths. `torvalds/linux` was only measured at 1000 Mbps because a full `git clone` of Linux at lower bandwidths takes ~8 min per run.

**`oven-sh/bun`**

| Mbps | ripclone full | ripclone depth=1 | ripclone files | git clone full | git clone --depth 1 |
|------|---------------|------------------|----------------|----------------|---------------------|
| 1000 | 5.1 s | 1.2 s | 0.7 s | 35.9 s | 7.1 s |
| 500 | 9.6 s | 1.9 s | 1.1 s | 35.0 s | 3.3 s |
| 250 | 17.0 s | 3.4 s | 1.9 s | 40.7 s | 3.2 s |
| 100 | 41.7 s | 5.9 s | 4.2 s | 67.2 s | 5.9 s |
| 50 | 84.4 s | 11.4 s | 9.2 s | 115.6 s | 10.9 s |

**`pandas-dev/pandas`**

| Mbps | ripclone full | ripclone depth=1 | ripclone files | git clone full | git clone --depth 1 |
|------|---------------|------------------|----------------|----------------|---------------------|
| 1000 | 4.6 s | 0.6 s | 0.5 s | 20.7 s | 2.3 s |
| 500 | 7.7 s | 0.8 s | 0.4 s | 20.9 s | 2.3 s |
| 250 | 14.8 s | 1.3 s | 0.4 s | 24.9 s | 2.3 s |
| 100 | 33.9 s | 2.1 s | 0.6 s | 43.0 s | 2.4 s |
| 50 | 65.2 s | 3.0 s | 1.9 s | 75.9 s | 3.0 s |

**`torvalds/linux`** (1000 Mbps only)

Because a full `git clone` of Linux at lower bandwidths takes ~8 min per run, we only measured the 1000 Mbps point.

| Mbps | ripclone full | ripclone depth=1 | ripclone files | git clone full | git clone --depth 1 |
|------|---------------|------------------|----------------|----------------|---------------------|
| 1000 | 84.3 s | 4.4 s | 3.0 s | 462.9 s | 33.5 s |

That works out to **5.5×** for full, **7.6×** for depth-1, and **11.2×** for files.

The ratio graph below shows **ripclone time / git time**; anything below the dashed `1.0` line means ripclone was faster.

![shaped benchmark ratios](benchmark/shaped_ratios.png)

At 1000 Mbps, `ripclone depth=1` and `ripclone files` are roughly 5× faster than `git clone --depth 1` for pandas and `torvalds/linux`; the gap narrows as bandwidth drops, but ripclone stays faster across every tested rate.

## Quick start

Build and run the server:

```bash
cd rust
cargo build --release

# Start the server locally
./target/release/ripclone-server \
  --cas-dir ./data/cache \
  --repo-root ./data/repos \
  --storage-dir ./data/storage \
  --default-depth 50
```

The default mirror depth is 50 commits. Increase it if you need to serve older commits or larger delta windows.

Build artifacts for a commit:

```bash
cargo run --release --bin ripclone -- build oven-sh/bun --commit abc123
```

Clone it:

```bash
cargo run --release --bin ripclone -- clone oven-sh/bun --dir bun
```

Add a fast worktree (Linux, reuses local objects and overlay staging):

```bash
cd bun
cargo run --release --bin ripclone -- worktree ../bun-wt -b HEAD
```

## GitHub Actions trigger

Add a workflow to a repo so ripclone builds artifacts on every push. Set `RIPCLONE_URL` as a repository variable and `RIPCLONE_TOKEN` as a repository secret.

```yaml
name: ripclone cache
on: push
jobs:
  notify:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger ripclone sync
        run: |
          curl -fsSL -X POST \
            -H "Authorization: Ripclone ${{ secrets.RIPCLONE_TOKEN }}" \
            "${{ vars.RIPCLONE_URL }}/v1/repos/${{ github.repository_owner }}/${{ github.event.repository.name }}/sync"
```

For private repos the ripclone server also needs a GitHub token with read access; set `RIPCLONE_GITHUB_TOKEN` on the server.

ripclone validates the `RIPCLONE_TOKEN`, syncs the mirror, builds artifacts for the new HEAD, and returns the artifact hashes.

## CLI usage

```bash
# Clone a repo (public or private)
ripclone clone owner/repo
ripclone clone owner/repo --branch feat/x --dir ./my-dir

# Update an existing clone to the latest commit
ripclone update

# Build artifacts for a specific commit (server-side)
ripclone build owner/repo --commit abc123

# Show resolved ref and artifact status
ripclone status
```

For private repos, pass a GitHub token:

```bash
GITHUB_TOKEN=ghp_xxx ripclone clone my-org/private-repo
```

Pushes go to GitHub directly, not through ripclone.

## Architecture

```
┌─────────────────┐
│  GitHub Actions │  triggers build on every push (OIDC verified)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ ripclone-server │  queues builds, serves artifacts, resolves refs
│   (this repo)   │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
Object storage   Local disk
(source of truth)  (hot cache)
```

- **Object storage** is the source of truth for all artifacts.
- **Local disk** is a ring-buffer hot cache.
- **Clients** download manifests, skeletons, and archives; stream-decompress frames; and write files directly.
- **GitHub remains the source of truth** for repos, refs, permissions, and writes.
- **IP rate limiting** protects public endpoints from abuse.

## Build options

By default the Rust crate uses `zlib-ng` for faster pack compression. On platforms without cmake you can build with the stock zlib instead:

```bash
cd rust
cargo build --release --no-default-features
```

Environment variables for tuning clone performance:

- `RIPCLONE_FETCH_CONCURRENCY` — max concurrent chunk downloads (default 6).
- `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` — thread counts for archive extraction.
- `RIPCLONE_BLOB_PACK_THREADS` — threads used when building a local blob pack in `full` mode.

## License

ripclone is licensed under the [Elastic License 2.0](LICENSE).

You may use, modify, and distribute the software freely. You may not provide
ripclone to third parties as a hosted or managed service. See the full text in
[`LICENSE`](LICENSE) for details.
