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

## Clone

A normal `git clone` is slow because it does several slow things in a row. The client and server figure out which objects to send. The client unpacks them and git builds an index. Then git writes out every file. Each step is fine alone, but chained together they make a lot of round trips and disk work that is hard to overlap.

ripclone moves that work to the server, ahead of time (see [Building a clonepack](#building-a-clonepack)). By the time you clone, the hard parts are done — the client just downloads the finished pieces and writes them to disk. You pick how much you want with `--mode`:

`--mode=editable` (the default) gives you a real, editable git repo, the same as `git clone --depth=1`: `git diff`, `git show`, `git log`, and commits all work. It downloads one git pack with `HEAD`'s objects and reads the working tree straight out of it — one download, no extra work.

`--mode=files` is the fastest way to get just a working tree, for agents and CI jobs that only need the files. It downloads the zstd archive and writes the files directly. There's no git object database, so `git diff`/`git show` don't work.

`--mode=skeleton` downloads only the metadata. You get a valid `.git/` with history and structure, but no working tree and no file contents.

### Performance

ripclone vs native `git clone`, at all three levels. Lower is better; **bold** is ripclone.

| repo | files | `git --depth 1` | ripclone `--depth 1` | `git clone` (full) | ripclone (full) | ripclone (files) |
|---|---|---|---|---|---|---|
| `facebook/react` (medium) | ~7k | 2.4 s | **0.5 s** | 50.8 s | **1.6 s** | **0.5 s** |
| `oven-sh/bun` (large) | ~19k | 3.6 s | **0.9 s** | 37.0 s | **2.5 s** | **0.7 s** |
| `torvalds/linux` (huge) | ~95k | 34.3 s | **~6 s** | — | — | — |

The wins grow with repo size and history depth. For `--depth 1` ripclone is **4–6× faster**; for a full clone it is **15–32× faster**, because git makes GitHub compute and stream the whole history pack on demand while ripclone just downloads pre-built, content-addressed packs in parallel. `files` mode (working tree only, from the zstd archive) is the fastest of all.

Measured on a Fly `performance-8x` client (Newark) against a ripclone server in Ashburn with artifacts in Tigris; warm server cache, client artifact cache disabled, written to an NVMe volume. git clones are from GitHub over the same link. Median of 3 runs.

> `torvalds/linux` is shown at `--depth 1` only — the realistic case for a repo this size. Pre-building its full ~1.3M-commit history is a heavy one-time job that our dev box couldn't complete (the object-storage upload of that much data times out); the depth=1 path, which is what CI and agents actually use, is unaffected.

## Building a clonepack

On every push, ripclone mirrors the repo and builds a **clonepack** for `HEAD` so the clone above is fast. A clonepack has three parts:

- **Manifest.** A small file listing everything else. The client grabs this first to know what to fetch.
- **Metadata chunk.** The repo's shape: a skeleton pack, a ready-made `.git/index`, and a table that says where each file lives. The client uses it to build `.git/` without running any git commands.
- **Content chunks.** The actual file bytes for `HEAD`, kept two ways so each clone mode can pick the better tradeoff.

### The skeleton

The skeleton is a git packfile with the `HEAD` commit and every tree, but no file contents. That's enough for git to know the shape of the repo — every folder, file, mode, and blob hash. The client drops it into `.git/objects/pack/` next to the prebuilt index, so `git ls-tree`, `git log`, and `git status` work right away.

### The two content formats

ripclone keeps the `HEAD` file bytes two ways:

***Depth pack.*** One git packfile with `HEAD`'s commit, trees, and all its files. The client drops it into `.git/objects` and reads the working tree straight out of it — one download, no archive, no extra work. Because the objects live in `.git`, all the git content commands work. This is what **editable** mode uses.

***Archive chunks.*** The same file bytes, zstd-compressed and split into chunks. Each chunk is made of independent frames, so the client can download many at once and start writing files as the first bytes land. It's the fastest path to a working tree, but the files don't go into `.git`, so git content commands don't work. This is what **files** mode uses.

### Two-phase publish

Most clones just want the latest `HEAD`, so a sync builds that first and the rest later.

- **Phase 1.** The server builds everything a `--depth 1` clone needs: the skeleton, the index, and a pack with `HEAD`'s files. This is ready fast and served right away. The ref says `build_status: "full history building"` so clients know more is coming.
- **Phase 2.** In the background, the server builds the full history and the archive, reusing whatever didn't change since last sync. When it's done, it clears `build_status`.

A clone that only wants `HEAD` is ready as soon as phase 1 finishes. A clone that wants full history waits for phase 2 — the server returns `202` while it works and the client retries on its own.

### Content-defined chunking and cheap re-syncs

The archive is split by content, not by fixed size: cut points land on the data itself (frames run about 1–16 MB). Each frame is compressed on its own and named by the hash of its bytes.

This makes re-syncs cheap. When a new commit lands, frames that didn't change hash to the same name and are reused as-is — no recompressing, no re-uploading. Only the frames that actually changed get rebuilt, so the work matches the size of the diff, not the whole repo. The builder also streams one file at a time, so memory stays flat no matter how big the repo gets.

### Sync performance

How long a sync takes to build the artifacts (server-side, the same hardware as the clone numbers above). There's no git equivalent — git builds nothing ahead of time.

| repo | phase 1 (depth=1 clone-ready) | phase 2 (full history, background) |
|---|---|---|
| `facebook/react` | 5.4 s | +32 s |
| `oven-sh/bun` | ~8 s | +13 s |
| `torvalds/linux` | ~40 s | very large |

Phase 1 is what a `--depth 1` clone waits for; phase 2 runs in the background and only gates full clones. react's phase 2 is a cold first build; bun's is much shorter because the incremental re-sync reuses unchanged history levels and archive frames. linux's phase 1 is dominated by building the HEAD-closure pack for its ~95k-file tree, and its full history is large enough that we don't pre-build it on the dev box.

> In production the server syncs on push, so this happens once per commit, ahead of any clone — by the time a CI runner or agent asks for the repo, the artifacts are already built.

## Quick start

Build and run the server:

```bash
cd rust
cargo build --release

# Start the server locally
./target/release/ripclone-server \
  --cas-dir ./data/cache \
  --repo-root ./data/repos
```

`--cas-dir` is the local cache; `--repo-root` holds the mirrors. `--host` (default `0.0.0.0`) and `--port` (default `8000`) set the listen address. Object storage (S3/R2/Tigris/MinIO) and most tuning are set with environment variables — see [Build options](#build-options) and `docs/BACKENDS.md`.

Build artifacts for a commit (sync the repo on the server):

```bash
cargo run --release --bin ripclone -- sync oven-sh/bun --server http://localhost:8000
```

Clone it:

```bash
cargo run --release --bin ripclone -- clone oven-sh/bun --dir bun --server http://localhost:8000
```

Add a fast worktree (Linux, reuses local objects and overlay staging):

```bash
cd bun
cargo run --release --bin ripclone -- worktree ../bun-wt -b HEAD
```

## GitHub Actions trigger

Add a workflow to a repo so ripclone builds artifacts on every push. Set `RIPCLONE_URL` as a repository variable and `RIPCLONE_TOKEN` as a repository secret. (A ready-to-copy version lives in [`docs/examples/github-actions-trigger.yml`](docs/examples/github-actions-trigger.yml).)

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

ripclone validates the `RIPCLONE_TOKEN`, syncs the mirror, builds artifacts for the new `HEAD`, and returns the artifact hashes.

## CLI usage

By default the CLI talks to the managed [Ripclone Cloud](https://ripclone.com). Point it at a self-hosted server with `--server`, the `RIPCLONE_SERVER` env var, or `ripclone login`. (Resolution order: `--server` > `RIPCLONE_SERVER` > saved login config > cloud.)

```bash
# Authorize this machine against the cloud (saves a token), or sign out
ripclone login
ripclone logout

# Clone a repo (public or private)
ripclone clone owner/repo
ripclone clone owner/repo --branch feat/x --dir ./my-dir

# Choose how the working tree is materialized
ripclone clone owner/repo --mode files         # working tree only, fastest
ripclone clone owner/repo --mode skeleton      # .git only, no working tree

# History depth: 1 = HEAD only (default), N = last N commits, 0 = full history
ripclone clone owner/repo --depth 0

# Clone the artifacts built for a specific rev (pairs with `sync --at`)
ripclone clone owner/repo --at HEAD~5

# Ephemeral, in-memory (tmpfs) clone for throwaway agent/CI machines (Linux)
ripclone clone owner/repo --temp

# Print a per-phase benchmark report after the clone
ripclone clone owner/repo --bench

# Build/refresh artifacts on the server
ripclone sync owner/repo
ripclone sync owner/repo --depth 1             # shallow mirror
ripclone sync owner/repo --at HEAD~5           # build at a past rev

# Add a fast worktree inside an existing clone
ripclone worktree ../wt -b HEAD
```

For private repos, the **server** needs `RIPCLONE_GITHUB_TOKEN`; you can also pass a token for a single sync:

```bash
ripclone sync my-org/private-repo --github-token ghp_xxx
```

Pushes go to GitHub directly, not through ripclone.

## Architecture

```
┌─────────────────┐
│  GitHub Actions │  triggers build on every push (token-authenticated)
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

- **Object storage** holds all the artifacts. A background job cleans up objects nothing points at anymore (after a grace period, so it never deletes an upload still in flight).
- **Local disk** is a hot cache that gets trimmed as it fills up.
- **Clients** download the pieces, decompress, and write files straight to disk.
- **GitHub stays the source of truth** for repos, refs, permissions, and writes.
- **Rate limiting** keeps public endpoints from being abused.

Ops endpoints: `GET /healthz` (alive?), `GET /readyz` (ready? — `503` if storage or the ref store is down), and `GET /metrics` (Prometheus format). There's also a plain-git fallback (`/v1/git/{owner}/{repo}/...`) so a normal `git clone` still works if the fast path is down.

## Build options

By default the Rust crate uses `zlib-ng` for faster pack compression. On platforms without cmake you can build with the stock zlib instead:

```bash
cd rust
cargo build --release --no-default-features
```

Environment variables for tuning clone performance:

- `RIPCLONE_FETCH_CONCURRENCY` — max concurrent chunk downloads (default 6).
- `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` — thread counts for archive extraction.
- `RIPCLONE_FETCH_MAX_ATTEMPTS` / `RIPCLONE_FETCH_BACKOFF_MS` — retry budget and base backoff for transient download failures (defaults 3 and 100).
- `RIPCLONE_IO_URING` — the worktree writer uses io_uring by default on Linux; set `=0` to force the POSIX writer. `RIPCLONE_IO_URING_DEPTH` (default 2) tunes per-thread ring overlap.
- `RIPCLONE_MODE` — default clone mode (`editable`, `files`, or `skeleton`) when `--mode` is omitted.
- `RIPCLONE_CACHE_DIR` / `RIPCLONE_NO_CACHE` — opt in to (or force off) a local artifact cache; off by default.

Server-side storage and retention (S3-compatible backends, remote GC, local eviction) are configured through `RIPCLONE_S3_*`, `RIPCLONE_RETENTION_*`, and `RIPCLONE_REMOTE_GC_*` variables; see `docs/BACKENDS.md` and `CHANGELOG.md` for the full list.

## License

ripclone is licensed under the [Elastic License 2.0](LICENSE).

You may use, modify, and distribute the software freely. You may not provide
ripclone to third parties as a hosted or managed service. See the full text in
[`LICENSE`](LICENSE) for details.
