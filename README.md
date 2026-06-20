<p align="center">
  <img src="assets/logo.png" alt="ripclone logo" width="200">
</p>

# ripclone

ripclone is the fastest way to clone git repos. Large repos see 5x-10x speedup; small repos are also faster.

ripclone pre-builds git artifacts for every pushed commit so that agents, CI systems, and humans can clone a repo and start working in seconds instead of waiting for a full `git clone`. It is **read-only** and **clone-only**: it does not proxy commits or pushes. You use normal git with your own GitHub tokens for writes.

It is designed to be self-hosted and works for private or public repos. For the easiest experience, sign up for free (for public repos) at [Ripclone Cloud](https://ripclone.com).

ripclone started from a simple question asked by [Jarred Sumner](https://x.com/jarredsumner/status/2066420871753838913): 

> *"It's hard to imagine why cloning a git repo should be much slower than downloading an equivalent-sized file. Where are the experiments with custom git clients that clone faster?"* 

ripclone is one answer.

## How it works

A normal `git clone` downloads a packfile of commits, trees, and blobs, then runs `git init`, `git index-pack`, `git read-tree`, and `git checkout-index` to build the `.git` directory and working tree. 

ripclone runs those steps ahead of time on the server so the client can skip them. On every push, ripclone mirrors the repo and builds a **clonepack** for the requested depth. A clonepack has two pieces:

The ***manifest*** is a small file that lists the signed object storage URLs and hashes of the pack chunks and the optional files artifact. The client downloads this first to know what to fetch.

The ***depth pack*** is a git packfile containing the commits, trees, and blobs for the requested history depth. The client installs it into `.git/objects/pack/` and uses it for both git operations and working-tree extraction.

The optional ***files artifact*** is the working tree as zstd-compressed raw bytes, used by `--mode files` for the fastest possible file-only clones.

### The depth pack

The depth pack is a normal git packfile containing the commits, trees, and blobs for the requested history depth. For `--depth 1` it includes the `HEAD` commit, its tree, and every blob reachable from `HEAD`. The client drops the pack into `.git/objects/pack/` alongside its prebuilt idx, so commands like `git status`, `git diff`, `git show`, and `git checkout` work immediately.

### The files artifact

For `--mode files`, ripclone also builds the working tree as zstd-compressed raw bytes. This is faster than extracting from a git pack when you only need files and do not need a usable `.git` directory.

### Clone modes

`--mode=editable` (the default) downloads the depth pack for `--depth N` and installs it as a real git repo. The working tree is extracted directly from the pack in parallel. The result is indistinguishable from `git clone --depth=N`.

`--mode=files` downloads the optional zstd files artifact and writes the working tree as fast as possible. This is ideal for CI / build-only workflows. It is not a usable git repo.

`--mode=skeleton` installs only the `.git` metadata (commit and tree objects) with no working tree or blobs.

### Design

A normal `git clone` is slow because it interleaves several expensive steps. The client and server ***negotiate*** which objects to send; the client ***indexes*** the pack; git builds the index; then it checks out every file. Each step is fine on its own, but together they create a long chain of round trips and disk operations that are hard to parallelize.

ripclone unbundles that chain. The server runs the ***negotiation***, ***indexing***, and ***tree walking*** once per push and stores the results as a git pack. The client only downloads the pack chunks it needs, installs the pack, and extracts the working tree in parallel.

For CI workflows that only need files, the server can also build a ***zstd files artifact***. This trades a small amount of extra storage for the fastest possible file materialization.

The result is a git repo built from precomputed parts rather than reconstructed on the fly.

### Performance

| repo | files | `git clone --depth=1` | ripclone | speedup |
|---|---|---|---|---|
| `oven-sh/bun` | ~15k | ~8 s | **~4.0 s** | **2×** |
| `facebook/react` | ~7.2k | ~3.8 s | **~1.0 s** | **3.8×** |
| `pandas-dev/pandas` | ~2.6k | ~3.3 s | **~0.5 s** | **6.6×** |

Measured on macOS over a 1 Gb/s link with a warm ripclone cache. On slower networks the absolute times grow, but ripclone is usually still faster because it transfers fewer bytes and overlaps download with extraction.

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

Clone just the files for CI:

```bash
cargo run --release --bin ripclone -- clone oven-sh/bun --mode files --dir bun
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
- **Clients** download manifests and pack chunks, install the pack, and extract files in parallel.
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
- `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` — thread counts for pack-to-files extraction.

## License

ripclone is licensed under the [Elastic License 2.0](LICENSE).

You may use, modify, and distribute the software freely. You may not provide
ripclone to third parties as a hosted or managed service. See the full text in
[`LICENSE`](LICENSE) for details.
