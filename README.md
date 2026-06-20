<p align="center">
  <img src="docs/logo.png" alt="ripclone logo" width="200">
</p>

# ripclone

ripclone is the fastest way to clone git repos. Large repos see 5x-10x speedup; small repos are also a bit faster.

ripclone pre-builds git artifacts for every pushed commit so that agents, CI systems, and humans can clone a repo and start working in seconds instead of waiting for a full `git clone`. It is **read-only** and **clone-only**: it does not proxy commits or pushes. You use normal git with your own GitHub tokens for writes.

It is designed to be self-hosted and works for private or public repos. For the easiest experience, sign up for free (for public repos) at [Ripclone Cloud](https://ripclone.com).

ripclone started from a simple question asked by [Jarred Sumner](https://x.com/jarredsumner/status/2066420871753838913): 

> *"It's hard to imagine why cloning a git repo should be much slower than downloading an equivalent-sized file. Where are the experiments with custom git clients that clone faster?"* 

ripclone is one answer.

## How it works

A normal `git clone` downloads a packfile of commits, trees, and blobs, then runs `git init`, `git index-pack`, `git read-tree`, and `git checkout-index` to build the `.git` directory and working tree. ripclone runs those steps ahead of time on the server so the client can skip them.

On every push, ripclone mirrors the repo and builds a **clonepack** for `HEAD`. A clonepack has three pieces:

**Manifest.** A small file that lists the hashes of the metadata chunk and every content chunk. The client downloads this first to know what to fetch.

**Metadata chunk.** Contains a skeleton pack and index, a prebuilt `.git/index`, and tables that map every file path to its mode, blob hash, and byte location in the archive. The client uses this to assemble `.git/` without running any git commands.

**Content chunks.** The actual file bytes for `HEAD`.

### The skeleton

The skeleton is a git packfile containing the `HEAD` commit object and every tree reachable from it, but no blobs. It is enough for git to understand the shape of the repo — every directory, file path, mode, and blob hash — without the file contents. The client drops the skeleton pack into `.git/objects/pack/` alongside the prebuilt index, so commands like `git ls-tree`, `git log`, and `git status` work immediately.

### Getting the file bytes

ripclone stores the same `HEAD` file bytes in two formats so you can choose the tradeoff.

**Head-blobs pack.** A normal git packfile containing every blob reachable from `HEAD`. When the client installs this pack next to the skeleton, `git diff`, `git show`, and `git checkout-index` all behave exactly like a regular `git clone --depth=1`.

**Archive chunks.** The same blob bytes grouped into zstd-compressed chunks. Each chunk is made of independent zstd frames, so the client can fetch many chunks in parallel and start decompressing and writing files as soon as the first bytes arrive, while later chunks are still downloading. This is faster than a git pack for pure materialization, but it leaves `.git/objects` without blobs, so raw git content commands do not work.

### Clone modes

`--mode=full` (the default) downloads the metadata chunk and the head-blobs pack, installs the prebuilt `.git/` artifacts, and runs `git checkout-index` to write the working tree. The result is indistinguishable from `git clone --depth=1`.

`--mode=fast` downloads the metadata chunk and the archive chunks, then writes files directly from the zstd frames without using git checkout. This is the fastest way to get a working tree for agents that only edit and commit.

`--mode=hybrid` downloads both the head-blobs pack and the archive chunks concurrently, writes files from the archive, and also installs the head-blobs pack so the repo has full git compatibility as soon as the pack lands.

`--mode=skeleton` downloads only the metadata chunk. It gives you a valid `.git/` with history and tree structure but no working tree and no blob objects.

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

## License

ripclone is licensed under the [Elastic License 2.0](LICENSE).

You may use, modify, and distribute the software freely. You may not provide
ripclone to third parties as a hosted or managed service. See the full text in
[`LICENSE`](LICENSE) for details.
