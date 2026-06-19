# ripclone

A headless backend for fast git clones.

ripclone pre-builds git artifacts for every pushed commit so that agents, CI systems, and humans can clone a repo and start working in seconds instead of waiting for a full `git clone`. It is **read-only** and **clone-only**: it does not proxy commits or pushes. Agents use normal git with their own GitHub tokens for writes.

It is designed to be self-hosted on Fly, your own infrastructure, or any cloud.

## How it works

Git stores a repo as a Merkle tree: commits point to trees, trees point to blobs, and blobs are file contents. The `.git/index` is a snapshot of which blobs should be on disk and what mode they should have. When you `git clone`, Git downloads a packfile containing commits/trees/blobs, and then checks out the files.

You can speed this up with a shallow clone (`--depth=1`), which skips old history but still fetches every blob for `HEAD`. Or a partial/blobless clone (`--filter=blob:none`), which skips blobs at first but has to fetch them lazily when you run commands like `git diff`. Neither is ideal for an agent that wants a fully working repo immediately.

ripclone takes a different approach. On every push, it builds a **clonepack**: a top-level manifest that points to a metadata chunk (skeleton pack, HEAD-blobs pack, prebuilt `.git/index`, plus file/frame tables) and content-addressed archive chunks holding the working-tree file bytes. When you run `ripclone clone`, the client fetches the manifest and metadata chunk, installs the prebuilt `.git` artifacts directly, and then materializes the working tree. By default it uses `git checkout-index` from the prebuilt HEAD-blobs pack; an opt-in archive-chunk extraction path (`RIPCLONE_EXTRACT_ARCHIVE=1`) writes files directly from zstd frames. No `git init`, `git index-pack`, `git read-tree`, or `git update-index`.

The result is a normal git repo with a clean `git status` and working `git diff`, ready in a fraction of the time.

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

## Docs

- [`ROADMAP.md`](ROADMAP.md) — technical direction for the headless backend.
- [`docs/GITHUB_INTEGRATION.md`](docs/GITHUB_INTEGRATION.md) — GitHub integration and auth notes.
- [`docs/ARCHIVE_ADVERSARIAL_REVIEW.md`](docs/ARCHIVE_ADVERSARIAL_REVIEW.md) — code-level review of the archive-first path.
- [`docs/CAS_CLONE_SPIKES_FINDINGS.md`](docs/CAS_CLONE_SPIKES_FINDINGS.md) — v1 spike results.

## Status

ripclone is under active development. The archive-first clone path, native git remote helper, S3-compatible object storage, token auth, rate limiting, retention, and smart-HTTP fallback are implemented. Streaming extraction and delta updates are future work.
