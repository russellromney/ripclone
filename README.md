<p align="center">
  <img src="assets/logo.png" alt="ripclone logo" width="200">
</p>

# ripclone

ripclone is the fastest way to clone git repos. Large repos see 5x-10x speedup; small repos are also a bit faster.

ripclone pre-builds git artifacts for every pushed commit so that agents, CI systems, and humans can clone a repo and start working in seconds instead of waiting for a full `git clone`. It is **read-only** and **clone-only**: it does not proxy commits or pushes. You use normal git with your own credentials for writes.

It works as two operations: you **sync** a repo so the server pre-builds its artifacts (automatic on every push), then you **clone** it — the client downloads those artifacts and writes the working tree in seconds.

It is self-hostable and host-agnostic — point it at GitHub, GitLab, Gitea, Bitbucket, or any git host — and works for private or public repos. For the easiest experience, sign up for free (for public repos) at [Ripclone Cloud](https://ripclone.com).

ripclone started from a simple question asked by [Jarred Sumner](https://x.com/jarredsumner/status/2066420871753838913): 

> *"It's hard to imagine why cloning a git repo should be much slower than downloading an equivalent-sized file. Where are the experiments with custom git clients that clone faster?"* 

ripclone is one answer. The goal: get a `git clone` as close as possible to downloading a file from object storage. A few design principles get there:

- **Move the slow work off the clone.** Negotiation, indexing, and the tree walk run once on the server at sync — never on the clone. The client downloads finished pieces and writes them.
- **Parallelize the downloads.** Packs and the archive are split into content-addressed chunks, so a clone is many parallel range-GETs, not one serial stream.
- **Keep every resource busy.** Download, decompress, process, and write all run across every core *and* overlap in time — the moment one stage produces output, the next starts on it. Network, CPU, and disk stay saturated instead of taking turns; on Linux the worktree writer uses io_uring to keep the disk queue full.
- **Process as little as possible.** A re-sync rebuilds only what the diff touched; a clone fetches only the artifacts its mode needs — files mode skips the git object database entirely.

## Clone

A normal `git clone` is slow because it does several slow things in a row. The client and server figure out which objects to send. The client unpacks them and git builds an index. Then git writes out every file. Each step is fine alone, but chained together they make a lot of round trips and disk work that is hard to overlap.

ripclone moves that work to the server, ahead of time (see [Sync](#sync)). By the time you clone, the hard parts are done — the client just downloads the finished pieces and writes them to disk. You pick how much you want with `--mode`:

`--mode=editable` (the default) gives you a real, editable git repo, the same as `git clone --depth=1`: `git diff`, `git show`, `git log`, and commits all work. It downloads `HEAD`'s objects as a git pack and reads the working tree straight out of it — one download at `--depth 1`, plus the history packs if you ask for more depth.

`--mode=files` is the fastest way to get just a working tree, for agents and CI jobs that only need the files. It downloads the zstd archive and writes the files directly. There's no git object database, so `git diff`/`git show` don't work.

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

## Sync

On every push, ripclone mirrors the repo and builds a **clonepack** for `HEAD` so the clone above is fast. A clonepack has three parts:

- **Manifest.** A small file listing everything else. The client grabs this first to know what to fetch.
- **Metadata chunk.** The repo's shape: a skeleton pack, a ready-made `.git/index`, and a table that says where each file lives. The client uses it to build `.git/` without running any git commands.
- **Content.** The objects and file bytes themselves, built three ways (HEAD pack, history packs, archive) so each clone takes only what it needs.

### The skeleton

The skeleton is a git packfile with the `HEAD` commit and every tree, but no file contents. That's enough for git to know the shape of the repo — every folder, file, mode, and blob hash. The client drops it into `.git/objects/pack/` next to the prebuilt index, so `git ls-tree`, `git log`, and `git status` work right away.

It is exactly the [HEAD pack](#the-three-content-artifacts) **minus the blobs**, and it ships inside the metadata chunk — small, and downloaded first — so **every** clone has the repo's full shape before any file content arrives. The content artifacts then layer the file bytes on top. The trees end up in both the skeleton and the HEAD pack, but git dedupes objects by hash, so the overlap costs nothing. This shape-first split is what lets a clone be useful almost immediately and stream the bulk of the bytes behind it.

### The three content artifacts

ripclone builds the content three ways so each clone takes only what it needs:

***HEAD pack.*** One undeltified git packfile with `HEAD`'s commit, trees, and every blob — the skeleton's shape plus the actual file bytes, a complete `--depth=1` repo. The client drops it into `.git/objects` and reads the working tree straight out of it: one download, no archive, no extra work. Because the objects live in `.git`, the git content commands work. An **editable** `--depth 1` clone installs just this.

***History packs.*** The rest of the commits, trees, and blobs, delta-compressed and split into immutable levels plus a small tail. A deeper or full editable clone (`--depth N` / `--depth 0`) adds these on top of the HEAD pack; git reads them for older history and the client never hand-parses them.

***Archive.*** The same `HEAD` file bytes, zstd-compressed and split into chunks. Each chunk is made of independent frames, so the client can download many at once and start writing files as the first bytes land. It's the fastest path to a working tree, but the files don't go into `.git`, so git content commands don't work. This is what **files** mode uses.

### Two-phase publish

Most clones just want the latest `HEAD`, so a sync builds that first and the rest later.

- **Phase 1.** The server builds everything a `--depth 1` clone needs: the skeleton, the index, and a pack with `HEAD`'s files. This is ready fast and served right away. The ref says `build_status: "full history building"` so clients know more is coming.
- **Phase 2.** In the background, the server builds the full history, then the archive — reusing whatever didn't change since the last sync. When it's done, it clears `build_status`.

A clone that only wants `HEAD` is ready as soon as phase 1 finishes. An editable full clone (`--depth 0`) is ready once the history lands — it reads the working tree from the packs and never touches the archive, so only `files` mode waits for the archive. While a phase is still building, the server returns `202` and the client retries on its own.

### Content-defined chunking and cheap re-syncs

The archive is split by content, not by fixed size: cut points land on the data itself (frames run about 1–16 MB). Each frame is compressed on its own and named by the hash of its bytes.

This makes re-syncs cheap. When a new commit lands, frames that didn't change hash to the same name and are reused as-is — no recompressing, no re-uploading. Only the frames that actually changed get rebuilt, and the builder reads just those changed regions, so the work matches the size of the diff, not the whole repo. The same is true of everything else a sync builds: only the commits and objects new since the last sync get packed, and re-syncing a commit that's already built does no work at all. A re-sync costs about what the diff costs.

### Sync performance

How long a sync takes to build the artifacts (server-side, the same hardware as the clone numbers above). There's no git equivalent — git builds nothing ahead of time.

| repo | phase 1 (depth=1 clone-ready) | phase 2 (full history, background) |
|---|---|---|
| `facebook/react` | 5.4 s | +32 s |
| `oven-sh/bun` | ~8 s | +13 s |
| `torvalds/linux` | ~40 s | very large |

Phase 1 is what a `--depth 1` clone waits for; phase 2 runs in the background and only gates full clones. react's phase 2 is a cold first build; bun's is much shorter because the incremental re-sync reuses unchanged history levels and archive frames. linux's phase 1 is dominated by building the HEAD-closure pack for its ~95k-file tree, and its full history is large enough that we don't pre-build it on the dev box.

> In production the server syncs on push, so this happens once per commit, ahead of any clone — by the time a CI runner or agent asks for the repo, the artifacts are already built.

## Install

Pick whichever fits. All install the `ripclone` CLI (and `ripclone-server`, `git-remote-ripclone`).

```sh
# 1. Shell installer (prebuilt binaries)
curl -fsSL https://github.com/russellromney/ripclone/releases/latest/download/install.sh | sh

# 2. Cargo (builds from source; also `cargo add ripclone` to embed the client lib)
cargo install ripclone --locked

# 3. pip (prebuilt wheel)
pip install ripclone
```

The prebuilt binaries link their C libraries (libgit2, openssl, zstd) dynamically; on Linux install the runtime packages (`libgit2`, `libssl3`), on macOS `brew install libgit2 openssl@3`. `cargo install` builds them from source instead.

Check your version and whether the configured server is compatible:

```sh
ripclone --version
ripclone version            # CLI + server versions, with a compatibility verdict
ripclone update             # check for a newer release
```

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
            "${{ vars.RIPCLONE_URL }}/v1/repos/github/${{ github.repository_owner }}/${{ github.event.repository.name }}/sync"
```

The `github` in the path is the provider instance (see [Providers](#providers)). For private repos the server needs read access to the upstream — configure a token for the provider, or pass one per request in the `X-Upstream-Token` header.

ripclone validates the `RIPCLONE_TOKEN`, syncs the mirror, builds artifacts for the new `HEAD`, and returns the artifact hashes.

## CLI usage

By default the CLI talks to the managed [Ripclone Cloud](https://ripclone.com). Point it at a self-hosted server with `--server`, the `RIPCLONE_SERVER` env var, or `ripclone login`. (Resolution order: `--server` > `RIPCLONE_SERVER` > saved login config > cloud.)

```bash
# Authorize this machine against the cloud (saves a token), or sign out
ripclone login
ripclone logout

# Show CLI + server versions and compatibility, and check for a newer release
ripclone version
ripclone update

# Clone a repo (public or private) — github is the default provider
ripclone clone owner/repo
ripclone clone owner/repo --branch feat/x --dir ./my-dir

# Another host: prefix the repo, or pass --provider (see Providers below)
ripclone clone gitlab:mygroup/project
ripclone --provider my-gitea clone owner/repo

# Working tree only (no git object database), fastest for files-only jobs
ripclone clone owner/repo --mode files

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

For a private repo, pass an upstream credential with `--token`. The client sends it as `X-Upstream-Token` and the server translates it to the host's auth form (GitHub, GitLab, Gitea, …):

```bash
ripclone --token ghp_xxx clone my-org/private-repo
```

Pushes go to your git host directly, not through ripclone.

## Providers

By default ripclone knows one host: the built-in `github` instance. To mirror from GitLab, Gitea/Forgejo/Codeberg, Bitbucket, or a self-hosted host, register provider instances on the server with the `RIPCLONE_PROVIDERS` environment variable (or a JSON config file):

```bash
export RIPCLONE_PROVIDERS='[
  {"id":"gitlab","kind":"gitlab","host":"gitlab.com"},
  {"id":"company-gitea","kind":"gitea","host":"git.example.com","token":"gitea-token"}
]'
```

Supported `kind` values: `github`, `gitlab`, `bitbucket`, `gitea`, `generic`. A `generic` host needs an `auth_template` (e.g. `"token {token}"`) so ripclone knows how to build the auth header. Then address a repo by instance id — `gitlab:mygroup/project` on the CLI, or `/v1/repos/gitlab/mygroup/project/...` on the API.

## Architecture

```
┌─────────────────┐
│  push / CI hook │  triggers a sync on every push (token-authenticated)
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
- **Your git host stays the source of truth** for repos, refs, permissions, and writes.
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
- `RIPCLONE_MODE` — default clone mode (`editable` or `files`) when `--mode` is omitted.
- `RIPCLONE_CACHE_DIR` / `RIPCLONE_NO_CACHE` — opt in to (or force off) a local artifact cache; off by default.

Server-side storage and retention (S3-compatible backends, remote GC, local eviction) are configured through `RIPCLONE_S3_*`, `RIPCLONE_RETENTION_*`, and `RIPCLONE_REMOTE_GC_*` variables; see `docs/BACKENDS.md` and `CHANGELOG.md` for the full list.

## License

ripclone is licensed under the [Elastic License 2.0](LICENSE).

You may use, modify, and distribute the software freely. You may not provide
ripclone to third parties as a hosted or managed service. See the full text in
[`LICENSE`](LICENSE) for details.
