<p align="center">
  <img src="assets/logo.png" alt="ripclone logo" width="200">
</p>

# ripclone

ripclone is the fastest way to clone git repos. Large repos see 5x-10x speedup; small repos are also a bit faster.

ripclone pre-builds git artifacts for every pushed commit so that agents, CI systems, and humans can clone a repo and start working in seconds instead of waiting for a full `git clone`. It is **read-only** and **clone-only**: it does not proxy commits or pushes. You use normal git with your own credentials for writes.

It works as two operations: you **sync** a repo so the server pre-builds its artifacts (automatic on every push), then you **clone** it — the client downloads those artifacts and writes the working tree in seconds.

It is self-hostable and host-agnostic — point it at GitHub, GitLab, Gitea, or any git host — and works for private or public repos. For the easiest experience, sign up for free (for public repos) at [Ripclone Cloud](https://ripclone.com).

ripclone started from a simple question asked by [Jarred Sumner](https://x.com/jarredsumner/status/2066420871753838913): 

> *"It's hard to imagine why cloning a git repo should be much slower than downloading an equivalent-sized file. Where are the experiments with custom git clients that clone faster?"* 

ripclone is one answer. The goal: get a `git clone` as close as possible to downloading a file from object storage. A few design principles get there:

| Principle | In practice |
|---|---|
| **Move slow work off the clone** | Negotiation, indexing, and the tree walk run once on the server at sync — never on the clone. The client just downloads finished pieces and writes them. |
| **Parallelize the downloads** | Packs and the archive are content-addressed chunks, so a clone is many parallel range-GETs, not one serial stream. |
| **Keep every resource busy** | Every stage runs across all cores *and* overlaps in time — the next starts the moment the last produces output. Network, CPU, and disk never idle (io_uring on Linux). |
| **Process as little as possible** | A re-sync rebuilds only what the diff touched; a clone fetches only the artifacts its mode needs — files mode skips the object database entirely. |

## Clone

On every push, ripclone prebuilds a **clonepack** for `HEAD` — a set of files in object storage laid out so a clone has almost nothing left to do but download and write. Pick how much you pull with `--mode`:

`--mode=editable` (the default) installs a full editable git repo by default, matching `git clone` history semantics while downloading prebuilt packs in parallel. Use `--depth 1` when you want the smaller HEAD-only clonepack for CI or agent work.

`--mode=files` writes the working tree straight from a zstd archive — the fastest path when you only need the files (agents, CI). No git object database, so `git diff`/`show` don't work.

**Git LFS:** LFS objects come from your git host, not ripclone — run `git lfs pull` after cloning to fetch them. ripclone stores the LFS pointer files (it never stores the blobs); resolving them is a pass-through to your host by design.

See **[Design](docs/DESIGN.md)** for how a clonepack is built and synced.

## Performance

ripclone pre-builds git artifacts so clones are faster than `git clone`. On fast links the wins are largest; as bandwidth drops the download itself dominates and the gap narrows. At 1 Gbps, on a Fly.io `performance-8x` client against `ripclone-server-dev` (median of 3 runs, cold client cache):

| Repo (1 Gbps) | ripclone full | ripclone depth=1 | ripclone files | `git clone` full |
|---|---|---|---|---|
| `oven-sh/bun` | 3.4 s · **11.7×** | 1.0 s · **3.3×** | 0.63 s · **5.4×** | 40.3 s |
| `pandas-dev/pandas` | 3.0 s · **7.6×** | 0.32 s · **6.0×** | 0.26 s · **7.4×** | 22.8 s |

*Mode labels:* `ripclone full` / `ripclone depth=1` are the `editable` mode with `--depth 0` / `--depth 1`; `ripclone files` is `files` mode (HEAD worktree only).

A real high-bandwidth `torvalds/linux` run on EC2 hits **~10×** at 5 Gbps for the full history and **~6×** for depth-1. Full sweep (250/500 Mbps + 1 Gbps, the EC2 high-bandwidth rows, the files-vs-tar.gz comparison, and honest caveats) is in **[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md)**.

## Install

Pick whichever fits. All install the `ripclone` CLI (and `ripclone-server`, `ripclone-worker`, `git-remote-ripclone`).

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

Hitting a snag? See **[Troubleshooting](docs/TROUBLESHOOTING.md)** (missing libgit2, `202` warming, `401` vs `403`, config drift).

### Uninstall

Remove whichever way you installed:

```sh
# Shell installer / prebuilt binaries — delete the installed binaries
rm -f "$(command -v ripclone)" "$(command -v ripclone-server)" \
      "$(command -v ripclone-worker)" "$(command -v git-remote-ripclone)"

# Cargo
cargo uninstall ripclone

# pip
pip uninstall ripclone
```

To wipe local state as well: the CLI's saved login lives at `~/.config/ripclone/` and a server's cache and bare mirrors default to `~/.local/share/ripclone/` (`cache` and `repos`). Removing ripclone touches nothing on your git host.

## Quick start

Build and run the server:

```bash
cd rust
cargo build --release

# The server requires a token; both the server and CLI read it from
# RIPCLONE_SERVER_TOKEN. Generate one and start the server:
export RIPCLONE_SERVER_TOKEN=$(openssl rand -hex 32)
./target/release/ripclone-server
```

The server defaults to storing its local cache and bare mirrors under
`~/.local/share/ripclone/` (`cache` and `repos`). Use `--cas-dir` and
`--repo-root` to override. `--host` (default `0.0.0.0`) and `--port` (default
`8000`) set the listen address. Object storage (S3/R2/Tigris/MinIO) and most
other tuning are set with environment variables — see [`docs/BUILD_OPTIONS.md`](docs/BUILD_OPTIONS.md)
and [`docs/BACKENDS.md`](docs/BACKENDS.md).

Build artifacts for a commit (sync the repo on the server):

```bash
# Uses the same RIPCLONE_SERVER_TOKEN for authentication
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

Add a workflow to a repo so ripclone builds artifacts on every push. Set `RIPCLONE_SERVER` as a repository variable and `RIPCLONE_SERVER_TOKEN` as a repository secret. (A ready-to-copy version lives in [`docs/examples/github-actions-trigger.yml`](docs/examples/github-actions-trigger.yml).)

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
            -H "Authorization: Ripclone ${{ secrets.RIPCLONE_SERVER_TOKEN }}" \
            "${{ vars.RIPCLONE_SERVER }}/v1/repos/github/${{ github.repository_owner }}/${{ github.event.repository.name }}/sync"
```

The `github` in the path is the provider instance (see [`docs/PROVIDERS.md`](docs/PROVIDERS.md)). For private repos the server needs read access to the upstream — configure a token for the provider, or pass one per request in the `X-Upstream-Token` header.

ripclone validates the `RIPCLONE_SERVER_TOKEN`, syncs the mirror, builds artifacts for the new `HEAD`, and returns the artifact hashes.

### Native push webhook (no per-repo workflow)

Instead of (or alongside) the Actions workflow, point a provider webhook at the server so it builds on every push with nothing added to the consumer repo. Set a per-provider secret, then add a repository/org webhook:

- **Payload URL:** `https://ripclone.example.com/webhooks/github` (`/v1/webhooks/github` is a back-compat alias)
- **Content type:** `application/json`
- **Secret:** the value of `RIPCLONE_WEBHOOK_SECRET_GITHUB` (the legacy `RIPCLONE_WEBHOOK_SECRET` is still honored for github)
- **Events:** the `push` event.

The server verifies the provider HMAC (`X-Hub-Signature-256`) over the raw body — constant-time, before any parse — then triggers a build via the same queue `/sync` uses, so artifacts are ready before any clone. Fail-closed: a provider with no configured secret returns `503`; a bad signature `401`. Branch deletes clean up that ref; tags/ping are acknowledged with no build.

By default the **default branch** is always warmed and other branches only if already built (so throwaway branches don't warm). Set `RIPCLONE_WEBHOOK_WARM_ALL=1` to warm every pushed branch, or `RIPCLONE_WEBHOOK_ALLOWLIST` to restrict which repos warm (comma-separated; GitHub repos are `owner/repo`, other providers are prefixed: `gitlab/group/sub/proj`). The receiver is provider-agnostic — **GitHub, GitLab, and Gitea/Forgejo** are supported (point the provider at `/webhooks/{provider}`, e.g. `/webhooks/gitlab`). Two per-provider notes: GitLab must use the **secret-token** webhook setting (the value of `X-Gitlab-Token`), not the newer signing-token scheme; and a Gitea/Forgejo webhook must have the **Delete** event enabled for branch-delete cleanup to fire. See [`docs/WEBHOOKS.md`](docs/WEBHOOKS.md).

### Polling fallback

For repos without a webhook, or to catch a missed delivery, set `RIPCLONE_POLL_INTERVAL_SECS` (default `0` = off). The server periodically `ls-remote`s known repos and builds any whose tip moved. This is a backstop; webhooks/Actions are the prompt path.

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

# Speed knob: HEAD-only shallow history for CI/agents
ripclone clone owner/repo --depth 1

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

For editable clones you can have the client cross-check the installed tip against
your git host with `--verify-upstream`. In `auto` mode (the default) this runs
whenever an upstream credential is available or the repo is public; for
credential-less private/agent flows it warns and skips, leaving the ripclone
server in the trust base. `files`-mode clones are not verifiable this way.

### Plain `git clone` through the helper

The `git-remote-ripclone` binary lets stock `git` clone and fetch through a ripclone server with no wrapper — handy for CI steps and tools that shell out to `git`:

```sh
git clone ripclone://github/oven-sh/bun.git bun
```

It supports `--depth 1` (shallow) or a full clone; push goes to your git host via `pushInsteadOf`. See [`docs/REMOTE_HELPER.md`](docs/REMOTE_HELPER.md) for URL syntax, server resolution, and the push workaround.

## Providers

ripclone is host-agnostic: point it at GitHub (built in), GitLab, Gitea/Forgejo/Codeberg, or a self-hosted host by registering provider instances with `RIPCLONE_PROVIDERS` or `config.toml`. See [`docs/PROVIDERS.md`](docs/PROVIDERS.md).

## Architecture

```
┌──────────────────┐
│  push / CI hook  │  POST /sync
└────────┬─────────┘
         ▼
┌──────────────────┐   enqueue   ┌──────────────────┐
│ ripclone-server  │ ──────────▶ │    sync queue    │
│ resolve · serve  │             │ in-process / SQL │
└────────┬─────────┘             └────────┬─────────┘
         │ serves                  claim  │
         ▼                                ▼
      clients               ┌──────────────────┐
                            │ ripclone-worker  │ ×N
                            │  fetch · build   │
                            └────────┬─────────┘
                                     ▼ writes
                       ┌────────────────────────────┐
                       │  artifact store · metadata │
                       │  object/local · SQLx/file  │
                       └────────────────────────────┘
```

ripclone splits into a **server** — it resolves refs, serves artifacts, and enqueues a sync job on every push — and one or more **workers** (`ripclone-worker`) that claim jobs from the queue, `git fetch` the upstream, and build the clonepack. On a single box the worker runs inside the server; with a SQL queue you run a farm of workers across machines. Three backends are pluggable, each set with environment variables (see [`docs/BACKENDS.md`](docs/BACKENDS.md)):

- **Artifact store.** Where clonepacks live: object storage (S3 / R2 / Tigris / MinIO), with signed URLs so clients read straight from it, or local disk. Local disk also caches hot artifacts in front of object storage. A background GC drops artifacts nothing references (after a grace period, so an in-flight upload is never deleted).
- **Metadata store.** The ref → clonepack mapping and build status. Any database SQLx supports (Postgres, MySQL, SQLite, libsql/Turso), or a file / object-storage store. Writes are ordered so a newer sync never loses to an older one.
- **Sync queue.** Pending build jobs: in-process for a single box, or SQL-backed so workers can claim jobs across machines. Upstream credentials are resolved per worker and never stored in the queue.

**Your git host stays the source of truth** for repos, refs, permissions, and writes. Clients download artifacts (signed URL or server proxy), decompress, and write files straight to disk. Public endpoints are rate-limited.

Ops endpoints: `GET /healthz` (alive?), `GET /readyz` (ready? — `503` if storage or the ref store is down), and `GET /metrics` (Prometheus format). There's also a plain-git fallback (`/v1/git/{owner}/{repo}/...`) so a normal `git clone` still works if the fast path is down.

## Build options

Compile flags (e.g. building without `zlib-ng`), client tuning knobs (`RIPCLONE_FETCH_*`, `RIPCLONE_IO_URING`, `RIPCLONE_MODE`, cache and `RIPCLONE_FSYNC` durability), and the server-side backend environment variables all live in [`docs/BUILD_OPTIONS.md`](docs/BUILD_OPTIONS.md). Backend details are in [`docs/BACKENDS.md`](docs/BACKENDS.md).

## Telemetry

After a successful clone, the CLI sends a single fire-and-forget metrics POST to the configured server. It is advertising-grade telemetry, never billing-grade, and never on the clone's critical path: a failure to send cannot change the clone's exit status. The report is skipped entirely when the server does not mint a clone id (self-host / older server), when `--no-metrics` is passed, or when `RIPCLONE_NO_METRICS` is set to any non-empty value.

The payload contains:

- `cloneId` — the server-minted clone id from the ref-resolve response.
- `repo` — `{ provider, owner, name }` of the cloned repo.
- `commit` — the resolved commit SHA.
- `mode` — `depth1`, `full`, or `files`.
- `cold` — whether the clone waited for a fresh build.
- `totalMs` — end-to-end clone wall time in milliseconds.
- `bytes` — total bytes downloaded.
- `downloadMs` — currently omitted in v1.
- `client` — `{ os, arch, ripcloneVersion }`.

Self-hosted servers accept and drop this POST at `POST /v1/clones/{cloneId}/metrics` so the CLI never spams its own server with 404s.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
