<p align="center">
  <img src="assets/logo.png" alt="ripclone logo" width="200">
</p>

# ripclone

ripclone is the fastest way to clone git repos. In our benchmarks, clones were
6x-30x faster than `git clone` from GitHub.

ripclone pre-builds git artifacts for every pushed commit so that agents, CI
systems, and humans can clone a repo and start working in seconds instead of
waiting for a full `git clone`. It is **read-only** and **clone-only**: it does
not proxy pushes. You use normal git with your own credentials for writes.

It is self-hostable and host-agnostic: point it at GitHub, GitLab, Gitea, or any
git host, public or private.

ripclone started from a question asked by
[Jarred Sumner](https://x.com/jarredsumner/status/2066420871753838913):

> *"It's hard to imagine why cloning a git repo should be much slower than
> downloading an equivalent-sized file. Where are the experiments with custom
> git clients that clone faster?"*

ripclone is one answer. The goal: get a `git clone` as close as possible to
downloading a file from object storage.

| Principle | In practice |
|---|---|
| **Move slow work off the clone** | Negotiation, indexing, and the tree walk run once on the server after a push, never on the clone. The client downloads finished pieces and writes them. |
| **Parallelize the downloads** | Packs and the archive are content-addressed chunks, so a clone is many parallel GETs, not one serial stream. |
| **Keep every resource busy** | Download, decode, and write overlap, so network, CPU, and disk stay busy. |
| **Process as little as possible** | A new commit rebuilds only what the diff touched; a clone fetches only the artifacts its mode needs. |

> **Status:** pre-release. There are no published binaries or packages yet;
> build from source.

## Clone modes

| Command | Same as | You get | Use it when |
|---|---|---|---|
| `ripclone clone owner/repo` | `git clone` | A full editable git repo with complete history, every branch and every tag | Coding: you or an agent will switch branches, read history, push. |
| `ripclone clone owner/repo --depth 1` | `git clone --depth 1` | An editable git repo with only `HEAD` and the tags it reaches | CI and agents that run git but don't walk history. |
| `ripclone clone owner/repo --mode files` | `git archive` of the tip | The working tree only, no `.git` | Builds that never run git. Fastest. |

**Git LFS:** ripclone stores LFS pointer files, never the blobs. Run
`git lfs pull` after cloning.

See **[Design](docs/DESIGN.md)** for how the artifacts are built.

## Performance

At 1 Gbps, client and server in one Hetzner region, median of 3 runs, cold
client cache. git cloned from GitHub:

| Repo | ripclone full | ripclone depth=1 | ripclone files | `git clone` | `git clone --depth 1` |
|---|---|---|---|---|---|
| `oven-sh/bun` | 2.37 s · **30×** | 0.46 s · **15×** | 0.42 s · **17×** | 71.6 s | 7.03 s |
| `pandas-dev/pandas` | 2.05 s · **13×** | 0.14 s · **18×** | 0.12 s · **21×** | 25.9 s | 2.51 s |
| `torvalds/linux` | 58.8 s · **6.5×** | 2.30 s · **8.3×** | 1.78 s · **11×** | 384 s | 19.2 s |

Full clones are compared with `git clone`; depth-1 and files with
`git clone --depth 1`. GitHub's speed varies from run to run, so treat the
ratios as one sample. Build times, S3 numbers and caveats:
**[Benchmarks](docs/BENCHMARKS.md)**.

## Build

Requirements: Rust 1.96 or newer (CI uses the version pinned in
`rust-toolchain.toml`), `protoc`, and `cmake` (for `zlib-ng`). At run time the
server and editable clones need `git` on the `PATH`; files-only clones do not.
Without it, the command stops before any work with "system Git is required".

```bash
cd rust
cargo build --release
# binaries: target/release/ripclone, target/release/ripclone-server
```

## Quick start

Start a server. Server and CLI authenticate with the same token, and the
server refuses to start without one:

```bash
export RIPCLONE_SERVER_TOKEN=$(openssl rand -hex 32)
./target/release/ripclone-server
```

The server keeps its artifacts, mirrors, and `control.db` under
`~/.local/share/ripclone/` by default (`--data-dir` moves them; `--host` and
`--port` set the listen address, default `0.0.0.0:8000`). With local storage,
`cache` is the durable artifact store. For object storage see [Server state and storage](docs/BACKENDS.md).

Add a repo, then clone it:

```bash
export RIPCLONE_SERVER=http://localhost:8000
ripclone add oven-sh/bun          # registers the repo and starts its first build
ripclone clone oven-sh/bun bun
```

`add` returns once the build is accepted; `--wait` waits for it to finish.
`ripclone list` and `ripclone rm owner/repo` manage registrations. Removing a
registration keeps the published artifacts; `rm --purge` also deletes the
repo's builds, its refs, its mirror, and the artifacts no other repo uses. Without `RIPCLONE_SERVER` (or
`server` in the config file) the CLI talks to `https://ripclone.com`.

## Keeping repos fresh

The server builds each new commit on the default branch. Three triggers, from
fastest to slowest:

- **Push webhook** at `/webhooks/{github|gitlab|gitea}`. See
  [Webhooks](docs/WEBHOOKS.md).
- **GitHub Actions**: call `POST /v1/repos/github/{owner}/{repo}/builds` with
  `{"selector": "<pushed sha>"}` on push.
  A ready-to-copy workflow is in
  [`docs/examples/github-actions-trigger.yml`](docs/examples/github-actions-trigger.yml).
- **Polling**: the server `ls-remote`s added repos every 5 minutes
  (`RIPCLONE_POLL_INTERVAL_SECS`; `0` turns it off).

`ripclone sync owner/repo` triggers a build by hand. What a clone does while
a new commit is still building: **[Freshness](docs/FRESHNESS.md)**. The HTTP
API behind all of these: **[API](docs/API.md)**.

## CLI

```bash
ripclone clone owner/repo [dir] [--branch B] [--depth 0|1] [--mode editable|files]
ripclone clone owner/repo --at <ref>        # a full commit SHA, branch, or tag
ripclone clone owner/repo --wait            # wait for the exact full build, no top-up
ripclone clone gitlab:group/project         # another provider
ripclone clone https://github.com/owner/repo.git  # a pasted clone URL works anywhere owner/repo does
ripclone clone owner/repo --quiet           # no progress or notes on stderr
ripclone clone owner/repo --progress        # progress on stderr even when it is not a terminal
ripclone sync owner/repo                    # build the default branch tip
ripclone sync owner/repo --at <ref> [--wait]
ripclone add owner/repo [--wait]
ripclone list
ripclone rm owner/repo [--purge]
ripclone provider add|list|rm|test          # provider instances in this CLI's config; test runs git ls-remote
ripclone version                            # CLI and server versions, with a compatibility check
ripclone login | logout                     # sign in to https://ripclone.com
```

`dir` defaults to the repository name. As with `git clone`, it may be an
existing empty directory; a non-empty one is refused. The branch tracks
`origin`, so `git pull` and `git push` work in the clone; `origin` is the
upstream from this CLI's provider config. `--branch` picks the branch to
clone; with `--at` it names the local branch instead. `--depth` takes `0`
(full history, the default) or `1`. `sync` builds failed outputs of the commit
again; with `--at` it also waits. `--wait` on `sync`, `add`, or `clone` polls
at most 40 times, about 70 seconds; run the command again to keep waiting.

All settings: **[Configuration](docs/CONFIG.md)**.

**Agents and CI.** Use `--depth 1` when the agent runs git but doesn't walk
history, and `--mode files` when it never runs git. Server errors are JSON `{ "error", "code" }`
([API](docs/API.md#errors)). The CLI's exit code says what kind of failure it
was:

| Exit | Meaning | Action |
|---|---|---|
| 0 | Success | |
| 1 | Any other failure (integrity, git, local I/O) | Read the message |
| 2 | Bad arguments | Fix the command |
| 3 | Not authenticated (401) | Log in or set the token |
| 4 | Forbidden (403) | Get access |
| 5 | Not found (404): repo not added, upstream repo not found, unknown ref, missing artifact | Add the repo or fix the name or ref |
| 6 | Server unreachable, rate limited, or unavailable (429, 5xx) | Retry |
| 130 | Interrupted (Ctrl-C); staging is removed before exit | |

**Private repos.** The server needs read access to the upstream: a token or a
GitHub App key in its environment, per provider
([Providers](docs/PROVIDERS.md#credentials)). The config file holds no
secrets. The CLI reads the upstream (top-up, `--verify-upstream`) with your
own git credentials.

**Checking the server.** `--verify-upstream` (`auto` by default) cross-checks an
editable clone's commit against the git host when it can. `always` also checks
that the clone's objects connect from that commit (`git fsck
--connectivity-only`), which adds seconds to a large full clone. A Files clone
has no git objects to check, so `--verify-upstream=always` fails for one.

## Architecture

```
push / webhook / poll ──▶ ripclone-server ──▶ control.db (SQLite)
                              │  mirror upstream, build artifacts
                              ▼
                          artifact store (local disk or S3-compatible)
                              ▲
             ripclone CLI ────┘  download in parallel, verify hashes, write tree
```

The server resolves each request to one exact commit, builds that commit's
artifacts once, and serves them. Artifacts are content-addressed and never
change after publication. With S3-compatible storage, clients download
directly through signed URLs. Your git host stays the source of truth for
repos, permissions, and writes.

Ops endpoints, all without the token: `GET /healthz`, `GET /readyz` (503 if
storage or the control database is down), `GET /v1/version`, and
`GET /metrics` (Prometheus).

## Telemetry

The CLI sends a metrics report only when the server asks for one: the build
answer carries an `x-ripclone-clone-id` header. The server in this repo never
sends it, so a self-hosted server gets no reports. When asked, the CLI sends
one fire-and-forget POST to that server after a successful clone: clone id,
repo, commit, mode, whether it waited for a build, wall time,
bytes, and client OS/arch/version. It never affects the clone's result.
`RIPCLONE_NO_METRICS=1` turns it off.

## Self-hosted and hosted

Self-hosted ripclone is free and complete. Your server owns the repo list,
builds, clone policy, and artifact storage. The hosted service at
`https://ripclone.com` is an optional convenience. The CLI uses it only when no
server is configured. The code in this repository has no accounts, billing, or
tenant logic.

## Support

- **Platforms:** Linux (x86-64, arm64) and macOS (x86-64, arm64). Windows is
  not supported.
- **Rust:** 1.96 or newer.
- **Wire protocol:** the client and server must speak the same protocol
  version (`GET /v1/version`). A mismatch fails with HTTP 426 and a message
  to upgrade, not with a partial clone.
- **Stored formats:** artifacts, mirrors, and `control.db` are internal. A
  pre-release version may need a fresh cache.

## Contributing

Bug fixes on the supported platforms are welcome as pull requests. New storage
backends, databases, providers, or product features need a design issue first.
CI runs `scripts/ci.sh`; `bash scripts/ci.sh lint` and `bash scripts/ci.sh test`
run the same checks locally.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
