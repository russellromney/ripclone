# Backends

`ripclone-server` has three independent, pluggable backends. The defaults need
zero infrastructure — a single binary with local storage and an in-process
builder — and you swap any one of them out without touching the others:

- **Storage** — where artifacts live (local filesystem or S3-compatible).
- **Metadata store** — where per-repo/branch refs (the pointers into storage)
  live (files, S3, or a SQL database).
- **Build queue** — where sync/build jobs are dispatched (in-process, or a SQL
  queue drained by standalone `ripclone-worker` processes for farm-out).

Storage and the metadata store hold all durable state; the build queue is just
coordination. A worker is therefore stateless — that is what lets builds be
farmed out to other machines.

## How to configure them

Each setting can come from an **environment variable** or from `config.toml`
(`~/.config/ripclone/config.toml`). Precedence is **env var > `config.toml` >
built-in default**, so an env var always wins. The sections below list the env
vars; the same values live under `[storage]`, `[metadata]`, and `[queue]` in the
file.

Set the file values with the CLI (writes the global `config.toml`, `0600`):

```bash
ripclone backend queue    --backend postgres --url postgres://user:pass@host:5432/ripclone
ripclone backend metadata --backend postgres --url postgres://user:pass@host:5432/ripclone
ripclone backend storage  --backend s3 --bucket my-bucket --endpoint https://s3.example.com
ripclone backend show     # effective values; flags which env var overrides each
```

Credentials are **never** read from `config.toml`: S3 keys come from
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, and `libsql` DB tokens may be set
in-file for now (`[queue].token` / `[metadata].token`) or via env.

## Storage

`ripclone-server` can store artifacts on the local filesystem or in any S3-compatible object store.

### Local filesystem (default, easiest for self-hosting)

If you do not set any S3 environment variables, the server stores artifacts in its CAS directory (`--cas-dir`, default `/data/cache`). This is the path used by `tests/fly/docker-compose.yml`.

Pros:
- Works out of the box.
- No external account or egress costs.
- Fast when the server and client are on the same machine or LAN.

Cons:
- The server must proxy every byte if clients are remote.
- No built-in CDN for distributed clients.

### S3-compatible object storage

Set these environment variables:

```bash
RIPCLONE_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
RIPCLONE_S3_REGION=us-east-1
RIPCLONE_S3_BUCKET=my-ripclone-bucket
RIPCLONE_S3_PREFIX=artifacts/          # optional
RIPCLONE_S3_CACHE_DIR=/data/cache      # local on-disk cache for hot reads
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

The server will redirect clients to signed URLs so bytes are served directly from the object store rather than proxied through the ripclone server.

Works with:
- **Tigris** (current default for the hosted service)
- **MinIO** (great for on-prem)
- **Cloudflare R2** (no egress fees, good global performance)
- **AWS S3**
- Any other S3-compatible provider

### AWS S3 Express One Zone (highest performance)

For a hosted service where you want the lowest latency and highest throughput to clients, use an **S3 Express One Zone directory bucket**.

1. Create a directory bucket in the AWS region closest to your users, e.g. `usw2-az1`.
2. Use the S3 Express endpoint pattern:

```bash
RIPCLONE_S3_ENDPOINT=https://my-bucket--usw2-az1--x-s3.s3express-us-west-2.amazonaws.com
RIPCLONE_S3_REGION=us-west-2
RIPCLONE_S3_BUCKET=my-bucket--usw2-az1--x-s3
```

S3 Express is significantly faster than standard S3 for the small, range-heavy reads ripclone clients make. Cost is higher, so it is best for the hosted/new-user path rather than the default open-source setup.

### CDN in front of S3

If you want a custom domain or edge cache in front of S3/Tigris/R2, put a CDN or reverse proxy between clients and the object store and point `RIPCLONE_S3_ENDPOINT` at it. The server generates presigned S3-style URLs against that endpoint; the CDN/proxy must forward the `Authorization` header and request path to the origin.

A future improvement is to support provider-specific signed URLs (e.g. CloudFront signed URLs) directly in the server.

## Metadata store

The metadata store holds one small `RefInfo` record per repo/branch — the commit
and the hashes that point at artifacts in storage. It never holds file bytes.
Choose it with `RIPCLONE_METADATA`, independently of storage:

| `RIPCLONE_METADATA` | Where refs live | Notes |
|---|---|---|
| *(unset)* | follows storage | S3 when S3 is configured, else local files — the historical default |
| `file` | `--repo-root/.ripclone-refs/` | one JSON file per ref |
| `s3` | the configured S3 bucket | requires `RIPCLONE_S3_*` |
| `sqlite` | a local SQLite file | single box |
| `postgres` | a Postgres database | shared across machines |
| `mysql` | a MySQL database | shared across machines |
| `libsql` | a remote Turso Cloud database | shared across machines |

The SQL backends read a connection URL (and a token for `libsql`):

```bash
RIPCLONE_METADATA=postgres
RIPCLONE_METADATA_DB_URL=postgres://user:pass@host:5432/ripclone
# mysql:  RIPCLONE_METADATA_DB_URL=mysql://user:pass@host:3306/ripclone
# sqlite: RIPCLONE_METADATA_DB_URL=/data/meta.db
# libsql: RIPCLONE_METADATA_DB_URL=libsql://db.turso.io  RIPCLONE_METADATA_DB_TOKEN=...
```

`libsql` is remote-only — for a local SQLite file use `sqlite`. The schema is
created on first start; no migration step. Put the metadata store on a database
your workers can also reach when you farm builds out (below).

## Build queue & workers (farm-out)

By default the server builds in-process: `/sync` runs the build on the server
itself. To move that CPU/IO-heavy work onto one or more separate machines, point
the server and one or more `ripclone-worker` processes at a shared **SQL queue**.
The server only enqueues; the workers claim, build, and write results to the
shared storage + metadata store. `/sync` polls the job to completion, so it does
not care which machine built it.

Choose the queue with `RIPCLONE_QUEUE`:

| `RIPCLONE_QUEUE` | Backend | Use |
|---|---|---|
| `local` *(default)* | in-process channel | single binary, no farm-out |
| `sqlite` | a local SQLite file | single-box farm-out (server + workers share the file) |
| `postgres` | a Postgres database | multi-machine farm-out |
| `mysql` | a MySQL database | multi-machine farm-out |
| `libsql` | a remote Turso Cloud database | multi-machine farm-out |

```bash
# Server: enqueue onto Postgres (builds run in workers, not here)
RIPCLONE_QUEUE=postgres
RIPCLONE_QUEUE_DB_URL=postgres://user:pass@host:5432/ripclone
# libsql also needs RIPCLONE_QUEUE_DB_TOKEN=...

# Worker(s): same queue + storage + metadata config, plus a scratch repo root
ripclone-worker --cas-dir /data/cache --repo-root /data/repos
```

Notes:

- **Same config on both sides.** A worker must see the same storage
  (`RIPCLONE_S3_*` or the shared `--cas-dir`), the same metadata store
  (`RIPCLONE_METADATA*`), and the same queue (`RIPCLONE_QUEUE*`) as the server.
  With a SQL queue, use a SQL metadata store too — a `file` store under a
  per-machine `--repo-root` would not be shared.
- **One `repo_root` per worker.** The bare mirror under `--repo-root` is per-repo
  scratch guarded by an in-process lock; give each worker its own. All durable
  state is in storage + the metadata store.
- **Credentials are never put in the queue.** A worker resolves its own upstream
  credentials from its provider config (`RIPCLONE_PROVIDERS` / `RIPCLONE_GITHUB_TOKEN`),
  so a per-request `X-Upstream-Token` is ignored on the cross-process path.
- **Keep async builds on.** `/sync` only enqueues when async builds are enabled
  (`RIPCLONE_ASYNC_BUILD`, on by default). With it off the server builds
  synchronously in-process and the queue is unused.

Worker tuning and queue housekeeping:

```bash
# worker flags
--idle-poll-ms 1000        # how often to poll an empty queue (default 1000)

# queue env (server + workers)
RIPCLONE_QUEUE_STALE_SECS=1800              # reclaim a crashed worker's claimed job after N s (default 1800)
RIPCLONE_QUEUE_FAILED_RETENTION_SECS=604800 # prune failed jobs older than N s (default 7 days)
```

`done` jobs are kept as build history; only `failed` jobs are pruned.

> Truly diskless workers (no bare mirror on disk, seeded from the clonepack
> instead of a fresh fetch) are future work — see the dispatcher design. Today a
> worker fetches the bare mirror it needs, and a server answering a clone fetches
> it on demand if it lacks one.

## Client authentication

If the server is configured with `RIPCLONE_TOKEN`, the client must send a SHA-256 hash of that token in the `Authorization` header. The client never sends the raw secret.

```bash
# Provide the raw token; the client hashes it before sending.
RIPCLONE_TOKEN=your-secret ripclone clone owner/repo

# Or provide the pre-hashed token directly (useful in CI / 1Password / .env files).
RIPCLONE_TOKEN_HASH=sha256-of-your-secret ripclone clone owner/repo
```

`git-remote-ripclone` reads the same variables.

## Client-side cache

The `ripclone` client has no local cache by default. This avoids filling disk with multi-gigabyte artifact copies during benchmarks or one-off clones.

Enable caching explicitly to make repeated clones of the same repo/commit almost entirely local:

```bash
RIPCLONE_CACHE_DIR=~/.cache/ripclone ripclone clone owner/repo
```

Environment variables:

```bash
RIPCLONE_CACHE_DIR=/path/to/cache   # enable / override cache location
RIPCLONE_NO_CACHE=1                  # forcibly disable caching
```

## Fast worktrees on Linux

`ripclone worktree <path> -b <branch>` adds a git worktree using the same overlay-staging trick as `ripclone clone`. Run it inside an existing ripclone clone:

```bash
cd my-clone
ripclone worktree ../my-clone-wt -b HEAD
```

For the same commit as the main clone, it reuses the local `.git/index` and object database, so nothing is downloaded. For a different branch/commit, it falls back to fetching the prebuilt depth pack from the server.

On cloud VMs with slow overlay rootfs, point the staging directory at a fast volume:

```bash
RIPCLONE_STAGING_DIR=/data ripclone worktree ../wt -b HEAD
```

Other overlay knobs:

```bash
RIPCLONE_NO_OVERLAY=1                # disable overlay staging
RIPCLONE_OVERLAY_THRESHOLD_MB=50     # only stage repos larger than this
RIPCLONE_OVERLAY_MARGIN_MB=128       # headroom required in staging dir
```

## Recommended matrix

| Use case | Storage | Metadata | Queue | Why |
|---|---|---|---|---|
| Local dev / single machine | Local filesystem | *(default)* | `local` | Zero setup, one binary |
| Small team self-host | MinIO or S3 | *(default = s3)* | `local` | Shared storage, still simple |
| Single box, offload builds | S3 / MinIO | `sqlite` or `s3` | `sqlite` | Server + workers on one host share the queue file |
| Multi-machine farm-out | S3 / R2 | `postgres`/`mysql`/`libsql` | `postgres`/`mysql`/`libsql` | Workers on other hosts share a network DB |
| Hosted service / new users | S3 Express One Zone or R2 | SQL | SQL | Fastest downloads + farm-out builds |
| Cost-sensitive hosted | R2 + client cache | SQL | SQL | No egress fees |
