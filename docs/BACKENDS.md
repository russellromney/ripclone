# Storage backends

`ripclone-server` can store artifacts on the local filesystem or in any S3-compatible object store.

## Local filesystem (default, easiest for self-hosting)

If you do not set any S3 environment variables, the server stores artifacts in its CAS directory (`--cas-dir`, default `/data/cache`). This is the path used by `docker-compose.yml`.

Pros:
- Works out of the box.
- No external account or egress costs.
- Fast when the server and client are on the same machine or LAN.

Cons:
- The server must proxy every byte if clients are remote.
- No built-in CDN for distributed clients.

## S3-compatible object storage

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

## AWS S3 Express One Zone (highest performance)

For a hosted service where you want the lowest latency and highest throughput to clients, use an **S3 Express One Zone directory bucket**.

1. Create a directory bucket in the AWS region closest to your users, e.g. `usw2-az1`.
2. Use the S3 Express endpoint pattern:

```bash
RIPCLONE_S3_ENDPOINT=https://my-bucket--usw2-az1--x-s3.s3express-us-west-2.amazonaws.com
RIPCLONE_S3_REGION=us-west-2
RIPCLONE_S3_BUCKET=my-bucket--usw2-az1--x-s3
```

S3 Express is significantly faster than standard S3 for the small, range-heavy reads ripclone clients make. Cost is higher, so it is best for the hosted/new-user path rather than the default open-source setup.

## CDN in front of S3

If you want a custom domain or edge cache in front of S3/Tigris/R2, put a CDN or reverse proxy between clients and the object store and point `RIPCLONE_S3_ENDPOINT` at it. The server generates presigned S3-style URLs against that endpoint; the CDN/proxy must forward the `Authorization` header and request path to the origin.

A future improvement is to support provider-specific signed URLs (e.g. CloudFront signed URLs) directly in the server.

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

For the same commit as the main clone, it reuses the local `.git/index` and object database, so nothing is downloaded. For a different branch/commit, it falls back to fetching the prebuilt index and head-blobs pack from the server.

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

| Use case | Backend | Why |
|---|---|---|
| Local dev / single machine | Local filesystem | Zero setup |
| Small team self-host | MinIO or S3 | Shared storage, still simple |
| Hosted service / new users | S3 Express One Zone or R2 | Fastest global downloads |
| Cost-sensitive hosted | R2 + client cache | No egress fees |
