# The ripclone worker env bag

This document freezes the environment-variable contract every compute provider
(`fly`, `exec`, `http`, k8s, custom, …) must deliver to a fresh
`ripclone-worker` process.

**Core rule:** the worker is platform-blind. A provider's only job is to
deliver this bag to a fresh process.

**Decision D-A (shipped as `RIPCLONE_METADATA=api`):** farmed-out workers should
hold **no database credentials**. Set `RIPCLONE_METADATA=api` with
`RIPCLONE_METADATA_REPORT_URL` (the server's `POST /v1/refs`) and
`RIPCLONE_METADATA_JOB_TOKEN` (HMAC bearer minted at enqueue, scoped to the
job's repo). The worker POSTs each ref-write; the **server** (which holds the
DB creds) performs the durable write. Self-host single-box may still use
direct SQL metadata (`RIPCLONE_METADATA=sqlite|postgres|mysql|libsql` +
`RIPCLONE_METADATA_DB_URL`) — that path is unchanged.

`size_class` is part of the provider-facing [`WorkerSpec`](mod.rs) (a config-driven
lane name such as `small` or `large`), not an env var.

## Required vs optional

| Category | Env var | Required | Default | Purpose |
|----------|---------|----------|---------|---------|
| **Queue (claim)** | `RIPCLONE_QUEUE` | Yes | `local` | Queue backend. Worker farm-out requires `sqlite`, `postgres`, `mysql`, or `libsql`. |
| | `RIPCLONE_QUEUE_DB_URL` | Yes, when `RIPCLONE_QUEUE` is SQL | — | DB path/URL for SQL queues. |
| | `RIPCLONE_QUEUE_DB_TOKEN` | Yes, when `RIPCLONE_QUEUE=libsql` | — | Auth token for remote libsql queue. |
| **Storage (upload)** | `RIPCLONE_S3_ENDPOINT` | Yes, for S3 | — | S3-compatible endpoint. Also accepts `AWS_ENDPOINT_URL_S3`. |
| | `RIPCLONE_S3_REGION` | No | `us-east-1` | S3 region. Also accepts `AWS_REGION`. |
| | `RIPCLONE_S3_BUCKET` | Yes, for S3 | — | Target bucket. Also accepts `BUCKET_NAME`. |
| | `RIPCLONE_S3_PREFIX` | No | — | Object key prefix. |
| | `RIPCLONE_S3_CACHE_DIR` | No | — | Local cache dir for S3 backend. |
| | `AWS_ACCESS_KEY_ID` | Yes, for S3 | — | S3 access key. |
| | `AWS_SECRET_ACCESS_KEY` | Yes, for S3 | — | S3 secret key. |
| | `AWS_SESSION_TOKEN` | No | — | Optional temporary S3 session token. |
| **Metadata target** (direct DB — self-host / legacy) | `RIPCLONE_METADATA` | No | `file` (follows storage) | Metadata backend: `file` \| `s3` \| `sqlite` \| `postgres` \| `mysql` \| `libsql` \| `api`. Farm-out prefers `api` (no DB creds on the worker). Direct SQL still works when the operator accepts workers holding DB creds. |
| | `RIPCLONE_METADATA_DB_URL` | Yes, when `RIPCLONE_METADATA` is SQL | — | DB path/URL for SQL metadata. **Do not set on farm-out workers using `api`.** |
| | `RIPCLONE_METADATA_DB_TOKEN` | Yes, when `RIPCLONE_METADATA=libsql` | — | Auth token for remote libsql metadata. **Do not set on farm-out workers using `api`.** |
| **Metadata target** (`api` — preferred for farm-out) | `RIPCLONE_METADATA_REPORT_URL` | Yes, when `RIPCLONE_METADATA=api` | — | Absolute `http(s)` URL of the server's `POST /v1/refs` report endpoint. Missing → worker fails at startup. |
| | `RIPCLONE_METADATA_JOB_TOKEN` | Yes, when `RIPCLONE_METADATA=api` | — | Per-job HMAC bearer token (`rcjt1.…`) minted by the server at enqueue, scoped to the job's repo (optional job id). Sent as `Authorization: Bearer …`. Missing → worker fails at startup. Bad/expired/wrong-scope → 401, no write. |
| **Upstream-credential source** | `RIPCLONE_PROVIDERS` | One source required | — | JSON provider registry; supplies instance tokens and auth templates. |
| | `RIPCLONE_GITHUB_TOKEN` | alt | — | Static GitHub personal/token for the static broker. |
| | `RIPCLONE_GITHUB_APP_ID` | alt | — | GitHub App broker: app ID. |
| | `RIPCLONE_GITHUB_APP_INSTALLATION_ID` | Yes, with App ID | — | Installation ID for the app. |
| | `RIPCLONE_GITHUB_APP_PRIVATE_KEY` | one key var required with App ID | — | Inline PEM private key. |
| | `RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH` | alt | — | Path to PEM private key file. |
| | `RIPCLONE_GITHUB_API_BASE` | No | `https://api.github.com` | GitHub Enterprise / test API base. |
| **Ripclone token** (reserved, not read yet) | `RIPCLONE_TOKEN` | — | — | Reserved name for a future shared ripclone authentication token. Job-report auth uses `RIPCLONE_METADATA_JOB_TOKEN` instead. |
| **Size-class ceiling** | `RIPCLONE_MAX_SIZE_CLASS` | No | — | Inclusive size-class ceiling this worker may claim. |
| **Lifecycle flags** | `RIPCLONE_IDLE_EXIT_SECS` | No | — | Exit after the queue has been empty this many seconds (scale-to-zero). |
| | `RIPCLONE_MAX_JOBS` | No | — | Exit after completing this many jobs (one-shot platforms). |

## Local fallbacks

If no S3 storage settings are present, storage falls back to local disk under
`cas_dir` (default `/data/cache`). If `RIPCLONE_METADATA` is unset, metadata
follows storage (`s3` if S3 storage is configured, else `file`) — `file` only
works when every worker shares the server's filesystem, so farm-out deploys
must set `RIPCLONE_METADATA` explicitly (`api` preferred). If no upstream
credential source is configured, anonymous upstream clones are attempted.

## Provider checklist

Before starting a worker, a provider must set:

1. Queue backend + claim credentials.
2. Storage credentials (or confirm local-disk operation is intended).
3. Metadata target:
   - **Farm-out (preferred):** `RIPCLONE_METADATA=api` +
     `RIPCLONE_METADATA_REPORT_URL` + `RIPCLONE_METADATA_JOB_TOKEN`. No
     `RIPCLONE_METADATA_DB_URL` / `RIPCLONE_METADATA_DB_TOKEN`.
   - **Self-host / legacy direct write:** `RIPCLONE_METADATA` +
     `RIPCLONE_METADATA_DB_URL` (+ `RIPCLONE_METADATA_DB_TOKEN` for libsql).
4. One upstream-credential source (`RIPCLONE_PROVIDERS`, `RIPCLONE_GITHUB_TOKEN`,
   or GitHub App vars).
5. Optional: `RIPCLONE_MAX_SIZE_CLASS` and lifecycle flags.

That is the entire provider-facing surface today. No CLI flags, no config
files, no platform-specific API knowledge.

## Farm-out worker env (no DB creds)

Example bag for a one-shot farmed-out worker:

```bash
RIPCLONE_QUEUE=sqlite|postgres|mysql|libsql
RIPCLONE_QUEUE_DB_URL=…          # claim only — not the metadata DB
# RIPCLONE_QUEUE_DB_TOKEN=…      # when queue is libsql

RIPCLONE_METADATA=api
RIPCLONE_METADATA_REPORT_URL=https://ripclone.example/v1/refs
RIPCLONE_METADATA_JOB_TOKEN=rcjt1.…   # minted at enqueue for this job/repo

# storage (S3 or local) …
# upstream credential source …
# optional: RIPCLONE_MAX_SIZE_CLASS, RIPCLONE_IDLE_EXIT_SECS, RIPCLONE_MAX_JOBS
```

**Do not set** `RIPCLONE_METADATA_DB_URL` or `RIPCLONE_METADATA_DB_TOKEN` on
that worker. The server that serves `POST /v1/refs` holds those.
