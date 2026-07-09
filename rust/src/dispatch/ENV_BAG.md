# The ripclone worker env bag

This document freezes the environment-variable contract every compute provider
(`fly`, `exec`, `http`, k8s, custom, …) must deliver to a fresh
`ripclone-worker` process.

**Core rule:** the worker is platform-blind. A provider never needs to know
worker internals; its only job is to deliver this bag to a fresh process and let
the worker exit when its lifecycle flags say so.

**Decision D-A:** workers hold **NO database credentials**. The metadata target
is an ApiRefStore report URL plus a per-job token, never a direct DB URL or
password.

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
| **Metadata target** | `RIPCLONE_METADATA_REPORT_URL` | Yes | — | ApiRefStore report endpoint (Decision D-A: no DB creds). |
| | `RIPCLONE_METADATA_JOB_TOKEN` | Yes | — | Per-job token for the metadata report endpoint. |
| **Upstream-credential source** | `RIPCLONE_PROVIDERS` | One source required | — | JSON provider registry; supplies instance tokens and auth templates. |
| | `RIPCLONE_GITHUB_TOKEN` | alt | — | Static GitHub personal/token for the static broker. |
| | `RIPCLONE_GITHUB_APP_ID` | alt | — | GitHub App broker: app ID. |
| | `RIPCLONE_GITHUB_APP_INSTALLATION_ID` | Yes, with App ID | — | Installation ID for the app. |
| | `RIPCLONE_GITHUB_APP_PRIVATE_KEY` | one key var required with App ID | — | Inline PEM private key. |
| | `RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH` | alt | — | Path to PEM private key file. |
| | `RIPCLONE_GITHUB_API_BASE` | No | `https://api.github.com` | GitHub Enterprise / test API base. |
| **Ripclone token** | `RIPCLONE_TOKEN` | Yes | — | Shared ripclone authentication token presented by the worker to ripclone-controlled endpoints. |
| **Size-class ceiling** | `RIPCLONE_MAX_SIZE_CLASS` | No | — | Inclusive size-class ceiling this worker may claim. |
| **Lifecycle flags** | `RIPCLONE_IDLE_EXIT_SECS` | No | — | Exit after the queue has been empty this many seconds (scale-to-zero). |
| | `RIPCLONE_MAX_JOBS` | No | — | Exit after completing this many jobs (one-shot platforms). |

## Local fallbacks

If no S3 storage settings are present, storage falls back to local disk under
`cas_dir` (default `/data/cache`). If no upstream credential source is
configured, anonymous upstream clones are attempted.

## Provider checklist

Before starting a worker, a provider must set:

1. Queue backend + claim credentials.
2. Storage credentials (or confirm local-disk operation is intended).
3. Metadata target (`RIPCLONE_METADATA_REPORT_URL` + `RIPCLONE_METADATA_JOB_TOKEN`).
4. One upstream-credential source (`RIPCLONE_PROVIDERS`, `RIPCLONE_GITHUB_TOKEN`,
   or GitHub App vars).
5. `RIPCLONE_TOKEN`.
6. Optional: `RIPCLONE_MAX_SIZE_CLASS` and lifecycle flags.

That is the entire provider-facing surface. No CLI flags, no config files, no
platform-specific API knowledge.
