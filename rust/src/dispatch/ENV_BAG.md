# The ripclone worker env bag

This document freezes the environment-variable contract every compute provider
(`fly`, `exec`, `http`, k8s, custom, …) must deliver to a fresh
`ripclone-worker` process.

**Core rule:** the worker is platform-blind. A provider's only job is to
deliver this bag to a fresh process. (Today that bag includes a real metadata
DB credential — see Decision D-A.)

**Decision D-A (target, not yet implemented):** the design intent is for
workers to hold **no database credentials** — the metadata target should be an
ApiRefStore report URL plus a per-job token, never a direct DB URL or
password. `ripclone-worker` does not implement ApiRefStore reporting yet: it
connects to the metadata store directly, the same way the server does (see
`select_metadata` in `backends.rs`). Until ApiRefStore ships, a provider must
give the worker real DB credentials via `RIPCLONE_METADATA_DB_URL` (below) —
that is a live D-A violation, tracked as follow-up work, not a doc error.

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
| **Metadata target** (today) | `RIPCLONE_METADATA` | No | `file` (follows storage) | Metadata backend: `file` \| `s3` \| `sqlite` \| `postgres` \| `mysql` \| `libsql`. Farm-out (workers on separate hosts) requires a shared backend — `s3` or a SQL kind, never `file`. |
| | `RIPCLONE_METADATA_DB_URL` | Yes, when `RIPCLONE_METADATA` is SQL | — | DB path/URL for SQL metadata (a **direct DB credential** — see Decision D-A above). |
| | `RIPCLONE_METADATA_DB_TOKEN` | Yes, when `RIPCLONE_METADATA=libsql` | — | Auth token for remote libsql metadata. |
| **Metadata target** (target design, not read by any code yet) | `RIPCLONE_METADATA_REPORT_URL` | — | — | Reserved name for the future ApiRefStore report endpoint (Decision D-A). Setting this today does nothing. |
| | `RIPCLONE_METADATA_JOB_TOKEN` | — | — | Reserved name for the future per-job ApiRefStore token. Setting this today does nothing. |
| **Upstream-credential source** | `RIPCLONE_PROVIDERS` | One source required | — | JSON provider registry; supplies instance tokens and auth templates. |
| | `RIPCLONE_GITHUB_TOKEN` | alt | — | Static GitHub personal/token for the static broker. |
| | `RIPCLONE_GITHUB_APP_ID` | alt | — | GitHub App broker: app ID. |
| | `RIPCLONE_GITHUB_APP_INSTALLATION_ID` | Yes, with App ID | — | Installation ID for the app. |
| | `RIPCLONE_GITHUB_APP_PRIVATE_KEY` | one key var required with App ID | — | Inline PEM private key. |
| | `RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH` | alt | — | Path to PEM private key file. |
| | `RIPCLONE_GITHUB_API_BASE` | No | `https://api.github.com` | GitHub Enterprise / test API base. |
| **Ripclone token** (reserved, not read yet) | `RIPCLONE_TOKEN` | — | — | Reserved name for a future shared ripclone authentication token the worker would present to ripclone-controlled endpoints (e.g. ApiRefStore, once it exists). `ripclone-worker` does not read this today — it has no outbound HTTP calls of its own to authenticate. |
| **Size-class ceiling** | `RIPCLONE_MAX_SIZE_CLASS` | No | — | Inclusive size-class ceiling this worker may claim. |
| **Lifecycle flags** | `RIPCLONE_IDLE_EXIT_SECS` | No | — | Exit after the queue has been empty this many seconds (scale-to-zero). |
| | `RIPCLONE_MAX_JOBS` | No | — | Exit after completing this many jobs (one-shot platforms). |

## Local fallbacks

If no S3 storage settings are present, storage falls back to local disk under
`cas_dir` (default `/data/cache`). If `RIPCLONE_METADATA` is unset, metadata
follows storage (`s3` if S3 storage is configured, else `file`) — `file` only
works when every worker shares the server's filesystem, so farm-out deploys
must set `RIPCLONE_METADATA` explicitly. If no upstream credential source is
configured, anonymous upstream clones are attempted.

## Provider checklist

Before starting a worker, a provider must set:

1. Queue backend + claim credentials.
2. Storage credentials (or confirm local-disk operation is intended).
3. Metadata target: `RIPCLONE_METADATA` + `RIPCLONE_METADATA_DB_URL` (+
   `RIPCLONE_METADATA_DB_TOKEN` for libsql) for farm-out today. (`RIPCLONE_TOKEN`
   and the ApiRefStore report-URL/token pair are reserved for the D-A target
   design — do not rely on them yet.)
4. One upstream-credential source (`RIPCLONE_PROVIDERS`, `RIPCLONE_GITHUB_TOKEN`,
   or GitHub App vars).
5. Optional: `RIPCLONE_MAX_SIZE_CLASS` and lifecycle flags.

That is the entire provider-facing surface today. No CLI flags, no config
files, no platform-specific API knowledge — but see Decision D-A above: the
metadata credential is a real DB credential until ApiRefStore ships.
