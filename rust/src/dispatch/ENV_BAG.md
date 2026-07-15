# The ripclone worker env bag

This document freezes the environment-variable contract every compute provider
(`fly`, `exec`, `http`, k8s, custom, …) must deliver to a fresh
`ripclone-worker` process.

**Core rule:** the worker is platform-blind. A provider's only job is to
deliver this bag to a fresh process.

**Decision D-A (shipped): farm-out workers hold NO database credentials.** Both
the queue and the metadata store are one DB, held only by the **server**. A
farmed-out worker reaches them entirely over HTTP with a single bearer token:

- **Queue:** `RIPCLONE_QUEUE=api` + `RIPCLONE_QUEUE_API_URL` (the server base
  URL). The worker claims/acks/heartbeats via `POST /v1/jobs/*`.
- **Metadata:** `RIPCLONE_METADATA=api` + `RIPCLONE_METADATA_REPORT_URL` (the
  server's `POST /v1/refs`). The worker POSTs each ref-write.
- **One token:** `RIPCLONE_METADATA_JOB_TOKEN` — a signed, expiring HMAC bearer
  (`rcjt1.…`) with no repo or job scope, because it is injected into a pooled
  worker that may claim any repo's job. It authenticates **all four** endpoints.
  It is the worker's whole credential.

**Token delivery is by provisioning, not per-dispatch minting.** An operator mints
one durable token (`ripclone mint-worker-token`, default 90d) and provisions it:

- **Fly (launch):** each pre-provisioned pooled machine carries the api env + the
  token as a **Fly machine secret** + storage creds, and **no** `*_DB_URL` /
  `*_DB_TOKEN`. `FlyProvider` just starts the machine (it does not inject env). The
  "no DB creds" guarantee on Fly is the provisioning — the machine physically has
  none.
- **exec / http (self-host escape hatch, typically local/trusted):** the
  dispatcher **forwards** the token + api config from its own env into each
  `WorkerSpec.env` (it does not mint). It sets `RIPCLONE_QUEUE=api` /
  `RIPCLONE_METADATA=api` and fails loudly at startup if api mode is configured
  but no `RIPCLONE_METADATA_JOB_TOKEN` is available to forward.

The four `_DB_URL`/`_DB_TOKEN` keys are absent from `WORKER_ENV_KEYS`. A 401 on any
endpoint → the worker exits cleanly and is respawned with a fresh token. Rotate by
re-minting. **Single-box self-host** keeps the direct SQL path
(`RIPCLONE_QUEUE=sqlite|…` + `RIPCLONE_QUEUE_DB_URL`, `RIPCLONE_METADATA=sqlite|…`)
— it is trusted and unchanged. Only the dispatcher-started / pooled farm-out path
is token-only.

`size_class` is part of the provider-facing [`WorkerSpec`](mod.rs) (a config-driven
lane name such as `small` or `large`), not an env var.

## Required vs optional

| Category | Env var | Required | Default | Purpose |
|----------|---------|----------|---------|---------|
| **Queue (claim)** | `RIPCLONE_QUEUE` | Yes | `local` | Queue backend. Token-only farm-out uses `api`; direct single-box uses `sqlite`. |
| | `RIPCLONE_QUEUE_API_URL` | Yes, when `RIPCLONE_QUEUE=api` | — | Server base URL serving `POST /v1/jobs/*`. No DB creds on the worker. |
| | `RIPCLONE_QUEUE_DB_URL` | Yes, when `RIPCLONE_QUEUE` is SQL (direct, single-box) | — | DB path/URL for SQL queues. **Never set on a farm-out worker.** |
| **Storage (upload)** | `RIPCLONE_S3_ENDPOINT` | Yes, for S3 | — | S3-compatible endpoint. Also accepts `AWS_ENDPOINT_URL_S3`. |
| | `RIPCLONE_S3_REGION` | No | `us-east-1` | S3 region. Also accepts `AWS_REGION`. |
| | `RIPCLONE_S3_BUCKET` | Yes, for S3 | — | Target bucket. Also accepts `BUCKET_NAME`. |
| | `RIPCLONE_S3_PREFIX` | No | — | Object key prefix. |
| | `RIPCLONE_S3_CACHE_DIR` | No | — | Local cache dir for S3 backend. |
| | `AWS_ACCESS_KEY_ID` | Yes, for S3 | — | S3 access key. |
| | `AWS_SECRET_ACCESS_KEY` | Yes, for S3 | — | S3 secret key. |
| | `AWS_SESSION_TOKEN` | No | — | Optional temporary S3 session token. |
| **Metadata target** (direct DB — single-box self-host only) | `RIPCLONE_METADATA` | No | `file` (follows storage) | Metadata backend: `file` \| `s3` \| `sqlite` \| `api`. Farm-out uses `api` (no DB creds); direct SQLite is single-box only. |
| | `RIPCLONE_METADATA_DB_URL` | Yes, when `RIPCLONE_METADATA` is SQL (direct) | — | DB path/URL for SQL metadata. **Never set on a farm-out worker.** |
| **Metadata target** (`api` — token-only farm-out) | `RIPCLONE_METADATA_REPORT_URL` | Yes, when `RIPCLONE_METADATA=api` | — | Absolute `http(s)` URL of the server's `POST /v1/refs` report endpoint. Missing → worker fails at startup. |
| | `RIPCLONE_METADATA_JOB_TOKEN` | Yes, when `RIPCLONE_QUEUE=api` or `RIPCLONE_METADATA=api` | — | The **one** signed, expiring HMAC bearer (`rcjt1.…`) for all four endpoints (claim/ack/heartbeat/refs); no repo or job scope. Operator-provisioned (`ripclone mint-worker-token`, default 90d) — a Fly machine secret, or forwarded by the dispatcher for exec/http. Sent as `Authorization: Bearer …`. Missing → worker fails at startup. Malformed/expired/wrong-secret → 401, no state change; the worker exits cleanly for respawn. |
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
must set `RIPCLONE_METADATA` explicitly. Token-only farm-out sets
`RIPCLONE_METADATA=api` (+ `RIPCLONE_QUEUE=api`); single-box self-host may use a
direct SQLite backend. If no upstream
credential source is configured, anonymous upstream clones are attempted.

## Provider checklist

Before starting a worker, a provider must set:

1. Queue backend + claim credentials.
2. Storage credentials (or confirm local-disk operation is intended).
3. Queue + metadata target:
   - **Token-only farm-out (`api`):** `RIPCLONE_QUEUE=api` +
     `RIPCLONE_QUEUE_API_URL` + `RIPCLONE_METADATA=api` +
     `RIPCLONE_METADATA_REPORT_URL` + `RIPCLONE_METADATA_JOB_TOKEN`. No
     `*_DB_URL` / `*_DB_TOKEN` — workers hold zero DB creds.
   - **Single-box direct SQLite (trusted):** `RIPCLONE_QUEUE=sqlite` +
     `RIPCLONE_QUEUE_DB_URL` + `RIPCLONE_METADATA=sqlite` +
     `RIPCLONE_METADATA_DB_URL`.
4. One upstream-credential source (`RIPCLONE_PROVIDERS`, `RIPCLONE_GITHUB_TOKEN`,
   or GitHub App vars).
5. Optional: `RIPCLONE_MAX_SIZE_CLASS` and lifecycle flags.

That is the entire provider-facing surface today. No CLI flags, no config
files, no platform-specific API knowledge.

## Token-only farm-out worker env (no DB creds)

Mint one durable token first: `ripclone mint-worker-token` (default 90d). Then
each farm-out worker carries:

```bash
RIPCLONE_QUEUE=api
RIPCLONE_QUEUE_API_URL=https://ripclone.example      # serves POST /v1/jobs/*

RIPCLONE_METADATA=api
RIPCLONE_METADATA_REPORT_URL=https://ripclone.example/v1/refs
RIPCLONE_METADATA_JOB_TOKEN=rcjt1.…   # the one bearer for all four endpoints

# storage (S3 or local) …
# upstream credential source …
# optional: RIPCLONE_MAX_SIZE_CLASS, RIPCLONE_IDLE_EXIT_SECS, RIPCLONE_MAX_JOBS
```

**No `*_DB_URL` / `*_DB_TOKEN` on that worker** — the server that serves the
endpoints holds the one queue + metadata DB. On Fly this bag is a machine
secret set on each pooled machine; for exec/http the dispatcher forwards it.
