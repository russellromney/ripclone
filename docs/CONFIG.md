# Configuration

This is the authoritative list of supported `RIPCLONE_*` environment variables.
Internal tuning knobs use code constants at their current defaults.

## User

- `RIPCLONE_SERVER` - server URL. Equivalent to `--server`.
- `RIPCLONE_SERVER_TOKEN` - raw shared server token. The client hashes it before
  sending `Authorization: Ripclone <sha256>`.
- `RIPCLONE_SERVER_TOKEN_HASH` - pre-hashed shared server token for CI or secret
  stores.
- `RIPCLONE_UPSTREAM_TOKEN` - upstream provider credential sent as
  `X-Upstream-Token`. Equivalent to `--token`.
- `RIPCLONE_MODE` - default clone mode when `--mode` is omitted: `editable` or
  `files`.
- `RIPCLONE_AGENT` - agent-fleet mode. Truthy (`1`/`true`/`yes`/`on`) sets
  fleet-sane clone defaults: **depth-1** history and no interactive prompts. An
  explicit falsey value overrides an `agent = true` config default. Explicit
  `--depth`/`[clone] depth` still win. See [Agents & CI](AGENTS.md). Config key:
  top-level `agent = true`.
- `RIPCLONE_VERIFY_UPSTREAM` - `auto`, `always`, or `never`.
- `RIPCLONE_CACHE_DIR` - opt in to the local artifact cache.
- `RIPCLONE_NO_CACHE` - disable the local artifact cache even if configured.
- `RIPCLONE_NO_METRICS` - skip the fire-and-forget clone metrics POST.

## Operator

- `RIPCLONE_CONFIG` - path to the global `config.toml`.
- `RIPCLONE_PROVIDERS` - JSON provider registry override.
- `RIPCLONE_GITHUB_TOKEN` - token shortcut for the built-in GitHub provider.
- `RIPCLONE_SERVER_TOKEN` / `RIPCLONE_SERVER_TOKEN_HASH` - server auth for
  clients and self-hosted servers.
- `RIPCLONE_S3_ENDPOINT`, `RIPCLONE_S3_REGION`, `RIPCLONE_S3_BUCKET`,
  `RIPCLONE_S3_PREFIX`, `RIPCLONE_S3_CACHE_DIR` - object storage backend.
- `RIPCLONE_METADATA`, `RIPCLONE_METADATA_DB_URL`,
  `RIPCLONE_METADATA_DB_TOKEN` - metadata/ref-store backend.
- `RIPCLONE_QUEUE`, `RIPCLONE_QUEUE_DB_URL`, `RIPCLONE_QUEUE_DB_TOKEN` - build
  queue backend.
- `RIPCLONE_WEBHOOK_SECRET_<PROVIDER>`, `RIPCLONE_WEBHOOK_ALLOWLIST`,
  `RIPCLONE_WEBHOOK_WARM_ALL` - webhook authentication and warming policy.
- `RIPCLONE_POLL_INTERVAL_SECS` - fallback polling interval; `0` disables it.
- `RIPCLONE_REMOTE_GC_INTERVAL_SECS`, `RIPCLONE_REMOTE_GC_GRACE_SECS`,
  `RIPCLONE_REMOTE_GC_DRY_RUN` - remote object garbage collection.

## Expert

These remain because tests or deployment safety need them, but they should not
be tuned casually.

- `RIPCLONE_FETCH_MAX_ATTEMPTS`, `RIPCLONE_FETCH_BACKOFF_MS` - client download
  retry budget.
- `RIPCLONE_IO_URING` - Linux worktree writer selection: unset/`auto`, `0`, or
  `1`.
- `RIPCLONE_FSYNC` - force durable local writes where supported.
- `RIPCLONE_JWT_SECRET`, `RIPCLONE_JWT_TTL_SECS`,
  `RIPCLONE_JWT_SESSION_MAX_SECS` - session-token signing and lifetime.
- `RIPCLONE_HEAD_REBASE_BYTES` - test/expert threshold for HEAD delta rebasing.
- `RIPCLONE_SIGNED_URL_TTL_SECS`, `RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS` -
  signed artifact URL lifetimes.
- `RIPCLONE_REF_CACHE_TTL_SECS` - in-process ref cache TTL.
- `RIPCLONE_RECHECK_MAX` - post-build freshness re-check cap.
- `RIPCLONE_LSM`, `RIPCLONE_LSM_MAX_LEVELS` - incremental history compaction.
- `RIPCLONE_TRUST_GATEWAY`, `RIPCLONE_TRUST_FORWARDED_FOR` - trust-boundary
  controls for self-hosted gateways/proxies.
- `RIPCLONE_BENCH` - emit structured sync benchmark logs.

The old short token and server-url aliases were removed before 1.0. Use the
explicit server names above.
