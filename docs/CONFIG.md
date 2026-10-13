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
- `RUST_LOG` - server/worker tracing filter, normally `info`; use `debug` for a
  bounded diagnostic run.
- `RIPCLONE_PROVIDERS` - JSON provider registry override.
- `RIPCLONE_SERVER_TOKEN` / `RIPCLONE_SERVER_TOKEN_HASH` - server auth for
  clients and self-hosted servers.
- `RIPCLONE_S3_ENDPOINT`, `RIPCLONE_S3_REGION`, `RIPCLONE_S3_BUCKET`,
  `RIPCLONE_S3_PREFIX` - object storage backend.
- `RIPCLONE_RETENTION_INTERVAL_SECS`, `RIPCLONE_RETENTION_MAX_AGE_DAYS`,
  `RIPCLONE_RETENTION_MAX_GB` - age/size trimming for the local build cache when
  S3-compatible storage is configured. These settings never delete local
  durable storage, S3 objects, or exact results.
- `RIPCLONE_CONTROL_DB_PATH` - server-owned SQLite database path.
- `RIPCLONE_TURSO_DATABASE_URL`, `RIPCLONE_TURSO_AUTH_TOKEN` - paired Turso
  primary settings enabling embedded-replica control mode.
- `RIPCLONE_QUEUE_API_URL`, `RIPCLONE_METADATA_REPORT_URL`,
  `RIPCLONE_METADATA_JOB_TOKEN` - standalone worker claim/ref APIs and bearer.
- `RIPCLONE_QUEUE_STALE_SECS`, `RIPCLONE_QUEUE_MAX_ATTEMPTS`,
  `RIPCLONE_QUEUE_RETRY_BACKOFF_MS`, `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` -
  durable job recovery and retention policy.
- `RIPCLONE_SIZE_CLASSES` - JSON array of size classes for durable job claims
  (overrides `[[control.size_classes]]` in config.toml). Each entry:
  `{ "name", "max_bytes", "machine"? }`. Ordered small→large; launch default is
  `small` (≤1 GiB) | `large` (catch-all). Worker flag:
  `ripclone-worker --max-size-class <name>` claims only jobs at or below that
  class; omit the flag to claim everything (single-worker self-host unchanged).
- `RIPCLONE_WEBHOOK_SECRET_<PROVIDER>`, `RIPCLONE_WEBHOOK_ALLOWLIST` - webhook
  authentication and added-repository allowlist. Only the payload-identified
  default branch admits exact work.
- `RIPCLONE_POLL_INTERVAL_SECS` - fallback polling interval, default `300`
  (on); `0` disables it.
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
- `RIPCLONE_LSM`, `RIPCLONE_LSM_MAX_LEVELS` - incremental history compaction.
- `RIPCLONE_TRUST_GATEWAY`, `RIPCLONE_TRUST_FORWARDED_FOR` - trust-boundary
  controls for self-hosted gateways/proxies.
- `RIPCLONE_BENCH` - emit structured sync benchmark logs.

The old short token and server-url aliases were removed before 1.0. Use the
explicit server names above.
