# Configuration

Settings come from flags, environment variables, and `config.toml`
(`~/.config/ripclone/config.toml`, or the path in `RIPCLONE_CONFIG`). Flags win
over env, env wins over the file.

This file lists the settings that are staying. Settings for features being
removed (see [`internal/rebuild/PLAN.md`](internal/rebuild/PLAN.md)) are not
listed.

## Client

| Variable | Meaning |
|---|---|
| `RIPCLONE_SERVER` | Server URL. Same as `--server`. |
| `RIPCLONE_SERVER_TOKEN` | Shared server token. The client sends `Authorization: Ripclone <sha256(token)>`. |
| `RIPCLONE_SERVER_TOKEN_HASH` | The token's SHA-256 hash, for secret stores that hold only the hash. |
| `RIPCLONE_MODE` | Default `--mode`: `editable` or `files`. |
| `RIPCLONE_AGENT` | Truthy: depth 1 by default and no interactive prompts. Config key `agent = true`. |
| `RIPCLONE_VERIFY_UPSTREAM` | `auto` (default), `always`, or `never`. |
| `RIPCLONE_CACHE_DIR` | Turn on the local artifact cache at this path. Off by default. |
| `RIPCLONE_NO_CACHE` | Force the cache off. |
| `RIPCLONE_NO_METRICS` | Skip the post-clone metrics POST. |
| `RIPCLONE_POLL_MS` | Wait between polls while a build is pending or a ref is resolving, default 2000. Tests set it low. |
| `RIPCLONE_FETCH_MAX_ATTEMPTS`, `RIPCLONE_FETCH_BACKOFF_MS` | Download retry budget (defaults 3 and 100 ms). |
| `RIPCLONE_FSYNC` | Flush the tree, index, and rename before reporting success. Off by default, like `git checkout`. |
| `RIPCLONE_BENCH` | Same as `--bench`. On the server, emits structured build timing logs. |

## Server

Flags: `--cas-dir`, `--repo-root`, `--control-db`, `--host` (default
`0.0.0.0`), `--port` (default `8000`).

| Variable | Meaning |
|---|---|
| `RIPCLONE_SERVER_TOKEN` / `RIPCLONE_SERVER_TOKEN_HASH` | The token clients must present. |
| `RUST_LOG` | Tracing filter, normally `info`. |
| `RIPCLONE_CONTROL_DB_PATH` | SQLite control database path. Config: `[control] path`. |
| `RIPCLONE_PROVIDERS` | JSON provider registry. See [Providers](PROVIDERS.md). |
| `RIPCLONE_S3_ENDPOINT`, `RIPCLONE_S3_REGION`, `RIPCLONE_S3_BUCKET`, `RIPCLONE_S3_PREFIX` | S3-compatible artifact storage. Credentials: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`. See [Server state and storage](BACKENDS.md). |
| `RIPCLONE_S3_REQUEST_TIMEOUT_SECS`, `RIPCLONE_S3_MAX_ATTEMPTS`, `RIPCLONE_S3_MAX_RETRY_DELAY_MS` | S3 client timeouts and retries. |
| `RIPCLONE_SIGNED_URL_TTL_SECS`, `RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS` | Signed artifact URL lifetimes for public and private repos. |
| `RIPCLONE_RETENTION_INTERVAL_SECS`, `RIPCLONE_RETENTION_MAX_AGE_DAYS`, `RIPCLONE_RETENTION_MAX_GB` | Trim the local build cache when S3 is the durable store. Never deletes S3 objects or published results. |
| `RIPCLONE_WEBHOOK_SECRET_<PROVIDER>` | Webhook secret per provider instance. See [Webhooks](WEBHOOKS.md). |
| `RIPCLONE_WEBHOOK_ALLOWLIST` | Optional comma-separated repos that webhooks may build. |
| `RIPCLONE_POLL_INTERVAL_SECS` | Fallback polling interval, default `300`; `0` turns it off. |
| `RIPCLONE_LS_REMOTE_TIMEOUT_SECS` | Timeout for each upstream `ls-remote`, default 30. |
| `RIPCLONE_RATE_LIMIT_PER_SEC`, `RIPCLONE_RATE_LIMIT_BURST` | Rate limit for public endpoints. |
| `RIPCLONE_TRUST_FORWARDED_FOR` | Use `X-Forwarded-For` for rate limiting behind a trusted proxy. |
| `RIPCLONE_LSM_MAX_LEVELS` | Maximum history levels before compaction. |
| `RIPCLONE_HEAD_REBASE_BYTES` | Threshold for rebasing the Head delta. Expert setting. |

### GitHub App credentials

Instead of a static token, the server can mint short-lived installation tokens
for GitHub repos:

| Variable | Meaning |
|---|---|
| `RIPCLONE_GITHUB_APP_ID` | App id. Setting it turns the GitHub App broker on. |
| `RIPCLONE_GITHUB_APP_INSTALLATION_ID` | Installation id. Required with the app id. |
| `RIPCLONE_GITHUB_APP_PRIVATE_KEY` or `RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH` | The app's PEM private key, inline or as a file. |
| `RIPCLONE_GITHUB_API_BASE` | GitHub API base URL for GitHub Enterprise, `https://` only. Default `https://api.github.com`. |
