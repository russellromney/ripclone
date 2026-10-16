# Configuration

Settings come from flags, environment variables, and TOML config files. A
flag beats the environment, and the environment beats a file.

Config files: `~/.config/ripclone/config.toml` (or the path in
`RIPCLONE_CONFIG`), and the first `ripclone.toml` found walking up from the
current directory. Keys in `ripclone.toml` beat the same keys in
`config.toml`. A file that does not parse stops the command.

## Server token

The server and the CLI share one token. The server refuses to start without
one.

| Variable | Meaning |
|---|---|
| `RIPCLONE_SERVER_TOKEN` | The shared token. Server and CLI hash it with SHA-256. |
| `RIPCLONE_SERVER_TOKEN_HASH` | The token's SHA-256 in hex, used instead of the token. Wins over `RIPCLONE_SERVER_TOKEN`. |

The CLI sends `Authorization: Ripclone <hash>`, and the server compares that
value with its stored hash. So the hash is itself a bearer credential: anyone
who has it can use the server. Keep `RIPCLONE_SERVER_TOKEN_HASH` as secret as
the token. Hashing gives no protection if the hash leaks.

## Client

| Setting | Meaning |
|---|---|
| `--server`, `RIPCLONE_SERVER`, config `server` | Server URL. Default `https://ripclone.com`. |
| `--provider`, config `default_provider` | Provider instance for a repo without a `provider:` prefix. Default `github`. |
| `--token`, `RIPCLONE_UPSTREAM_TOKEN` | Upstream credential for `--verify-upstream`. Beats the provider's configured token. Never sent to the ripclone server. |
| `RIPCLONE_MODE`, config `[clone] mode` | Default `--mode`: `editable` (default) or `files`. |
| config `[clone] depth` | Default `--depth`: `0` (full history) or `1`. |
| `RIPCLONE_AGENT`, config `agent` | Truthy (`1`, `true`, `yes`, `on`): depth 1 by default and no interactive prompts. An explicit or configured depth still wins. |
| `--verify-upstream`, `RIPCLONE_VERIFY_UPSTREAM` | `auto` (default), `always`, or `never`. |
| `RIPCLONE_CACHE_DIR` | Turn on the local artifact cache at this path. Off by default. |
| `RIPCLONE_NO_CACHE` | Set: no cache, even with `RIPCLONE_CACHE_DIR`. |
| `--no-metrics`, `RIPCLONE_NO_METRICS` | Never send the post-clone metrics report. |
| `RIPCLONE_POLL_MS` | Longest wait between polls while an output the CLI needs is pending, default 2000. Polls start at 100 ms and double up to this cap. A command stops after 40 polls. |
| `RIPCLONE_FETCH_MAX_ATTEMPTS`, `RIPCLONE_FETCH_BACKOFF_MS` | Attempts per artifact download and the first retry delay, defaults 3 and 100 ms. |
| `RIPCLONE_FSYNC` | `1` or `true`: flush the tree, index, and rename before reporting success. Off by default, like `git checkout`. |
| `--bench`, `RIPCLONE_BENCH` | Print a per-phase timing report after a clone. |
| `RIPCLONE_NO_BROWSER` | Set: `ripclone login` does not open a browser. |
| `RIPCLONE_ORIGIN_BASE` | Top up and verify repos of the built-in `github` provider from `<base>/<owner>/<repo>.git` instead of `github.com`. |
| `RIPCLONE_PROVIDERS`, config `[providers.<id>]` | Provider instances. See [Providers](PROVIDERS.md). |
| `RUST_LOG` | Tracing filter. |

Without `RIPCLONE_SERVER_TOKEN` or `RIPCLONE_SERVER_TOKEN_HASH`, the CLI uses
the token `ripclone login` saved in `~/.config/ripclone/tokens.json`.

## Server

| Flag | Meaning |
|---|---|
| `--cas-dir` | Local artifact store, default `~/.local/share/ripclone/cache`. With local storage it is the durable store. |
| `--repo-root` | Git mirrors, default `~/.local/share/ripclone/repos`. |
| `--control-db` | Control database. See `RIPCLONE_CONTROL_DB_PATH`. |
| `--host`, `--port` | Listen address, default `0.0.0.0` and `8000`. Port `0` picks a free port; the log names the bound address. |

| Variable | Meaning |
|---|---|
| `RIPCLONE_SERVER_TOKEN`, `RIPCLONE_SERVER_TOKEN_HASH` | The token clients must present. See [Server token](#server-token). |
| `RUST_LOG` | Tracing filter. Default `info`. |
| `RIPCLONE_CONTROL_DB_PATH`, config `[control] path` | SQLite control database. Default `control.db` in the parent of `--repo-root`. |
| `RIPCLONE_PROVIDERS`, config `[providers.<id>]` | Provider instances and their tokens. See [Providers](PROVIDERS.md). |
| `RIPCLONE_S3_ENDPOINT` (or `AWS_ENDPOINT_URL_S3`), config `[storage] endpoint` | S3-compatible endpoint. Setting it turns S3 storage on. See [Server state and storage](BACKENDS.md). |
| `RIPCLONE_S3_REGION` (or `AWS_REGION`), config `[storage] region` | Region, default `us-east-1`. |
| `RIPCLONE_S3_BUCKET` (or `BUCKET_NAME`), config `[storage] bucket` | Bucket. Required with S3. |
| `RIPCLONE_S3_PREFIX`, config `[storage] prefix` | Key prefix. |
| config `[storage] backend` | `local` or `s3`. `local` ignores the S3 settings; `s3` requires an endpoint. Unset: S3 when an endpoint is set. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | S3 credentials. Environment only. |
| `RIPCLONE_S3_REQUEST_TIMEOUT_SECS` | Timeout per S3 request, default 30. |
| `RIPCLONE_S3_MAX_ATTEMPTS` | Attempts per S3 request, default 5. |
| `RIPCLONE_S3_MAX_RETRY_DELAY_MS` | Longest wait between S3 retries, default 2000. |
| `RIPCLONE_SIGNED_URL_TTL_SECS` | Lifetime of signed artifact URLs, default 1200. |
| `RIPCLONE_RETENTION_INTERVAL_SECS`, `RIPCLONE_RETENTION_MAX_AGE_DAYS`, `RIPCLONE_RETENTION_MAX_GB` | Trim the local artifact cache when S3 is the durable store: how often, and the age and size limits. Defaults 300 seconds, 7 days, 100 GB; `0` turns each off. With local storage, retention is off. It never deletes S3 objects. |
| `RIPCLONE_WEBHOOK_SECRET_<PROVIDER>` | Webhook secret per provider instance. See [Webhooks](WEBHOOKS.md). |
| `RIPCLONE_WEBHOOK_ALLOWLIST` | Comma-separated repos that webhooks may build. Unset: every added repo. |
| `RIPCLONE_POLL_INTERVAL_SECS` | How often the server checks added repos for new commits, default 300; `0` turns polling off. |
| `RIPCLONE_LS_REMOTE_TIMEOUT_SECS` | Timeout for each upstream `ls-remote`, default 30; `0` means the default. |
| `RIPCLONE_FETCH_TIMEOUT_SECS` | Longest one upstream fetch into the server's mirror may run, default 3600; `0` means the default. A fetch that moves under 1000 bytes per second for 60 seconds stops sooner. A stopped fetch is a retryable build failure. |
| `RIPCLONE_RATE_LIMIT_BURST`, `RIPCLONE_RATE_LIMIT_PER_SEC` | Per-IP rate limit: burst size and refill per second, defaults 60 and 10. See [API](API.md#authentication). |
| `RIPCLONE_TRUST_FORWARDED_FOR` | `1` or `true`: rate-limit by the last `X-Forwarded-For` address. Only behind one trusted proxy. |
| `RIPCLONE_HEAD_SLOTS` | Builds that run Head at once. Default: a quarter of the CPU cores, at least 2. At least 1. |
| `RIPCLONE_FILES_SLOTS` | Builds that run Files at once. Default: a quarter of the CPU cores, at least 2. `0` pauses Files; Head and Full still build. |
| `RIPCLONE_FULL_SLOTS` | Builds that run Full at once. Default: a quarter of the CPU cores, at least 2. `0` pauses Full; Head and Files still build. A long Full never holds back another commit's Files. |
| `RIPCLONE_BUILD_RETRY_BASE_MS` | First retry delay after a retryable build failure, default 2000. Each retry doubles it, up to 300 seconds. |
| `RIPCLONE_BUILD_MAX_ATTEMPTS` | Build attempts per phase (Head, then Files, then Full) before the output fails, default 5. At least 1. |
| `RIPCLONE_LSM_MAX_LEVELS` | Most history levels a Full keeps before merging the smallest neighbors, default 16. |
| `RIPCLONE_HEAD_REBASE_BYTES` | When a commit's Head packs past their base reach this size, its Full build compacts them into a new base for later Heads. Default 128 MiB. |
| `RIPCLONE_ORIGIN_BASE` | Fetch repos of the built-in `github` provider from `<base>/<owner>/<repo>.git` instead of `github.com`. For mirrors and tests. |
| `RIPCLONE_BENCH` | Log a JSON timing report for each build. |

A repository runs at most one build of each kind at a time, so slots past one
per kind only help when several repositories build at once. After a
repository's first build, each kind builds only what changed since its base
and spends much of its time waiting on uploads, so every pool defaults to a
quarter of the cores, at least 2: 2 each on 8 cores, 4 on 16, 16 on 64. A
repository's Fulls build in the order its commits were admitted.

Every numeric server setting is a non-negative integer, except
`RIPCLONE_RATE_LIMIT_PER_SEC`, which may be a decimal. A value that is set but
does not parse, including an empty one, stops the server at startup with an
error that names the variable. The S3 numbers are checked when S3 storage is
configured.

The server reads `[storage]` and `[control]` from `config.toml`, and
`[providers]` from both config files. Entries in `[providers]` beat
`RIPCLONE_PROVIDERS` entries with the same id.

### GitHub App credentials

Instead of a static token, the server can mint short-lived installation tokens
for GitHub repos:

| Variable | Meaning |
|---|---|
| `RIPCLONE_GITHUB_APP_ID` | App id. Setting it turns the GitHub App broker on. |
| `RIPCLONE_GITHUB_APP_INSTALLATION_ID` | Installation id. Required with the app id. |
| `RIPCLONE_GITHUB_APP_PRIVATE_KEY` or `RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH` | The app's PEM private key, inline or as a file. |
| `RIPCLONE_GITHUB_API_BASE` | GitHub API base URL for GitHub Enterprise, `https://` only. Default `https://api.github.com`. |

## Removed settings

Settings for removed features are not read. A removed control-store setting
(the old `[metadata]` and `[queue]` sections and their variables) stops the
server at startup; any other removed variable that is set gets one warning at
startup naming it. The full list is `REMOVED_ENVIRONMENT` in `rust/src/control/mod.rs`.
