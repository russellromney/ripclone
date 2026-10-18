# Configuration

Settings come from flags, environment variables, and one TOML file:
`~/.config/ripclone/config.toml`, or the path in `RIPCLONE_CONFIG`. The file
holds the CLI's server URL and provider instances. The environment beats the
file. A file that does not parse stops the command.

## Server token

The server and the CLI share one token, `RIPCLONE_SERVER_TOKEN`. Both hash it
with SHA-256. The server refuses to start without it. Without it, the CLI uses
the token `ripclone login` saved in `~/.config/ripclone/tokens.json`.

## Client

| Setting | Meaning |
|---|---|
| `RIPCLONE_SERVER`, config `server` | Server URL. Default `https://ripclone.com`. |
| Config `[providers.<id>]` | Provider instances. A repo without a `provider:` prefix uses `github`. See [Providers](PROVIDERS.md). |
| `RIPCLONE_CACHE_DIR` | Turn on the local artifact cache at this path. Off by default. |
| `RIPCLONE_NO_METRICS` | Set: never send the post-clone metrics report. |
| `RUST_LOG` | Tracing filter. |

`ripclone clone` takes `--branch`, `--depth`, `--mode`, `--at`, `--wait`,
`--verify-upstream`, `--quiet` and `--progress`; `ripclone clone
--help` explains each. The progress line is drawn only when stderr is a
terminal, unless `--progress` is given; `--quiet` prints only warnings and
errors.

## Server

| Flag | Meaning |
|---|---|
| `--data-dir` | Server state: artifacts in `cache/`, git mirrors in `repos/`, the control database `control.db`. Default `~/.local/share/ripclone`. With local storage, `cache/` is the durable store. |
| `--host`, `--port` | Listen address, default `0.0.0.0` and `8000`. Port `0` picks a free port; the log names the bound address. |

| Variable | Meaning |
|---|---|
| `RIPCLONE_SERVER_TOKEN` | The token clients must present. |
| `RIPCLONE_CONFIG`, config `[providers.<id>]` | The config file path, and the provider instances and their tokens in it. See [Providers](PROVIDERS.md). |
| `RIPCLONE_S3_BUCKET` | Bucket. Setting it turns S3 storage on. See [Server state and storage](BACKENDS.md). |
| `RIPCLONE_S3_ENDPOINT` | S3-compatible endpoint. Required with a bucket. |
| `RIPCLONE_S3_REGION` | Region, default `us-east-1`. |
| `RIPCLONE_S3_PREFIX` | Key prefix. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | S3 credentials. |
| `RIPCLONE_RETENTION_MAX_GB` | With S3 storage, the local artifact cache keeps at most this many GB and nothing older than 7 days. Default 100; `0` keeps any size. It never deletes S3 objects. With local storage, nothing is trimmed. |
| `RIPCLONE_WEBHOOK_SECRET_<PROVIDER>` | Webhook secret per provider instance. See [Webhooks](WEBHOOKS.md). |
| `RIPCLONE_POLL_INTERVAL_SECS` | How often the server checks added repos for new commits, default 300; `0` turns polling off. |
| `RUST_LOG` | Tracing filter. Default `info`. |

A value that is set but does not parse, including an empty one, stops the
server at startup with an error that names the variable.

### GitHub App credentials

Instead of a static token, the server can mint short-lived installation tokens
for GitHub repos. It looks up the app's installation on each repo, so one app
serves every account that installed it; a repo the app is not installed on is
read anonymously. A lookup is reused for ten minutes, so a new installation
takes effect within that time.

| Variable | Meaning |
|---|---|
| `RIPCLONE_GITHUB_APP_ID` | App id. Setting it turns the GitHub App broker on. |
| `RIPCLONE_GITHUB_APP_PRIVATE_KEY` | The app's PEM private key. One line with literal `\n` escapes also works. |
| `RIPCLONE_GITHUB_API_BASE` | GitHub API base URL for GitHub Enterprise, `https://` only. Default `https://api.github.com`. |

## Build scheduling

A repository runs at most one build of each kind at a time. Each kind (Head,
Files, Full) has a quarter of the CPU cores as slots, at least 2, so several
repositories build at once. A commit's Head and Files build at the same time,
and its Full once Head is ready. A repository's Fulls build in the order its
commits were admitted.

Other variables the code reads exist for tests and benchmarks. They are not a
stable interface; see the test settings in
[CONTRACT_TESTS.md](internal/rebuild/CONTRACT_TESTS.md#test-settings).
