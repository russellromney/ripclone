# Backends

SQLite is Ripclone's only supported database. Artifact bytes remain independent
of the database and may live on the local filesystem or in S3-compatible
storage such as Tigris, MinIO, Cloudflare R2, or AWS S3.

The supported runtime compositions are:

- a server using SQLite metadata with the in-process local queue;
- a server and trusted standalone worker sharing SQLite metadata and queue files;
- a server owning SQLite while authenticated remote workers claim and report
  work over the HTTP API without database credentials; and
- temporary file or S3 ref stores used only as legacy rollback paths.

## Compatibility notice

MySQL, PostgreSQL, and libSQL/sqld database support has been removed. State
created in those databases is not readable by this binary. There is no automatic
migration. Operators must retain the old binary and its database data for
rollback, or start Ripclone with a new SQLite database using the setup below.

## Configuration precedence

Backend settings may come from environment variables or the global
`config.toml` (`~/.config/ripclone/config.toml`, or the file named by
`RIPCLONE_CONFIG`). Environment values win over file values. A removed effective
database value fails startup; it never falls back to SQLite, files, or S3.

```toml
[storage]
backend = "s3"
bucket = "my-bucket"
endpoint = "https://s3.example.com"

[metadata]
backend = "sqlite"
url = "/data/ripclone-metadata.db"

[queue]
backend = "sqlite"
url = "/data/ripclone-queue.db"
```

S3 credentials are read from `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`, never from the config file.

## Artifact storage

With no S3 settings, artifacts are stored under the server CAS directory
(`--cas-dir`, default `/data/cache`). For S3-compatible storage:

```bash
RIPCLONE_S3_ENDPOINT=https://s3.example.com
RIPCLONE_S3_REGION=us-east-1
RIPCLONE_S3_BUCKET=my-ripclone-bucket
RIPCLONE_S3_PREFIX=artifacts/
RIPCLONE_S3_CACHE_DIR=/data/cache
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

Artifact storage is not a metadata database. SQLite metadata works with either
local or S3-compatible artifact bytes.

## Metadata and legacy ref stores

Choose metadata with `RIPCLONE_METADATA`:

| Value | Purpose |
|---|---|
| `sqlite` | Supported database metadata; requires `RIPCLONE_METADATA_DB_URL` |
| `api` | Worker-only authenticated ref reporting; no database credentials |
| `file` | Temporary legacy rollback ref store on the local filesystem |
| `s3` | Temporary legacy rollback ref store in the configured S3 bucket |
| unset | Historical rollback behavior: S3 refs when S3 is configured, otherwise files |

For the supported database path:

```bash
RIPCLONE_METADATA=sqlite
RIPCLONE_METADATA_DB_URL=/data/ripclone-metadata.db
```

The SQLite schema is created and migrated at startup. Keep the database on
durable storage and allow only the active server (plus trusted direct workers
where explicitly configured) to open it.

## Build queue and workers

`RIPCLONE_QUEUE=local` runs the worker in the server process. For a trusted
standalone worker on the same machine, both processes share a SQLite queue:

```bash
RIPCLONE_QUEUE=sqlite
RIPCLONE_QUEUE_DB_URL=/data/ripclone-queue.db
```

The server enqueues and the standalone `ripclone-worker` claims from that file.
The worker must also use the same artifact storage and metadata configuration.

Remote workers must not mount or open SQLite. They use the authenticated API:

```bash
RIPCLONE_QUEUE=api
RIPCLONE_QUEUE_API_URL=https://ripclone.example.com
RIPCLONE_METADATA=api
RIPCLONE_METADATA_REPORT_URL=https://ripclone.example.com/v1/refs
RIPCLONE_METADATA_JOB_TOKEN=rcjt1...
```

The server continues to use `RIPCLONE_QUEUE=sqlite` and
`RIPCLONE_METADATA=sqlite`. API workers receive no SQLite path or database
credentials; the bearer token authorizes claim, heartbeat, acknowledgement, and
ref reporting through the server.

## Recommended combinations

| Use case | Artifact bytes | Metadata | Queue / worker |
|---|---|---|---|
| Local development | local | SQLite | local in-process |
| Single-box standalone worker | local or S3-compatible | SQLite | shared SQLite direct worker |
| Remote worker fleet | S3-compatible | SQLite on server | authenticated API workers |
| Temporary legacy rollback | local or S3-compatible | file or S3 refs | local in-process |

File and S3 ref stores are retained only for rollback of the current legacy
path. They are separate from artifact-byte storage and receive no new features.

## Client authentication

If the server sets `RIPCLONE_SERVER_TOKEN`, clients supply the matching token:

```bash
RIPCLONE_SERVER_TOKEN=your-secret ripclone clone owner/repo
```

`git-remote-ripclone` reads the same client variables. Provider credentials and
workspace isolation are unchanged by the database selection.
