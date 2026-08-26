# Control database, workers, and artifact storage

ripclone has one control database per server. It stores refs, added repositories,
durable jobs, claims, attempts, and worker heartbeats. Exact-result creation and
job admission commit in one transaction.

Artifact bytes are separate: use local disk or any S3-compatible service.

## Plain SQLite (default)

The server creates `control.db` beside its default local artifact CAS and
repository directories. Set an explicit path with either:

```bash
ripclone-server --control-db /var/lib/ripclone/control.db
```

```bash
export RIPCLONE_CONTROL_DB_PATH=/var/lib/ripclone/control.db
```

or global configuration:

```toml
[control]
path = "/var/lib/ripclone/control.db"
```

Only one server process may own a control path. A second server fails before it
binds its listener or starts storage, local cache cleanup, polling, or worker
tasks. Accepted jobs survive restart, and claims abandoned by a dead worker
become eligible for recovery after `RIPCLONE_QUEUE_STALE_SECS`.

An existing database without the current control schema marker is rejected. The
server does not rewrite or automatically migrate it.

Control state written by the removed file, S3, PostgreSQL, MySQL, remote
libSQL/sqld, or in-memory implementations is unreadable by this binary. There
is no import or compatibility path. Rolling back requires the old binary and
its matching old control data; otherwise initialize a fresh SQLite control
database and re-admit repositories through current requests.

## Turso embedded replica

Turso is the only replicated control mode. The server opens the same local path
as an embedded replica and synchronizes writes to its Turso primary:

```bash
export RIPCLONE_CONTROL_DB_PATH=/var/lib/ripclone/control-replica.db
export RIPCLONE_TURSO_DATABASE_URL=libsql://example-org.turso.io
export RIPCLONE_TURSO_AUTH_TOKEN=...
ripclone-server
```

Configuration-file equivalent:

```toml
[control]
path = "/var/lib/ripclone/control-replica.db"
turso_url = "libsql://example-org.turso.io"
turso_token = "..."
```

URL and token are a required pair. Startup performs an initial sync before the
server begins work; bootstrap or primary-write failure is fatal. The local path
still has one process owner.

## Embedded and standalone workers

The server always starts embedded workers. They claim the durable jobs table
through the server's existing database handle and hold each claim through Head,
Files, and Full publication before acknowledging it.

`ripclone-worker` is the standalone option. It is authenticated API-only and
accepts no control path, database URL, database token, or Turso credential:

```bash
export RIPCLONE_QUEUE_API_URL=https://ripclone.example.com
export RIPCLONE_METADATA_REPORT_URL=https://ripclone.example.com/v1/refs
export RIPCLONE_METADATA_JOB_TOKEN=rcjt1...
ripclone-worker --cas-dir /tmp/cache --repo-root /tmp/repos
```

The token covers claim, acknowledgement, heartbeat, and ref reports. Mint one
with `ripclone mint-worker-token`. Local cache and repository paths are scratch;
artifact storage configuration may be shared with the server.

Server database credentials are rejected in the worker environment and config
before it creates scratch directories or initializes artifact storage.

## Artifact storage

Local storage is the default and needs no configuration. To use S3-compatible
storage, set:

```bash
export RIPCLONE_S3_ENDPOINT=https://s3.example.com
export RIPCLONE_S3_REGION=us-east-1
export RIPCLONE_S3_BUCKET=ripclone
export RIPCLONE_S3_PREFIX=production
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

Configuration-file fields are `[storage].backend`, `endpoint`, `region`,
`bucket`, and `prefix`. Credentials remain environment-only.
Supported services include AWS S3, Cloudflare R2, Tigris, and MinIO.

S3 stores clonepack artifacts only. It never stores refs, repository build
settings, jobs, claims, heartbeats, or any other control state.

## Repository build settings

The server stores one build-settings record per repository in the control
database. `GET` and `POST /v1/admin/config/{owner}/{repo}` read and replace that
record. A repository with no record uses the documented defaults: shallow and
full clonepacks with zstd level 6. Only an absent row selects defaults; a
database, decode, or validation error rejects admission.

The server snapshots the validated settings into each durable job. Embedded
and API-only workers use that snapshot, so changing a repository's settings
does not alter work already admitted. Branch-level overrides were removed;
requests containing the old `?branch=` query fail without changing state.

## Queue policy

These settings apply to the one durable jobs table:

- `RIPCLONE_QUEUE_STALE_SECS`
- `RIPCLONE_QUEUE_MAX_ATTEMPTS`
- `RIPCLONE_QUEUE_RETRY_BACKOFF_MS`
- `RIPCLONE_QUEUE_FAILED_RETENTION_SECS`
- `RIPCLONE_SIZE_CLASSES`

Size classes can also be declared as `[[control.size_classes]]` entries with
`name`, `max_bytes`, and optional `machine` fields.

## Removed configuration

The server and worker fail closed if old metadata, queue, or dispatcher
selectors and database connection fields are present. PostgreSQL, MySQL, direct
remote libSQL/sqld, file/S3 control stores, and the in-memory queue are not
supported. File/S3 repository-config objects and branch overrides are also
unreadable. There is no dual-read or dual-write compatibility mode.
