# Server state and storage

A server has two kinds of state: a small control database and the artifact
bytes.

## Control database

One SQLite file holds added repositories, per-repo build settings, and the
state of every build. The default path is `~/.local/share/ripclone/control.db`;
override it with `--control-db`, `RIPCLONE_CONTROL_DB_PATH`, or
`[control] path` in `config.toml`.

Only one server process may own a control database. A second server on the
same path exits before it binds its port or starts any work. Admitted builds
survive a restart.

## Artifact storage

Local disk is the default and needs no configuration. With local disk, the
`cache` directory is the durable artifact store and must be kept.

For S3-compatible storage (AWS S3, Cloudflare R2, Tigris, MinIO):

```bash
export RIPCLONE_S3_ENDPOINT=https://s3.example.com
export RIPCLONE_S3_REGION=us-east-1
export RIPCLONE_S3_BUCKET=ripclone
export RIPCLONE_S3_PREFIX=production
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

The file equivalent is `[storage]` with `backend`, `endpoint`, `region`,
`bucket`, and `prefix`; credentials stay in the environment. With S3, clients
download through signed URLs, and the local cache and git mirrors are
disposable. S3 holds artifacts only, never control state.

Published artifacts never expire. See [Artifact lifecycle](ARTIFACT_LIFECYCLE.md).

## Per-repo build settings

`GET` and `POST /v1/admin/config/{owner}/{repo}` read and replace a repo's
build settings. A repo with no settings uses the defaults (zstd level 6).
