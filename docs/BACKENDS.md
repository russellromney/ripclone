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

On Linux the server's `git` children die with it, even after a SIGKILL. macOS
has no parent-death signal, so a killed server's `git` children run until
their command ends, alongside a restarted server.

## Artifact storage

Local disk is the default and needs no configuration. With local disk, the
`cache` directory is the durable artifact store and must be kept. Builds
stage temporary files in `cache/tmp`; the server empties it at startup, so a
crashed build leaves nothing behind.

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

`GET` and `POST /v1/admin/config/{provider}/{path}` (for example
`/v1/admin/config/github/acme/app`) read and replace a repo's
build settings. There is one setting:

```json
{"compression_level": 9}
```

`compression_level` is the zstd level of the Files archive, 1 to 22, default
6; a level out of range is `400 invalid_request`. An unknown key is rejected
with `422`, except `clonepack_depths`, `archive_chunk_size` and
`head_blobs_chunk_size`, which are accepted and ignored. A repo with no stored
settings uses the default, and `GET` answers `404 config_not_found`. Full and
Files builds read the settings when they start.
