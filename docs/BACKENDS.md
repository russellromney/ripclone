# Server state and storage

A server has two kinds of state: a small control database and the artifact
bytes.

## Control database

One SQLite file holds added repositories, per-repo build settings, and the
state of every build. It is `control.db` in the server's `--data-dir`, by
default `~/.local/share/ripclone`.

Only one server process may own a control database. A second server on the
same path exits before it binds its port or starts any work. Admitted builds
survive a restart.

A server killed with SIGKILL leaves its `git` children running until their
command ends, alongside a restarted server. This holds on every OS.

## Artifact storage

Local disk is the default and needs no configuration. With local disk, the
`cache` directory is the durable artifact store and must be kept. Builds
stage temporary files in `cache/tmp`; the server empties it at startup, so a
crashed build leaves nothing behind. A leftover still in use, such as one a
killed server's git process is writing, is logged and removed at a later
start.

For S3-compatible storage (AWS S3, Cloudflare R2, Tigris, MinIO), set a
bucket and an endpoint:

```bash
export RIPCLONE_S3_BUCKET=ripclone
export RIPCLONE_S3_ENDPOINT=https://s3.example.com
export RIPCLONE_S3_REGION=us-east-1
export RIPCLONE_S3_PREFIX=production/
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

An object's key
is the prefix followed directly by its content hash, with no separator added,
so end the prefix with `/` to keep artifacts in a folder. With S3, clients
download through signed URLs, and the local cache and git mirrors are
disposable. S3 holds artifacts only, never control state.

Published artifacts never expire. See [Artifact lifecycle](ARTIFACT_LIFECYCLE.md).
