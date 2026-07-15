# Changelog

## Unreleased

### Breaking: SQLite-only database support

SQLite is now Ripclone's only supported database. MySQL, PostgreSQL, and
libSQL/sqld implementations, selectors, dependencies, migrations, tests, and CI
coverage have been removed.

Existing state in a removed database is not readable by the new binary. There
is no automatic migration. Retain the old binary and its database data if you
need a rollback path, or start with a new SQLite database as documented in
[`docs/BACKENDS.md`](docs/BACKENDS.md).

Local and authenticated API workers remain supported. Artifact bytes may still
use local or S3-compatible/Tigris storage, and the temporary legacy file/S3 ref
stores remain available for rollback.
