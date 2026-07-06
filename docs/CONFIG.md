# Configuration and environment variables

This page covers the user-facing flags and environment variables that control
`ripclone` behavior. For operator/server configuration, see [`BACKENDS.md`](BACKENDS.md).

## Clone flags

- `--server` / `RIPCLONE_SERVER` — ripclone server URL.
- `--provider` / `RIPCLONE_PROVIDER` — git provider instance id.
- `--token` / `RIPCLONE_UPSTREAM_TOKEN` — upstream credential sent as
  `X-Upstream-Token`.
- `--mode` / `RIPCLONE_MODE` — `editable` (default) or `files`.
- `--depth` — `1` (shallow, default) or `0` (full history).
- `--verify-upstream` / `RIPCLONE_VERIFY_UPSTREAM` — `auto` (default),
  `always`, or `never`. Cross-checks the installed tip against the upstream git
  host for editable clones. See the README for details.
- `--no-metrics` / `RIPCLONE_NO_METRICS` — suppress the post-clone metrics
  report.

## Telemetry

After a successful clone, the CLI sends a single fire-and-forget POST to the
configured server. The report is skipped when:

- the server does not return an `X-Ripclone-Clone-Id` header (self-host / older
  server),
- `--no-metrics` is passed, or
- `RIPCLONE_NO_METRICS` is set to any non-empty value.

The payload is advertising-grade telemetry only. It contains:

- `cloneId` — server-minted clone id.
- `repo` — `{ provider, owner, name }`.
- `commit` — resolved commit SHA.
- `mode` — `depth1`, `full`, or `files`.
- `cold` — whether the clone waited for a fresh build.
- `totalMs` — end-to-end clone wall time.
- `bytes` — total bytes downloaded.
- `downloadMs` — currently omitted in v1.
- `client` — `{ os, arch, ripcloneVersion }`.

Self-hosted servers accept and drop this POST at
`POST /v1/clones/{cloneId}/metrics`; the cloud route is the analytics sink.
