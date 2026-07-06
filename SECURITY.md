# Security Policy

## Supported Versions

Security fixes target the current `main` branch until the project publishes
stable releases.

## Reporting A Vulnerability

Please report suspected vulnerabilities privately by opening a GitHub security
advisory for this repository, or by contacting the maintainer directly if
advisories are unavailable.

Include:

- affected version or commit
- reproduction steps
- expected impact
- whether the issue requires a malicious server, malicious storage backend,
  malicious upstream repository, or only an unauthenticated network attacker

Do not open a public issue for embargoed vulnerabilities.

## Security Expectations

Ripclone treats object hashes, archive frame hashes, pack hashes, and metadata
hashes as integrity boundaries. Storage backends must return bytes matching the
requested hash, and server/client code should verify content-addressed artifacts
before publishing or materializing them.

## Telemetry

After a successful clone, the CLI sends a single fire-and-forget metrics POST to
the configured server. The report is advertising-grade telemetry only: it is
never on the clone's critical path, never billing-grade, and a send failure does
not change the clone's exit status.

The POST is skipped when:

- the server does not return an `X-Ripclone-Clone-Id` header (self-host / older
  server),
- `--no-metrics` is passed, or
- `RIPCLONE_NO_METRICS` is set to any non-empty value.

The payload contains:

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
`POST /v1/clones/{cloneId}/metrics` so a self-hosted CLI never spams its own
server with 404s. The cloud route is the analytics sink.
