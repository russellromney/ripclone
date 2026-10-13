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
