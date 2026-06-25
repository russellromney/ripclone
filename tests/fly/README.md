# tests/fly

Deploy configs and container images used for **testing / dev deploys** — not the
production cloud. Kept here (rather than the repo root) to keep root clean.

- `Dockerfile`, `Dockerfile.client`, `Dockerfile.client.overlay` — the real
  (cache-mount) build images; validated on the Fly/Depot deploy builder.
- `Dockerfile.ci`, `Dockerfile.client.ci` — CI-only images assembled from
  prebuilt binaries; built by `.github/workflows/ci.yml`.
- `fly.toml`, `fly.server-dev.toml`, `fly.client.toml`, `fly.client-dev.toml` —
  Fly app configs for the dev/test apps.
- `docker-compose.yml` — local self-hosted server stack.

## Deploying

The build context is the **repo root** (the Dockerfiles `COPY rust/` and
`scripts/`), so run `fly deploy` from the repo root and point it at the config:

```bash
fly deploy --config tests/fly/fly.server-dev.toml
```

Likewise `docker compose -f tests/fly/docker-compose.yml up` builds with the
repo root as its context.
