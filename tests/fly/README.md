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
`scripts/`), so deploys must run from the repo root with the Dockerfile given by
a root-relative path. Use the helper, which handles this for every app:

```bash
scripts/fly_deploy.sh server-dev      # ripclone-server-dev
scripts/fly_deploy.sh client-dev      # ripclone-client-dev
scripts/fly_deploy.sh client          # ripclone-client-test
scripts/fly_deploy.sh prod            # ripclone
# extra flyctl args pass through, e.g.:
scripts/fly_deploy.sh server-dev --now
```

Equivalent raw command (run from the repo root):

```bash
fly deploy --config tests/fly/fly.server-dev.toml --dockerfile tests/fly/Dockerfile
```

Likewise `docker compose -f tests/fly/docker-compose.yml up` builds with the
repo root as its context.
