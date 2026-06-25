#!/usr/bin/env bash
# Deploy a ripclone Fly app using the test/dev configs in tests/fly/.
#
# The Dockerfiles `COPY rust/` and `scripts/`, so the Docker build context MUST
# be the repo root. flyctl uses the current directory as the build context (and
# does not change it for the `dockerfile` setting), so this script cd's to the
# repo root and passes the config + Dockerfile by repo-root-relative path
# explicitly — avoiding any ambiguity in how flyctl resolves the toml's
# `dockerfile` field relative to a `--config` in a subdirectory.
#
# Usage: scripts/fly_deploy.sh <server-dev|client|client-dev|prod> [extra flyctl args...]
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

target="${1:-}"
if [ -z "$target" ]; then
  echo "usage: scripts/fly_deploy.sh <server-dev|client|client-dev|prod> [flyctl args...]" >&2
  exit 2
fi
shift

case "$target" in
  server-dev) config=tests/fly/fly.server-dev.toml; dockerfile=tests/fly/Dockerfile ;;
  prod)       config=tests/fly/fly.toml;            dockerfile=tests/fly/Dockerfile ;;
  client)     config=tests/fly/fly.client.toml;     dockerfile=tests/fly/Dockerfile.client ;;
  client-dev) config=tests/fly/fly.client-dev.toml; dockerfile=tests/fly/Dockerfile.client ;;
  *) echo "unknown target '$target' (expected: server-dev|client|client-dev|prod)" >&2; exit 2 ;;
esac

# Build context = repo root (cwd); Dockerfile + config given by root-relative path.
exec flyctl deploy --config "$config" --dockerfile "$dockerfile" "$@"
