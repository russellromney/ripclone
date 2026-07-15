#!/usr/bin/env bash
set -euo pipefail

: "${RIPCLONE_REQUIRE_GITEA:?RIPCLONE_REQUIRE_GITEA=1 is required}"
[ "$RIPCLONE_REQUIRE_GITEA" = 1 ] || { echo "error: RIPCLONE_REQUIRE_GITEA must be 1" >&2; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${RIPCLONE_BIN_DIR:-$ROOT/rust/target/release}"
GITEA_IMAGE="gitea/gitea@sha256:2edc102cbb636ae1ddac5fa0c715aa5b03079dee13ac6800b2cef6d4e912e718"
CONTAINER="ripclone-gitea-$PPID-$$"

for command in docker curl jq; do
  command -v "$command" >/dev/null || { echo "error: required command unavailable: $command" >&2; exit 1; }
done
[ -x "$BIN_DIR/ripclone" ] || { echo "error: missing release binary $BIN_DIR/ripclone" >&2; exit 1; }

cleanup() { docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker pull "$GITEA_IMAGE" >/dev/null
docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::3000 \
  -e USER_UID=1000 -e USER_GID=1000 \
  -e GITEA__database__DB_TYPE=sqlite3 \
  -e GITEA__security__INSTALL_LOCK=true \
  -e GITEA__server__ROOT_URL=http://localhost:3000/ \
  "$GITEA_IMAGE" >/dev/null
PORT="$(docker port "$CONTAINER" 3000/tcp | sed -E 's/.*:([0-9]+)$/\1/' | head -1)"
[ -n "$PORT" ] || { echo "error: Gitea port was not assigned" >&2; exit 1; }
URL="http://127.0.0.1:$PORT"

ready=0
for _ in $(seq 1 240); do
  if curl --max-time 2 -fsS "$URL/api/healthz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.25
done
[ "$ready" = 1 ] || { docker logs "$CONTAINER" >&2; echo "error: Gitea unavailable after 60s" >&2; exit 1; }

docker exec --user git "$CONTAINER" gitea admin user create \
  --username ci --password 'local-gitea-password' --email ci@example.com --admin --must-change-password=false >/dev/null
TOKEN="$(curl --max-time 5 -fsS -u 'ci:local-gitea-password' \
  -H 'Content-Type: application/json' -d '{"name":"ripclone-e2e","scopes":["all"]}' \
  "$URL/api/v1/users/ci/tokens" | jq -er .sha1)"
[ -n "$TOKEN" ] || { echo "error: Gitea token creation failed" >&2; exit 1; }

echo "row: authenticated private Gitea repository with server-side credential isolation"
(cd "$ROOT/rust" && \
  RIPCLONE_GITEA_URL="$URL" RIPCLONE_GITEA_TOKEN="$TOKEN" RIPCLONE_GITEA_USER=ci \
  CARGO_BIN_EXE_ripclone="$BIN_DIR/ripclone" \
  cargo test --profile ci --locked --test e2e_gitea_provider \
    gitea_server_side_token_end_to_end -- --exact --ignored --nocapture)

echo "e2e_gitea_local: PASS image=$GITEA_IMAGE"
