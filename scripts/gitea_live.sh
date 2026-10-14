#!/usr/bin/env bash
# Start a digest-pinned Gitea and run the contract suite's live Gitea row
# (rust/tests/contract/gitea.rs) against it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GITEA_IMAGE="gitea/gitea:1.24.6@sha256:2edc102cbb636ae1ddac5fa0c715aa5b03079dee13ac6800b2cef6d4e912e718"
CONTAINER="ripclone-gitea-$$"

: "${RIPCLONE_BIN_DIR:?RIPCLONE_BIN_DIR must name the release binary directory}"
test -x "$RIPCLONE_BIN_DIR/ripclone" || {
  echo "error: missing release CLI $RIPCLONE_BIN_DIR/ripclone" >&2
  exit 1
}
command -v docker >/dev/null || {
  echo "error: Docker is required" >&2
  exit 1
}

cleanup() {
  timeout 20 docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

timeout 15 docker image inspect "$GITEA_IMAGE" >/dev/null 2>&1 || timeout 120 docker pull "$GITEA_IMAGE"
timeout 30 docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::3000 \
  -e GITEA__security__INSTALL_LOCK=true \
  -e GITEA__database__DB_TYPE=sqlite3 \
  -e GITEA__server__ROOT_URL=http://127.0.0.1:3000/ \
  -e GITEA__server__HTTP_PORT=3000 \
  -e GITEA__service__DISABLE_REGISTRATION=true \
  "$GITEA_IMAGE" >/dev/null
HOST_PORT="$(timeout 10 docker port "$CONTAINER" 3000/tcp | awk -F: 'NR==1 {print $NF}')"
test -n "$HOST_PORT" || {
  echo "error: Docker did not publish the Gitea port" >&2
  exit 1
}
GITEA_URL="http://127.0.0.1:$HOST_PORT"

ready=0
for _ in $(seq 1 60); do
  if curl --max-time 2 -fsS "$GITEA_URL/api/v1/version" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
test "$ready" -eq 1 || {
  echo "error: digest-pinned Gitea did not become ready within 60 seconds" >&2
  exit 1
}

timeout 30 docker exec -u git "$CONTAINER" gitea admin user create \
  --admin --username ci --password ci-password-123 \
  --email ci@example.com --must-change-password=false >/dev/null
token="$(curl --max-time 10 -fsS -X POST \
  -H 'Content-Type: application/json' \
  -u ci:ci-password-123 \
  -d '{"name":"ripclone-ci","scopes":["write:repository","write:user","read:repository","read:user"]}' \
  "$GITEA_URL/api/v1/users/ci/tokens" | jq -r .sha1)"
test -n "$token" && test "$token" != "null" || {
  echo "error: failed to create Gitea access token" >&2
  exit 1
}
export RIPCLONE_GITEA_URL="$GITEA_URL"
export RIPCLONE_GITEA_USER=ci
export RIPCLONE_GITEA_TOKEN="$token"

if [ -n "${CI_ARTIFACTS:-}" ]; then
  run=("$CI_ARTIFACTS/contract" --ignored gitea_ --nocapture)
else
  run=(cargo test --profile ci --locked --test contract -- --ignored gitea_ --nocapture)
fi

log="$(mktemp "${TMPDIR:-/tmp}/ripclone-gitea.XXXXXX")"
set +e
(cd "$ROOT/rust" && timeout 300 "${run[@]}") 2>&1 | tee "$log"
rc=${PIPESTATUS[0]}
set -e
test "$rc" -eq 0 || exit "$rc"
# A filter that matched nothing would also pass.
grep -Eq "test result: ok\. 1 passed; 0 failed;" "$log" || {
  echo "error: the Gitea contract row did not report one passing test" >&2
  exit 1
}
rm -f "$log"
echo "Gitea image: $GITEA_IMAGE"
