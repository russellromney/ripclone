#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${RIPCLONE_BIN_DIR:-$ROOT/rust/target/release}"
SERVER_BIN="$BIN_DIR/ripclone-server"
for bin in "$SERVER_BIN" git curl; do
  if [[ "$bin" == */* ]]; then
    [ -x "$bin" ] || { echo "error: missing binary $bin" >&2; exit 1; }
  else
    command -v "$bin" >/dev/null || { echo "error: missing command $bin" >&2; exit 1; }
  fi
done

BASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-smart-http.XXXXXX")"
ORIGIN_ROOT="$BASE_DIR/origins"
WORK="$BASE_DIR/work"
PORT=$((20000 + RANDOM % 40000))
SERVER_URL="http://127.0.0.1:$PORT"
SERVER_PID=""
cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  [ -n "$SERVER_PID" ] && wait "$SERVER_PID" 2>/dev/null || true
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

mkdir -p "$ORIGIN_ROOT/acme" "$WORK"
git -C "$WORK" init -q -b main
git -C "$WORK" config user.email smart-http@example.invalid
git -C "$WORK" config user.name "Smart HTTP E2E"
printf 'served by vanilla git\n' >"$WORK/proof.txt"
git -C "$WORK" add proof.txt
git -C "$WORK" commit -q -m fixture
EXPECTED_COMMIT="$(git -C "$WORK" rev-parse HEAD)"
git init --bare -q -b main "$ORIGIN_ROOT/acme/smart-http.git"
git -C "$WORK" push -q "$ORIGIN_ROOT/acme/smart-http.git" main

export RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-smart-http-e2e-token}"
export RIPCLONE_ORIGIN_BASE="file://$ORIGIN_ROOT"
export RIPCLONE_TRUST_GATEWAY=1
if command -v sha256sum >/dev/null; then
  TOKEN_HASH="$(printf %s "$RIPCLONE_SERVER_TOKEN" | sha256sum | awk '{print $1}')"
else
  TOKEN_HASH="$(printf %s "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')"
fi

RUST_LOG=warn "$SERVER_BIN" \
  --cas-dir "$BASE_DIR/cas" \
  --repo-root "$BASE_DIR/repos" \
  --host 127.0.0.1 \
  --port "$PORT" >"$BASE_DIR/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 200); do
  if curl -fsS "$SERVER_URL/readyz" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$BASE_DIR/server.log"
    exit 1
  fi
  sleep 0.1
done
curl -fsS "$SERVER_URL/readyz" >/dev/null || { cat "$BASE_DIR/server.log"; exit 1; }

# Ordinary HTTP integrations do not need a private protocol declaration.
curl -fsS -X POST \
  -H "Authorization: Ripclone $TOKEN_HASH" \
  "$SERVER_URL/v1/repos/github/acme/smart-http/add?source=api" >/dev/null

# Vanilla Git sends Basic authentication but no x-ripclone-protocol header.
git clone --quiet \
  "http://ripclone:${TOKEN_HASH}@127.0.0.1:$PORT/v1/git/github/acme/smart-http" \
  "$BASE_DIR/clone"
test "$(git -C "$BASE_DIR/clone" rev-parse HEAD)" = "$EXPECTED_COMMIT"
test "$(cat "$BASE_DIR/clone/proof.txt")" = "served by vanilla git"
test -z "$(git -C "$BASE_DIR/clone" status --porcelain)"

echo "smart-HTTP e2e: vanilla authenticated git clone passed"
