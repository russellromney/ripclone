#!/usr/bin/env bash
set -euo pipefail

REPO="${REPO:-oven-sh/bun}"
MODE="${MODE:-full}"
RIPCLONE_TOKEN="${RIPCLONE_TOKEN:-bench-token}"
export RIPCLONE_TOKEN

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="$ROOT_DIR/rust/target/release/ripclone"
SERVER="$ROOT_DIR/rust/target/release/ripclone-server"

BASE_DIR="$(mktemp -d /tmp/ripclone-verify.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"
SERVER_PORT=$(( 10000 + RANDOM % 50000 ))
SERVER_PID=""

cleanup() {
  if [ -n "${SERVER_PID:-}" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

"$SERVER" --cas-dir "$CAS_DIR" --repo-root "$REPO_ROOT" --host 127.0.0.1 --port "$SERVER_PORT" > "$BASE_DIR/server.log" 2>&1 &
SERVER_PID=$!
SERVER_URL="http://127.0.0.1:$SERVER_PORT"
for i in $(seq 1 30); do
  if curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done

echo "==> syncing $REPO..."
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO" > "$BASE_DIR/sync.log" 2>&1

echo "==> cloning $MODE..."
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --mode "$MODE" --dir "$BASE_DIR/$MODE" > "$BASE_DIR/clone.log" 2>&1

cd "$BASE_DIR/$MODE"
echo "==> git status"
git status --short
echo "==> git fsck"
git fsck --full 2>&1 | tail -5
echo "==> git log"
git log --oneline -1
echo "==> verifying a few tracked files exist"
ls -l README.md package.json 2>/dev/null || true

echo ""
echo "BASE_DIR=$BASE_DIR"
