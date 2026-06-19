#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PORT=${PORT:-18765}
SERVER_PID=""
TMPDIR=$(mktemp -d)
trap 'kill $SERVER_PID 2>/dev/null || true; rm -rf "$TMPDIR"' EXIT

echo "==> Starting server..."
RUST_LOG=info ./rust/target/release/ripclone-server \
  --cas-dir "$TMPDIR/cas" \
  --repo-root "$TMPDIR/repos" \
  --port "$PORT" \
  > "$TMPDIR/server.log" 2>&1 &
SERVER_PID=$!

# Wait for server to come up.
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null; then
    break
  fi
  sleep 0.1
done

curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null || { echo "server did not start"; cat "$TMPDIR/server.log"; exit 1; }

echo "==> Syncing oven-sh/bun..."
curl -sf -X POST "http://127.0.0.1:$PORT/v1/repos/oven-sh/bun/sync" >/dev/null

echo "==> Smart-HTTP clone..."
GIT_CLONE_DIR="$TMPDIR/bun-smart"
git clone --quiet "http://127.0.0.1:$PORT/v1/git/oven-sh/bun" "$GIT_CLONE_DIR"

echo "==> Verifying clone..."
(cd "$GIT_CLONE_DIR" && git status >/dev/null && git log --oneline -1 >/dev/null)

echo ""
echo "=========================================================="
echo "Smart-HTTP fallback e2e passed."
echo "=========================================================="
