#!/usr/bin/env bash
set -euo pipefail

# End-to-end test of the native git remote helper.
# Verifies that `git clone ripclone://owner/repo.git` produces a working repo.

REPO="oven-sh/bun"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="$ROOT_DIR/rust/target/release/ripclone"
SERVER="$ROOT_DIR/rust/target/release/ripclone-server"
HELPER="$ROOT_DIR/rust/target/release/git-remote-ripclone"

for bin in "$RIPCLONE" "$SERVER" "$HELPER"; do
  if [ ! -x "$bin" ]; then
    echo "error: missing binary $bin (run cargo build --release in rust/)"
    exit 1
  fi
done

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

PORT=$(( 10000 + RANDOM % 50000 ))
SERVER_URL="http://127.0.0.1:$PORT"
BASE_DIR="$(mktemp -d /tmp/ripclone-remote-helper-e2e.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"

cleanup() {
  if [ -n "${SERVER_PID:-}" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

echo "==> Starting server..."
"$SERVER" \
  --cas-dir "$CAS_DIR" \
  --repo-root "$REPO_ROOT" \
  --host 127.0.0.1 \
  --port "$PORT" \
  > "$BASE_DIR/server.log" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then
  echo "error: server failed to start"
  cat "$BASE_DIR/server.log"
  exit 1
fi

export RIPCLONE_URL="$SERVER_URL"
export PATH="$ROOT_DIR/rust/target/release:$PATH"

echo "==> Syncing mirror and building artifacts (one-time)..."
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO" >/dev/null

echo "==> git clone ripclone://$REPO.git..."
clone_dir="$BASE_DIR/bun-git"
clone_start=$(now_ms)
git clone "ripclone://$REPO.git" "$clone_dir"
clone_end=$(now_ms)
printf "git clone took %d ms\n" $((clone_end - clone_start))

echo "==> Verifying git status is clean..."
cd "$clone_dir"
if [ -n "$(git status --short)" ]; then
  echo "error: git status not clean after clone"
  git status --short
  exit 1
fi

echo "==> Verifying git diff is clean..."
if ! git diff --quiet HEAD; then
  echo "error: git diff reports changes"
  exit 1
fi

echo "==> Verifying basic git operations..."
if ! git log --oneline -1 >/dev/null; then
  echo "error: git log failed"
  exit 1
fi

echo "==> Verifying origin remote..."
origin_url=$(git remote get-url origin 2>/dev/null || true)
if [ "$origin_url" != "ripclone://oven-sh/bun.git" ]; then
  echo "error: unexpected origin url: '$origin_url'"
  exit 1
fi

echo ""
echo "=========================================================="
echo "Remote helper e2e passed for $REPO."
echo "  git clone: $((clone_end - clone_start)) ms"
echo "=========================================================="
