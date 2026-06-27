#!/usr/bin/env bash
set -euo pipefail

# Benchmark the direct artifact-install clone path on oven-sh/bun.
# The server builds all artifacts during sync; the client only downloads and
# writes files.

REPO="${REPO:-oven-sh/bun}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="$ROOT_DIR/rust/target/release/ripclone"
SERVER="$ROOT_DIR/rust/target/release/ripclone-server"

for bin in "$RIPCLONE" "$SERVER"; do
  if [ ! -x "$bin" ]; then
    echo "error: missing binary $bin (run cargo build --release in rust/)"
    exit 1
  fi
done

RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-${RIPCLONE_TOKEN:-}}"
if [ -z "$RIPCLONE_SERVER_TOKEN" ]; then
  echo "error: RIPCLONE_SERVER_TOKEN must be set (server is fail-closed)"
  exit 1
fi
AUTH_HASH=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')
CURL_AUTH=(-H "Authorization: Ripclone $AUTH_HASH")

file_size() {
  if [ -f "$1" ]; then
    stat -f%z "$1" 2>/dev/null || stat -c%s "$1"
  else
    echo 0
  fi
}

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

PORT=$(( 10000 + RANDOM % 50000 ))
SERVER_URL="http://127.0.0.1:$PORT"
BASE_DIR="$(mktemp -d /tmp/ripclone-install-bench.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"

cleanup_overlay() {
  umount -l "$BASE_DIR"/bun-install 2>/dev/null || true
  rm -rf /dev/shm/ripclone-overlay-* 2>/dev/null || true
}

cleanup() {
  cleanup_overlay
  if [ -n "${SERVER_PID:-}" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

echo "==> data dir: $BASE_DIR"

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

echo ""
echo "==> Syncing mirror and building artifacts (one-time)..."
sync_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO"
sync_end=$(now_ms)
printf "sync=%d ms\n" $((sync_end - sync_start))

OWNER="$(echo "$REPO" | cut -d/ -f1)"
NAME="$(echo "$REPO" | cut -d/ -f2)"

# Resolve the ref so we can report artifact sizes.
ref_json=$(curl -fsS "${CURL_AUTH[@]}" "$SERVER_URL/v1/repos/$OWNER/$NAME/refs/HEAD")
clonepack_manifest=$(echo "$ref_json" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("clonepack_manifest",""))')

# CAS objects are stored by hash under CAS_DIR/<first2>/<hash>.
cas_path() {
  local h="$1"
  echo "$CAS_DIR/${h:0:2}/${h}"
}

echo ""
echo "==> Artifact sizes"
printf "  clonepack manifest: %s\n" "$clonepack_manifest"
if [ -n "$clonepack_manifest" ]; then
  printf "  clonepack manifest size: %s bytes\n" "$(file_size "$(cas_path "$clonepack_manifest")")"
fi

echo ""
echo "==> Direct-install clone..."
install_dir="$BASE_DIR/bun-install"
install_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$install_dir"
install_end=$(now_ms)
printf "install=%d ms\n" $((install_end - install_start))

echo ""
echo "==> Verifying..."
cd "$install_dir"
if [ -n "$(git status --short)" ]; then
  echo "error: git status not clean after install"
  git status --short
  exit 1
fi

if ! git diff --quiet HEAD; then
  echo "error: git diff reports changes"
  exit 1
fi

if ! git log --oneline -1 >/dev/null; then
  echo "error: git log failed"
  exit 1
fi

echo ""
echo "==> Benchmark complete."
