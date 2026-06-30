#!/usr/bin/env bash
set -euo pipefail

# Quick single-cell profile: Full vs Fast clone through shaped proxy.
REPO="${REPO:-oven-sh/bun}"
BANDWIDTH="${BANDWIDTH:-250}"
RTT_MS="${RTT_MS:-50}"
CORES="${CORES:-4}"
RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-${RIPCLONE_TOKEN:-bench-token}}"
export RIPCLONE_SERVER_TOKEN

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="$ROOT_DIR/rust/target/release/ripclone"
SERVER="$ROOT_DIR/rust/target/release/ripclone-server"
PROXY="$ROOT_DIR/rust/target/release/ripclone-proxy"

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }

BASE_DIR="$(mktemp -d /tmp/ripclone-profile.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"
SERVER_PORT=$(( 10000 + RANDOM % 50000 ))
SERVER_PID=""
PROXY_PID=""

cleanup() {
  if [ -n "${PROXY_PID:-}" ]; then kill "$PROXY_PID" 2>/dev/null || true; wait "$PROXY_PID" 2>/dev/null || true; fi
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
sync_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO" > "$BASE_DIR/sync.log" 2>&1
sync_end=$(now_ms)
echo "sync=$((sync_end - sync_start)) ms"

# Artifact sizes
OWNER="$(echo "$REPO" | cut -d/ -f1)"
NAME="$(echo "$REPO" | cut -d/ -f2)"
TOKEN_HASH=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')
ref_json=$(curl -fsS -H "Authorization: Ripclone $TOKEN_HASH" "$SERVER_URL/v1/repos/$OWNER/$NAME/refs/HEAD")
clonepack_manifest=$(echo "$ref_json" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("clonepack_manifest",""))')
echo "clonepack manifest: $clonepack_manifest"
if [ -n "$clonepack_manifest" ]; then
  man_size=$(stat -f%z "$CAS_DIR/${clonepack_manifest:0:2}/${clonepack_manifest}" 2>/dev/null || echo 0)
  echo "manifest size: $man_size bytes"
fi

PROXY_PORT=$(( 10000 + RANDOM % 50000 ))
while [ "$PROXY_PORT" -eq "$SERVER_PORT" ]; do PROXY_PORT=$(( 10000 + RANDOM % 50000 )); done
latency=$(awk "BEGIN {printf \"%.6f\", $RTT_MS/2000}")
"$PROXY" "127.0.0.1:$PROXY_PORT" "$SERVER_URL" "$latency" "$BANDWIDTH" --forward-auth > "$BASE_DIR/proxy.log" 2>&1 &
PROXY_PID=$!
PROXY_URL="http://127.0.0.1:$PROXY_PORT"
for i in $(seq 1 30); do
  if curl -fsS "$PROXY_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.2
done

threads=$(( CORES - 1 )); [ "$threads" -lt 1 ] && threads=1

wait_for_full() {
  local timeout="${1:-300}"
  local start end
  start=$(now_ms)
  echo ""
  echo "==> waiting for full (depth=0) artifacts ..."
  while true; do
    if "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --depth 0 --dir "$BASE_DIR/probe-full" >/dev/null 2>&1; then
      rm -rf "$BASE_DIR/probe-full"
      end=$(now_ms)
      echo "full artifacts ready in $((end - start)) ms"
      return 0
    fi
    rm -rf "$BASE_DIR/probe-full" 2>/dev/null || true
    end=$(now_ms)
    if [ $((end - start)) -ge $((timeout * 1000)) ]; then
      echo "error: full artifacts not ready after ${timeout}s" >&2
      return 1
    fi
    sleep 2
  done
}

run_clone() {
  local label="$1" mode="$2" extra_args="$3" log="$4" outdir="$5"
  rm -rf "$outdir"
  echo ""
  echo "==> cloning $label (mode=$mode cores=$CORES rtt=${RTT_MS}ms bw=${BANDWIDTH}Mbps)..."
  local s e
  s=$(now_ms)
  # shellcheck disable=SC2086
  RUST_LOG=info RIPCLONE_FETCH_THREADS="$threads" RIPCLONE_WRITE_THREADS="$threads" \
    "$RIPCLONE" --server "$PROXY_URL" clone "$REPO" --mode "$mode" $extra_args --dir "$outdir" > "$log" 2>&1
  e=$(now_ms)
  echo "$label=$((e - s)) ms"
  echo "--- top log lines ---"
  tail -n 30 "$log"
}

run_clone "files" files "" "$BASE_DIR/clone-files.log" "$BASE_DIR/install-files"
run_clone "editable-depth1" editable "--depth 1" "$BASE_DIR/clone-editable-depth1.log" "$BASE_DIR/install-editable-depth1"

wait_for_full

run_clone "editable-full" editable "--depth 0" "$BASE_DIR/clone-editable-full.log" "$BASE_DIR/install-editable-full"

echo ""
echo "==> full logs in $BASE_DIR"
