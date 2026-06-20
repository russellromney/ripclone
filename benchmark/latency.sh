#!/usr/bin/env bash
set -euo pipefail

# Benchmark the direct-install clone path through a latency-injecting proxy.
#
# Usage:
#   LATENCY=0.05 REPO=oven-sh/bun ./benchmark/latency.sh
#
# Environment:
#   LATENCY   - one-way delay in seconds; request and response are each delayed,
#               so total added RTT is roughly 2*LATENCY (default 0.05).
#   BANDWIDTH - optional aggregate cap in Mbps (default unlimited).
#   REPO      - "owner/repo" to clone (default oven-sh/bun).
#   ITER      - number of clone runs to average (default 3).

REPO="${REPO:-oven-sh/bun}"
LATENCY="${LATENCY:-0.05}"
BANDWIDTH="${BANDWIDTH:-0}"
ITER="${ITER:-3}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="$ROOT_DIR/rust/target/release/ripclone"
SERVER="$ROOT_DIR/rust/target/release/ripclone-server"
PROXY="$ROOT_DIR/rust/target/release/ripclone-proxy"

for bin in "$RIPCLONE" "$SERVER" "$PROXY"; do
  if [ ! -x "$bin" ]; then
    echo "error: missing binary $bin (run cargo build --release in rust/)"
    exit 1
  fi
done

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

SERVER_PORT=$(( 10000 + RANDOM % 50000 ))
PROXY_PORT=$(( 10000 + RANDOM % 50000 ))
while [ "$PROXY_PORT" -eq "$SERVER_PORT" ]; do
  PROXY_PORT=$(( 10000 + RANDOM % 50000 ))
done

SERVER_URL="http://127.0.0.1:$SERVER_PORT"
PROXY_URL="http://127.0.0.1:$PROXY_PORT"
BASE_DIR="$(mktemp -d /tmp/ripclone-latency-bench.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"

SERVER_PID=""
PROXY_PID=""

cleanup_overlay() {
  if [ -d "$BASE_DIR" ]; then
    for d in "$BASE_DIR"/*/; do
      umount -l "$d" 2>/dev/null || true
    done
  fi
  rm -rf /dev/shm/ripclone-overlay-* 2>/dev/null || true
}

cleanup() {
  cleanup_overlay
  if [ -n "${PROXY_PID:-}" ]; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
  fi
  if [ -n "${SERVER_PID:-}" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

echo "==> data dir: $BASE_DIR"
echo "==> latency: ${LATENCY}s per hop (~$(( $(echo "$LATENCY * 2000" | bc | cut -d. -f1) )) ms added RTT)"
if [ "$BANDWIDTH" != "0" ]; then
  echo "==> bandwidth: ${BANDWIDTH} Mbps"
fi

# Start server (no proxy; sync talks directly to GitHub).
"$SERVER" \
  --cas-dir "$CAS_DIR" \
  --repo-root "$REPO_ROOT" \
  --host 127.0.0.1 \
  --port "$SERVER_PORT" \
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

# Start latency/bandwidth proxy between client and server.
UPSTREAM_URL="http://127.0.0.1:$SERVER_PORT"
if [ "$BANDWIDTH" != "0" ]; then
  "$PROXY" "127.0.0.1:$PROXY_PORT" "$UPSTREAM_URL" "$LATENCY" "$BANDWIDTH" --forward-auth \
    > "$BASE_DIR/proxy.log" 2>&1 &
else
  "$PROXY" "127.0.0.1:$PROXY_PORT" "$UPSTREAM_URL" "$LATENCY" --forward-auth \
    > "$BASE_DIR/proxy.log" 2>&1 &
fi
PROXY_PID=$!

for i in $(seq 1 30); do
  if curl -fsS "$PROXY_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! curl -fsS "$PROXY_URL/healthz" >/dev/null 2>&1; then
  echo "error: proxy failed to start"
  cat "$BASE_DIR/proxy.log"
  exit 1
fi

echo ""
echo "==> Syncing mirror and building artifacts (one-time, no proxy)..."
sync_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO"
sync_end=$(now_ms)
printf "sync=%d ms\n" $((sync_end - sync_start))

OWNER="$(echo "$REPO" | cut -d/ -f1)"
NAME="$(echo "$REPO" | cut -d/ -f2)"

# Report artifact sizes.
file_size() {
  if [ -f "$1" ]; then
    stat -f%z "$1" 2>/dev/null || stat -c%s "$1"
  else
    echo 0
  fi
}
cas_path() {
  local h="$1"
  echo "$CAS_DIR/${h:0:2}/${h}"
}
AUTH_HASH=$(printf '%s' "$RIPCLONE_TOKEN" | shasum -a 256 | awk '{print $1}')
ref_json=$(curl -fsS -H "Authorization: Ripclone $AUTH_HASH" "$SERVER_URL/v1/repos/$OWNER/$NAME/refs/HEAD")
clonepack_manifest=$(echo "$ref_json" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("clonepack_manifest",""))')

echo ""
echo "==> Artifact sizes"
printf "  clonepack manifest: %s\n" "$clonepack_manifest"
if [ -n "$clonepack_manifest" ]; then
  printf "  clonepack manifest size: %s bytes\n" "$(file_size "$(cas_path "$clonepack_manifest")")"
fi

echo ""
echo "==> Direct-install clone through proxy ($ITER runs)..."
total=0
for n in $(seq 1 "$ITER"); do
  install_dir="$BASE_DIR/install-$n"
  install_start=$(now_ms)
  "$RIPCLONE" --server "$PROXY_URL" clone "$REPO" --dir "$install_dir"
  install_end=$(now_ms)
  elapsed=$((install_end - install_start))
  total=$((total + elapsed))
  printf "  run %d: %d ms\n" "$n" "$elapsed"
done
avg=$((total / ITER))
printf "average install: %d ms\n" "$avg"

echo ""
echo "==> Verifying last clone..."
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
echo "verification OK"

echo ""
echo "=========================================================="
echo "Latency benchmark complete for $REPO."
echo "  latency: ${LATENCY}s per hop"
if [ "$BANDWIDTH" != "0" ]; then
  echo "  bandwidth: ${BANDWIDTH} Mbps"
fi
echo "  avg install: ${avg} ms"
echo "=========================================================="
