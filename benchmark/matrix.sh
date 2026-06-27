#!/usr/bin/env bash
set -euo pipefail

# Matrix benchmark for the unified archive+blob-pack clone path.
#
# Sweeps cores (via fetch/write thread env vars), RTT, and a fixed bandwidth.
#
# Environment:
#   REPO      - "owner/repo" to clone (default oven-sh/bun)
#   BANDWIDTH - cap in Mbps (default 250)
#   ITER      - runs per cell (default 3)
#   CORES     - space-separated list (default "4 8")
#   RTTS      - space-separated list in ms (default "50 125 250")

REPO="${REPO:-oven-sh/bun}"
MODE="${MODE:-full}"
BANDWIDTH="${BANDWIDTH:-250}"
ITER="${ITER:-3}"
CORES="${CORES:-4 8}"
RTTS="${RTTS:-50 125 250}"
RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-${RIPCLONE_TOKEN:-bench-token}}"
export RIPCLONE_SERVER_TOKEN
# The server expects the SHA-256 hash of the token in the Authorization header.
TOKEN_HASH=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')
AUTH_HEADER="Authorization: Ripclone $TOKEN_HASH"

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
BASE_DIR="$(mktemp -d /tmp/ripclone-matrix-bench.XXXXXX)"
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

echo "==> repo: $REPO"
echo "==> bandwidth: ${BANDWIDTH} Mbps"
echo "==> cores: $CORES"
echo "==> rtts (ms): $RTTS"
echo "==> iterations per cell: $ITER"
echo "==> data dir: $BASE_DIR"

"$SERVER" \
  --cas-dir "$CAS_DIR" \
  --repo-root "$REPO_ROOT" \
  --host 127.0.0.1 \
  --port "$SERVER_PORT" \
  > "$BASE_DIR/server.log" 2>&1 &
SERVER_PID=$!

SERVER_URL="http://127.0.0.1:$SERVER_PORT"
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
echo "==> Syncing mirror and building artifacts (one-time, no proxy)..."
sync_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO"
sync_end=$(now_ms)
printf "sync=%d ms\n" $((sync_end - sync_start))

OWNER="$(echo "$REPO" | cut -d/ -f1)"
NAME="$(echo "$REPO" | cut -d/ -f2)"

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
ref_json=$(curl -fsS -H "$AUTH_HEADER" "$SERVER_URL/v1/repos/$OWNER/$NAME/refs/HEAD")
clonepack_manifest=$(echo "$ref_json" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("clonepack_manifest",""))')

echo ""
echo "==> Artifact sizes"
printf "  clonepack manifest: %s\n" "$clonepack_manifest"
if [ -n "$clonepack_manifest" ]; then
  printf "  clonepack manifest size: %s bytes\n" "$(file_size "$(cas_path "$clonepack_manifest")")"
fi

# For a clean first run, clone once outside the measured loop to warm any OS cache.
echo ""
echo "==> Warm-up clone (unmeasured, direct server)..."
warmup_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --mode "$MODE" --dir "$BASE_DIR/warmup" 2>&1 || true
warmup_end=$(now_ms)
printf "warmup=%d ms\n" $((warmup_end - warmup_start))
rm -rf "$BASE_DIR/warmup"

echo ""
echo "==> Matrix results (ms)"
printf "%-6s %-8s %-10s %-12s %-12s %-12s\n" "cores" "rtt_ms" "avg_ms" "min_ms" "max_ms" "runs"

for cores in $CORES; do
  # Simulate N cores by capping fetch/write threads to N-1 each, matching the
  # default formula used in the extractor.
  threads=$(( cores - 1 ))
  if [ "$threads" -lt 1 ]; then threads=1; fi

  for rtt_ms in $RTTS; do
    # LATENCY is one-way delay; RTT is two hops.
    latency=$(awk "BEGIN {printf \"%.6f\", $rtt_ms/2000}")

    PROXY_PORT=$(( 10000 + RANDOM % 50000 ))
    while [ "$PROXY_PORT" -eq "$SERVER_PORT" ]; do
      PROXY_PORT=$(( 10000 + RANDOM % 50000 ))
    done
    PROXY_URL="http://127.0.0.1:$PROXY_PORT"

    UPSTREAM_URL="http://127.0.0.1:$SERVER_PORT"
    "$PROXY" "127.0.0.1:$PROXY_PORT" "$UPSTREAM_URL" "$latency" "$BANDWIDTH" --forward-auth \
      > "$BASE_DIR/proxy-${cores}-${rtt_ms}.log" 2>&1 &
    PROXY_PID=$!

    for i in $(seq 1 30); do
      if curl -fsS "$PROXY_URL/healthz" >/dev/null 2>&1; then break; fi
      sleep 0.2
    done
    if ! curl -fsS "$PROXY_URL/healthz" >/dev/null 2>&1; then
      echo "error: proxy failed to start for cores=$cores rtt=$rtt_ms"
      cat "$BASE_DIR/proxy-${cores}-${rtt_ms}.log"
      exit 1
    fi

    min_ms=999999
    max_ms=0
    total_ms=0
    for n in $(seq 1 "$ITER"); do
      install_dir="$BASE_DIR/install-${cores}-${rtt_ms}-${n}"
      install_start=$(now_ms)
      RIPCLONE_FETCH_THREADS="$threads" RIPCLONE_WRITE_THREADS="$threads" \
        "$RIPCLONE" --server "$PROXY_URL" clone "$REPO" --mode "$MODE" --dir "$install_dir" 2>&1
      install_end=$(now_ms)
      elapsed=$((install_end - install_start))
      total_ms=$((total_ms + elapsed))
      if [ "$elapsed" -lt "$min_ms" ]; then min_ms=$elapsed; fi
      if [ "$elapsed" -gt "$max_ms" ]; then max_ms=$elapsed; fi
    done

    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
    PROXY_PID=""

    avg_ms=$((total_ms / ITER))
    printf "%-6s %-8s %-10s %-12s %-12s %-12s\n" \
      "$cores" "$rtt_ms" "$avg_ms" "$min_ms" "$max_ms" "$ITER"
  done
done

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
echo "Matrix benchmark complete for $REPO @ ${BANDWIDTH} Mbps"
echo "  iterations per cell: $ITER"
echo "=========================================================="
