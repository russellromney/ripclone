#!/usr/bin/env bash
set -euo pipefail

# Single-rate shaped clone benchmark for a remote ripclone server.
#
# Usage (run inside the Fly client machine or any Linux host with CAP_NET_ADMIN):
#   RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
#   RIPCLONE_TOKEN=... \
#   ./benchmark/fly_shaped_benchmark.sh <owner/repo> <rate_mbps> [runs] [target_dir]
#
# Compared modes (each run uses a fresh dir with the client cache disabled):
#   * ripclone full (depth=0)
#   * ripclone depth=1
#   * ripclone files (mode files, depth=1)
#   * native git clone --depth 1
#   * native git clone full

REPO="${1:?owner/repo required}"
RATE_MBPS="${2:?rate in Mbps required}"
RUNS="${3:-3}"
TARGET="${4:-/data}"

SERVER_URL="${RIPCLONE_URL:-https://ripclone-server-dev.fly.dev}"
TOKEN="${RIPCLONE_TOKEN:-}"
RIPCLONE="${RIPCLONE:-ripclone}"

REPO_NAME="$(basename "$REPO")"
LOG_DIR="$TARGET/shaped_logs/${REPO_NAME}/${RATE_MBPS}Mbps"
mkdir -p "$LOG_DIR"

if [ -z "$TOKEN" ]; then
  echo "warning: RIPCLONE_TOKEN not set; server auth may fail" >&2
fi
export RIPCLONE_TOKEN="$TOKEN"
export RIPCLONE_NO_CACHE=1

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }

median() {
  sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}'
}

COMMIT="${COMMIT:-latest}"

# ---------------------------------------------------------------------------
# Server warm-up / keep-alive
# ---------------------------------------------------------------------------

wait_for_server() {
  local url="$1" timeout="${2:-120}"
  local start end
  start=$(now_ms)
  while true; do
    if curl -fsS "${url%/}/healthz" >/dev/null 2>&1; then return 0; fi
    end=$(now_ms)
    if [ $((end - start)) -ge $((timeout * 1000)) ]; then
      echo "error: server $url not healthy after ${timeout}s" >&2
      return 1
    fi
    sleep 1
  done
}

keepalive_server() {
  local url="$1"
  while true; do
    curl -fsS "${url%/}/healthz" >/dev/null 2>&1 || true
    sleep 15
  done
}

get_default_branch() {
  local owner name auth_hash
  owner=$(echo "$REPO" | cut -d/ -f1)
  name=$(echo "$REPO" | cut -d/ -f2)
  auth_hash=$(printf '%s' "$RIPCLONE_TOKEN" | shasum -a 256 | awk '{print $1}')
  curl -fsS -H "Authorization: Ripclone $auth_hash" "${SERVER_URL%/}/v1/repos/$owner/$name/refs/HEAD" 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("default_branch","HEAD"))'
}

probe_full_clone() {
  local dir="$TARGET/probe.$$"
  rm -rf "$dir"
  if "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$REF" --depth 0 --dir "$dir" >/dev/null 2>&1; then
    rm -rf "$dir"
    return 0
  else
    rm -rf "$dir"
    return 1
  fi
}

wait_for_artifacts() {
  local timeout="${1:-1200}"
  local start end
  start=$(now_ms)
  echo "  waiting for full clonepack artifacts to be consistent ..."
  while true; do
    if probe_full_clone; then
      echo "  artifacts ready (full clone succeeded)"
      return 0
    fi
    end=$(now_ms)
    if [ $((end - start)) -ge $((timeout * 1000)) ]; then
      echo "error: artifacts not ready after ${timeout}s" >&2
      return 1
    fi
    echo "    not ready yet, retrying in 10s ..."
    sleep 10
  done
}

warm_server() {
  if [ "${SKIP_SYNC:-0}" = "1" ]; then
    return 0
  fi
  echo "  warming server mirror for $REPO ..."
  "$RIPCLONE" --server "$SERVER_URL" sync "$REPO" >/dev/null 2>&1
  REF="${BENCH_REF:-$(get_default_branch)}"
  echo "  pinned ref: $REF"
  wait_for_artifacts
}

# ---------------------------------------------------------------------------
# Traffic shaping
# ---------------------------------------------------------------------------

shape_reset() {
  nft delete table inet shaped 2>/dev/null || true
}

apply_shape() {
  local rate="$1"
  # nftables limit rate uses bytes/sec; 1 Mbps = 125000 bytes/sec.
  local limit_kbps=$(( rate * 125 ))

  shape_reset

  nft add table inet shaped
  nft add chain inet shaped input '{ type filter hook input priority 0; policy accept; }'
  nft add rule inet shaped input limit rate "${limit_kbps} kbytes/second" counter accept
  nft add rule inet shaped input drop

  nft add chain inet shaped output '{ type filter hook output priority 0; policy accept; }'
  nft add rule inet shaped output limit rate "${limit_kbps} kbytes/second" counter accept
  nft add rule inet shaped output drop

  echo "  shaped with nftables inet input/output @ ${rate} Mbps (${limit_kbps} kbytes/s)"
}

# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

run_one() {
  local label="$1" cmd_log="$2"; shift 2
  local dir="$TARGET/bench-${label// /_}-${RATE_MBPS}Mbps.$$"
  rm -rf "$dir"
  local s e
  s=$(now_ms)
  if "$@" "$dir" >"$cmd_log" 2>&1; then
    e=$(now_ms)
    rm -rf "$dir"
    echo $((e - s))
  else
    rm -rf "$dir"
    echo "FAILED"
  fi
}

bench_cmd() {
  local label="$1"; shift
  local times=()
  local i
  for i in $(seq 1 "$RUNS"); do
    local log="$LOG_DIR/${label}-run${i}.log"
    local t
    t=$(run_one "$label" "$log" "$@")
    if [ "$t" = "FAILED" ]; then
      echo "  $label: FAILED (run $i) — see $log"
      return 1
    fi
    times+=("$t")
  done
  local med
  med=$(printf '%s\n' "${times[@]}" | median)
  printf '  %-26s median=%5dms   runs=[%s]\n' "$label" "$med" "$(IFS=,; echo "${times[*]}")"
}

rc_full()  { "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$REF" --depth 0 --dir "$1"; }
rc_depth1(){ "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$REF" --depth 1 --dir "$1"; }
rc_files() { "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$REF" --depth 1 --mode files --dir "$1"; }
git_depth1(){ git clone --depth 1 "https://github.com/$REPO.git" "$1"; }
git_full() { git clone "https://github.com/$REPO.git" "$1"; }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "=== repo=$REPO commit=${REF:-latest} rate=${RATE_MBPS}Mbps runs=$RUNS shaped=${SHAPED:-1} host=$(hostname) cpus=$(nproc 2>/dev/null || echo ?) ==="

wait_for_server "$SERVER_URL"
keepalive_server "$SERVER_URL" &
KEEPALIVE_PID=$!

cleanup() {
  if [ "${SHAPED:-1}" = "1" ]; then
    shape_reset
  fi
  kill "$KEEPALIVE_PID" 2>/dev/null || true
  wait "$KEEPALIVE_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Ensure REF is always set (needed when SKIP_SYNC=1 skips warm_server).
REF="${REF:-$(get_default_branch)}"

warm_server
if [ "${SHAPED:-1}" = "1" ]; then
  apply_shape "$RATE_MBPS"
else
  echo "  running unshaped"
fi

echo "--- rate=${RATE_MBPS}Mbps ---"
if [ "${SKIP_RIPCLONE:-0}" != "1" ]; then
  bench_cmd "ripclone full (depth=0)" rc_full
  bench_cmd "ripclone depth=1"        rc_depth1
  bench_cmd "ripclone files"          rc_files
fi
if [ "${SKIP_GIT:-0}" != "1" ]; then
  bench_cmd "git clone full"          git_full
  bench_cmd "git clone --depth 1"     git_depth1
fi
