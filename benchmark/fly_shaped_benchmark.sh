#!/usr/bin/env bash
set -euo pipefail

# Single-rate shaped clone benchmark for a remote ripclone server.
#
# Usage (run inside the Fly client machine or any Linux host with CAP_NET_ADMIN):
#   RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
#   RIPCLONE_SERVER_TOKEN=... \
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
TOKEN="${RIPCLONE_SERVER_TOKEN:-${RIPCLONE_TOKEN:-}}"
RIPCLONE="${RIPCLONE:-ripclone}"
PROVIDER="${RIPCLONE_PROVIDER:-github}"

REPO_NAME="$(basename "$REPO")"
RESOLVED_REF_FILE="/tmp/ripclone_bench_ref_${REPO//\//_}"
LOG_DIR="$TARGET/shaped_logs/${REPO_NAME}/${RATE_MBPS}Mbps"
mkdir -p "$LOG_DIR"

if [ -z "$TOKEN" ]; then
  echo "warning: RIPCLONE_SERVER_TOKEN not set; server auth may fail" >&2
fi
export RIPCLONE_SERVER_TOKEN="$TOKEN"
export RIPCLONE_NO_CACHE=1

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }

median() {
  sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}'
}

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
  auth_hash=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')
  curl -fsS -H "Authorization: Ripclone $auth_hash" "${SERVER_URL%/}/v1/repos/$PROVIDER/$owner/$name/refs/HEAD" 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("default_branch","HEAD"))'
}

head_ref_json() {
  local branch="${1:-HEAD}"
  # The server path already includes /refs/, so strip a leading "refs/" from
  # the caller's branch name (e.g. "refs/tags/v2.2.2" -> "tags/v2.2.2").
  branch="${branch#refs/}"
  local owner name auth_hash
  owner=$(echo "$REPO" | cut -d/ -f1)
  name=$(echo "$REPO" | cut -d/ -f2)
  auth_hash=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')
  curl -fsS -H "Authorization: Ripclone $auth_hash" "${SERVER_URL%/}/v1/repos/$PROVIDER/$owner/$name/refs/$branch" 2>/dev/null
}

probe_full_clone() {
  local dir="$TARGET/probe.$$"
  rm -rf "$dir"
  if "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --at "$REF" --depth 0 --dir "$dir" >/dev/null 2>&1; then
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

# Poll /refs/HEAD until the server reports a non-empty full_pack for the current
# tip.  This is more reliable than trusting the /sync response commit, which can
# reflect a coalesced in-flight build for an older tip when the branch moves.
wait_for_ref_ready() {
  local branch="${1:-HEAD}"
  local timeout="${2:-1200}"
  local start end
  start=$(now_ms)
  echo "  waiting for full clonepack artifacts to be consistent ..." >&2
  while true; do
    local out commit ready
    out=$(head_ref_json "$branch")
    commit=$(echo "$out" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("commit",""))')
    # A full editable clone is ready when the server has either a single full_pack
    # (legacy) or pack_chunk_urls/idx_bundle_url (LSM-style full history).
    ready=$(echo "$out" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("1" if (d.get("full_pack") or d.get("pack_chunk_urls") or d.get("idx_bundle_url")) else "")')
    if [ -n "$commit" ] && [ -n "$ready" ]; then
      echo "  artifacts ready for $commit" >&2
      echo "$commit"
      return 0
    fi
    end=$(now_ms)
    if [ $((end - start)) -ge $((timeout * 1000)) ]; then
      echo "error: artifacts not ready after ${timeout}s" >&2
      return 1
    fi
    echo "    not ready yet, retrying in 10s ..." >&2
    sleep 10
  done
}

warm_server() {
  local owner name auth_hash branch_or_ref
  owner=$(echo "$REPO" | cut -d/ -f1)
  name=$(echo "$REPO" | cut -d/ -f2)
  auth_hash=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')
  branch_or_ref="${BENCH_REF:-$(get_default_branch)}"

  # CLONE_REF is the branch/tag name passed to `ripclone clone --branch`.
  # AT_REF is an optional `--at <rev>` override; we only use it for explicit
  # commit SHAs because branch/tag builds are keyed by the branch/tag name.
  CLONE_REF="$branch_or_ref"
  AT_REF=""

  if [ "${SKIP_SYNC:-0}" = "1" ]; then
    REF="${BENCH_REF:-$(cat "$RESOLVED_REF_FILE" 2>/dev/null || get_default_branch)}"
    echo "  using pinned ref: $REF (skipping sync)"
    if [[ "$REF" =~ ^[0-9a-f]{40}$ ]]; then
      CLONE_REF="HEAD"
      AT_REF="$REF"
    else
      CLONE_REF="$REF"
      AT_REF=""
    fi
    return 0
  fi

  # If the caller passed a full commit SHA, pin it directly.  Otherwise treat the
  # value as a branch/tag name, sync it, and capture the exact commit the server
  # built artifacts for.
  if [[ "$branch_or_ref" =~ ^[0-9a-f]{40}$ ]]; then
    REF="$branch_or_ref"
    # Use the repo's default branch as the ref key and pass the commit via --at.
    # This lets the server serve the commit through the branch's history even when
    # the commit is no longer the branch tip.
    CLONE_REF="HEAD"
    AT_REF="$REF"
    echo "  using pinned commit $REF"
    curl -fsS -X POST \
      -H "Authorization: Ripclone $auth_hash" \
      "${SERVER_URL%/}/v1/repos/$PROVIDER/$owner/$name/sync?rev=$REF" >/dev/null 2>&1
    wait_for_artifacts
  else
    echo "  warming server mirror for $REPO @ $branch_or_ref ..."
    curl -fsS -X POST \
      -H "Authorization: Ripclone $auth_hash" \
      "${SERVER_URL%/}/v1/repos/$PROVIDER/$owner/$name/sync?branch=$branch_or_ref" >/dev/null 2>&1
    REF=$(wait_for_ref_ready "$branch_or_ref")
    CLONE_REF="$branch_or_ref"
    AT_REF=""
    echo "  resolved $branch_or_ref -> $REF"
  fi

  # Persist the resolved commit so a multi-rate sweep stays on the same tip even
  # if the upstream branch moves while later rates run.
  printf '%s\n' "$REF" > "$RESOLVED_REF_FILE"
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

rc_full()  {
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$CLONE_REF" --at "$AT_REF" --depth 0 --dir "$1"
  else
    "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$CLONE_REF" --depth 0 --dir "$1"
  fi
}
rc_depth1(){
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$CLONE_REF" --at "$AT_REF" --depth 1 --dir "$1"
  else
    "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$CLONE_REF" --depth 1 --dir "$1"
  fi
}
rc_files() {
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$CLONE_REF" --at "$AT_REF" --depth 1 --mode files --dir "$1"
  else
    "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --branch "$CLONE_REF" --depth 1 --mode files --dir "$1"
  fi
}
git_depth1(){
  if [ -n "${GIT_REF:-}" ]; then
    git clone --branch "$GIT_REF" --depth 1 "https://github.com/$REPO.git" "$1"
  elif [ -n "$AT_REF" ]; then
    # No equivalent fast path for an arbitrary non-tip commit; clone default branch.
    git clone --depth 1 "https://github.com/$REPO.git" "$1"
  else
    git clone --branch "$CLONE_REF" --depth 1 "https://github.com/$REPO.git" "$1"
  fi
}
git_full() {
  if [ -n "${GIT_REF:-}" ]; then
    git clone --branch "$GIT_REF" "https://github.com/$REPO.git" "$1"
  elif [ -n "$AT_REF" ]; then
    git clone "https://github.com/$REPO.git" "$1" && (cd "$1" && git checkout "$AT_REF")
  else
    git clone --branch "$CLONE_REF" "https://github.com/$REPO.git" "$1"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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

echo "=== repo=$REPO commit=${REF:-latest} rate=${RATE_MBPS}Mbps runs=$RUNS shaped=${SHAPED:-1} host=$(hostname) cpus=$(nproc 2>/dev/null || echo ?) ==="
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
