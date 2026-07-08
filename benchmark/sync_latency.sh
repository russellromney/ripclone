#!/usr/bin/env bash
set -euo pipefail

# B4: phase-1 sync latency + storage amplification measurement.
#
# Reuses the release-server pattern from benchmark/profile_one.sh and the
# shaped-sweep environment conventions in benchmark/README.md.
#
# Environment:
#   REPO                owner/repo to measure (default: oven-sh/bun)
#   BENCH_REF           commit SHA / tag to sync (default: main)
#   GIT_REF             optional tag name when BENCH_REF is a SHA (notes only)
#   RIPCLONE_SERVER_TOKEN
#   RUNS                number of runs (default: 3)
#   COLD_RUNS           cold sync runs (default: RUNS)
#   INCREMENTAL_RUNS    incremental sync runs (default: RUNS)
#   RIPCLONE_URL        if set, benchmark this remote server (Fly mode);
#                       otherwise start a local release server.
#   RIPCLONE            path to ripclone binary (default: rust/target/release/ripclone)
#   SERVER              path to ripclone-server binary (local mode only)
#   FLY_APP             Fly app name for logs/ssh (default: ripclone-server-dev)
#   CLIENT_APP          if set, run /sync POSTs and clone readiness probes from
#                       this Fly client app (for example: ripclone-client-dev)
#   FORK_OWNER          GitHub user owning the fork for incremental runs
#                       (remote mode; default: current `gh` user)
#   FORK_BRANCH         branch to reset/push on the fork (default: main)
#   NO_CLEANUP          set to 1 to keep temp dirs
#
# Remote-mode prerequisites (for Fly verdict runs):
#   - curl, gh (GitHub CLI), fly, aws-cli
#   - AWS_* and BUCKET_NAME envs for the dev-server S3 bucket, so the script
#     can wipe ref metadata between cold runs. The mirror directory on the Fly
#     VM is wiped via `fly ssh console`.
#
# Output: a markdown block on stdout with stage tables, amplification tables,
# and the two verdict lines.

REPO="${REPO:-oven-sh/bun}"
BENCH_REF="${BENCH_REF:-main}"
GIT_REF="${GIT_REF:-${BENCH_REF}}"
RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-${RIPCLONE_TOKEN:-bench-token}}"
export RIPCLONE_SERVER_TOKEN
RUNS="${RUNS:-3}"
COLD_RUNS="${COLD_RUNS:-$RUNS}"
INCREMENTAL_RUNS="${INCREMENTAL_RUNS:-$RUNS}"
RIPCLONE_URL="${RIPCLONE_URL:-}"
FLY_APP="${FLY_APP:-ripclone-server-dev}"
CLIENT_APP="${CLIENT_APP:-}"
FORK_BRANCH="${FORK_BRANCH:-main}"
NO_CLEANUP="${NO_CLEANUP:-0}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-1800}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="${RIPCLONE:-$ROOT_DIR/rust/target/release/ripclone}"
SERVER="${SERVER:-$ROOT_DIR/rust/target/release/ripclone-server}"

OWNER="$(echo "$REPO" | cut -d/ -f1)"
NAME="$(echo "$REPO" | cut -d/ -f2)"

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }
sha256() { if command -v sha256sum >/dev/null; then sha256sum | awk '{print $1}'; else shasum -a 256 | awk '{print $1}'; fi; }
TOKEN_HASH=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | sha256)

BASE_DIR="$(mktemp -d /tmp/ripclone-sync-latency.XXXXXX)"
ORIGIN_ROOT="$BASE_DIR/origins"
WORK="$BASE_DIR/work"
SERVER_PID=""
FLY_LOGS_PID=""

cleanup() {
  [ -n "${FLY_LOGS_PID:-}" ] && { kill "$FLY_LOGS_PID" 2>/dev/null || true; wait "$FLY_LOGS_PID" 2>/dev/null || true; }
  [ -n "${SERVER_PID:-}" ] && { kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; }
  if [ "$NO_CLEANUP" != "1" ]; then
    rm -rf "$BASE_DIR"
  else
    echo "NO_CLEANUP=1; temp data left in $BASE_DIR" >&2
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

require_local_tools() {
  for bin in "$RIPCLONE" "$SERVER"; do
    [ -x "$bin" ] || { echo "error: missing binary $bin (set RIPCLONE/SERVER or run cargo build --release)" >&2; exit 1; }
  done
}

require_remote_tools() {
  for bin in curl gh fly aws; do
    command -v "$bin" >/dev/null || { echo "error: remote mode requires $bin" >&2; exit 1; }
  done
  [ -x "$RIPCLONE" ] || { echo "error: missing ripclone binary $RIPCLONE" >&2; exit 1; }
  for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL_S3 BUCKET_NAME; do
    if [ -z "${!v:-}" ]; then
      echo "warning: $v not set; cold-run server wipe will be skipped" >&2
    fi
  done
  if [ -n "$CLIENT_APP" ]; then
    fly ssh console -a "$CLIENT_APP" -C "/bin/bash -lc 'command -v ripclone >/dev/null && command -v curl >/dev/null'" >/dev/null
  fi
}

start_server_local() {
  local cas_dir="$1" repo_root="$2" port="$3"
  [ -n "$SERVER_PID" ] && { kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=""; }
  RUST_LOG=warn RIPCLONE_BENCH=1 "$SERVER" \
    --cas-dir "$cas_dir" --repo-root "$repo_root" --host 127.0.0.1 --port "$port" \
    > "$cas_dir/server.log" 2>&1 &
  SERVER_PID=$!
  local url="http://127.0.0.1:$port"
  for _ in $(seq 1 100); do
    if curl -fsS "$url/healthz" >/dev/null 2>&1; then echo "$url"; return 0; fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "error: local server died" >&2
      cat "$cas_dir/server.log" >&2
      exit 1
    fi
    sleep 0.2
  done
  echo "error: local server not ready" >&2
  cat "$cas_dir/server.log" >&2
  exit 1
}

stop_server_local() {
  [ -n "$SERVER_PID" ] && { kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=""; }
}

# The server refuses to sync, serve refs for, or clone a repo that was never
# added: it answers 404 with {"code":"repo_not_added"}. `add` is idempotent, so
# re-adding is harmless.
#
# On a fresh local server `add` also performs the initial build, which is the
# build the cold run scrapes its sync-bench line from; the `sync` that follows
# lands on the mirror it left behind.
add_repo_local() {
  local server_url="$1" repo="$2"
  "$RIPCLONE" --server "$server_url" add "$repo" >/dev/null
}

wait_for_full_build() {
  local server_url="$1" repo="$2" rev="${3:-}" branch="${4:-}" timeout="${5:-$WAIT_TIMEOUT}"
  local start end probe_dir
  start=$(now_ms)
  echo "  waiting for full build artifacts ..." >&2
  while true; do
    probe_dir="$BASE_DIR/probe-depth0.$$"
    rm -rf "$probe_dir"
    local clone_cmd=("$RIPCLONE" --server "$server_url" clone "$repo" --depth 0)
    [ -n "$branch" ] && clone_cmd+=(--branch "$branch")
    [ -n "$rev" ] && clone_cmd+=(--at "$rev")
    clone_cmd+=(--dir "$probe_dir")
    if run_clone_probe "$probe_dir" "${clone_cmd[@]}"; then
      rm -rf "$probe_dir"
      end=$(now_ms)
      echo "  full build ready in $((end - start)) ms" >&2
      return 0
    fi
    rm -rf "$probe_dir" 2>/dev/null || true
    end=$(now_ms)
    if [ $((end - start)) -ge $((timeout * 1000)) ]; then
      echo "error: full build not ready after ${timeout}s" >&2
      return 1
    fi
    sleep 5
  done
}

run_clone_probe() {
  local probe_dir="$1"
  shift
  if [ -z "$CLIENT_APP" ]; then
    "$@" >/dev/null 2>&1
    return $?
  fi

  local remote_dir="/data/ripclone-sync-probe-${repo//\//_}-$$"
  local remote_cmd arg
  remote_cmd="set -e; rm -rf $(printf '%q' "$remote_dir"); RIPCLONE_SERVER_TOKEN_HASH=$(printf '%q' "$TOKEN_HASH")"
  for arg in "$@"; do
    if [ "$arg" = "$probe_dir" ]; then
      remote_cmd+=" $(printf '%q' "$remote_dir")"
    elif [ "$arg" = "$RIPCLONE" ]; then
      remote_cmd+=" ripclone"
    else
      remote_cmd+=" $(printf '%q' "$arg")"
    fi
  done
  remote_cmd+=" >/dev/null; rm -rf $(printf '%q' "$remote_dir")"
  fly ssh console -a "$CLIENT_APP" -C "/bin/bash -lc $(printf '%q' "$remote_cmd")" >/dev/null 2>&1
}

parse_sync_bench() {
  local log="$1"
  grep '"kind":"sync-bench"' "$log" 2>/dev/null | while read -r line; do
    printf '%s\n' "$line" | python3 -c '
import sys, json, re
m = re.search(r"\{.*\"kind\":\"sync-bench\".*\}", sys.stdin.read())
print(m.group(0) if m else "{}")
' 2>/dev/null
  done
}

median() {
  sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'
}

stage_values() {
  local mode="$1" key="$2"
  [ "$mode" = "incremental" ] && mode="inc"
  local f n
  for f in "$BASE_DIR/$mode"-*.json; do
    [ -f "$f" ] || continue
    n=$(basename "$f")
    [[ "$n" =~ ^${mode}-[0-9]+\.json$ ]] || continue
    python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('phases',{}).get('$key',0))" < "$f"
  done | median
}

amplification_table() {
  local mode="$1"
  [ "$mode" = "incremental" ] && mode="inc"
  local f="$BASE_DIR/$mode-1.json"
  [ -f "$f" ] || { echo "missing $f" >&2; return 1; }
  python3 - "$f" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f).get("storage_amplification") or {}
repo = d.get("repo_size_bytes", 0)
total = d.get("total_storage_bytes", 0)
ratio = total / repo if repo else 0.0
print(f"| head packs | {d.get('head_pack_bytes',0):,} |")
print(f"| history packs | {d.get('history_pack_bytes',0):,} |")
print(f"| archive chunks | {d.get('archive_chunk_bytes',0):,} |")
print(f"| metadata | {d.get('metadata_bytes',0):,} |")
print(f"| **total** | **{total:,}** |")
print(f"| upstream repo size | {repo:,} |")
print(f"| **amplification** | **{ratio:.2f}×** |")
PY
}

merge_report() {
  local response_file="$1" bench_file="$2" out_file="$3"
  python3 <<PY
import json, sys, os
with open('$response_file') as f:
    resp = json.load(f)
report = {'phases': resp.get('phases', {})}
if '$bench_file' and os.path.exists('$bench_file') and os.path.getsize('$bench_file') > 0:
    with open('$bench_file') as f:
        bench = json.load(f)
    report['storage_amplification'] = bench.get('storage_amplification')
    # The HTTP response can come from a retry that observes/reuses a just-finished
    # build. Prefer the build's sync-bench line for timings when it is available.
    bench_phases = bench.get('phases', {})
    for k, v in bench_phases.items():
        if v is not None:
            report['phases'][k] = v
else:
    report['storage_amplification'] = None
with open('$out_file', 'w') as f:
    json.dump(report, f)
PY
}

# ---------------------------------------------------------------------------
# Local mode
# ---------------------------------------------------------------------------

setup_local_origin() {
  echo "--- setting up local origin for $REPO ---" >&2
  local bare="$ORIGIN_ROOT/$REPO.git"
  mkdir -p "$(dirname "$bare")"
  git init --bare -q -b main "$bare"
  git -C "$bare" remote add origin "https://github.com/$REPO.git"
  echo "  fetching $BENCH_REF from GitHub into local origin ..." >&2
  git -C "$bare" fetch -q origin "$BENCH_REF"
  git -C "$bare" update-ref refs/heads/main "$BENCH_REF"

  mkdir -p "$WORK"
  local w="$WORK/$NAME"
  rm -rf "$w"
  git clone -q "$bare" "$w"
  git -C "$w" config user.email "b4@ripclone.local"
  git -C "$w" config user.name "B4 Measurement"
}

run_cold_local() {
  local run url cas_dir repo_root port
  for ((run = 1; run <= COLD_RUNS; run++)); do
    echo "--- cold run $run ---" >&2
    cas_dir="$BASE_DIR/cold-$run/cache"
    repo_root="$BASE_DIR/cold-$run/repos"
    mkdir -p "$cas_dir" "$repo_root"
    port=$(( 20000 + RANDOM % 40000 ))
    url=$(start_server_local "$cas_dir" "$repo_root" "$port")
    # `add` is what performs the initial cold fetch+build under the added-repos
    # model. setup_local_origin points the origin's `main` at BENCH_REF, so this
    # first build IS the cold build of BENCH_REF and emits the cold sync-bench
    # line. The `sync --at BENCH_REF` that follows lands on the warm mirror; its
    # line is discarded below (head -n 1 keeps only the cold one, and keeps
    # cold-$run.json a single JSON object for stage_values' json.load).
    add_repo_local "$url" "$REPO"
    echo "  syncing $REPO at $BENCH_REF ..." >&2
    "$RIPCLONE" --server "$url" sync "$REPO" --at "$BENCH_REF" >/dev/null
    wait_for_full_build "$url" "$REPO" "$BENCH_REF"
    stop_server_local
    parse_sync_bench "$cas_dir/server.log" | head -n 1 > "$BASE_DIR/cold-$run.json"
  done
}

run_incremental_local() {
  local run url cas_dir repo_root port w new_commit
  w="$WORK/$NAME"
  for ((run = 1; run <= INCREMENTAL_RUNS; run++)); do
    echo "--- incremental run $run ---" >&2
    cas_dir="$BASE_DIR/inc-$run/cache"
    repo_root="$BASE_DIR/inc-$run/repos"
    mkdir -p "$cas_dir" "$repo_root"
    port=$(( 20000 + RANDOM % 40000 ))

    url=$(RIPCLONE_ORIGIN_BASE="file://$ORIGIN_ROOT" RIPCLONE_TRUST_GATEWAY=1 \
      start_server_local "$cas_dir" "$repo_root" "$port")

    add_repo_local "$url" "$REPO"
    echo "  warm-up sync ..." >&2
    "$RIPCLONE" --server "$url" sync "$REPO" >/dev/null
    wait_for_full_build "$url" "$REPO"

    printf 'B4 synthetic commit run %d\n' "$run" > "$w/b4-measure-$run.txt"
    git -C "$w" add "b4-measure-$run.txt"
    git -C "$w" commit -q -m "B4 synthetic commit run $run [skip ci]"
    git -C "$w" push -q origin "$FORK_BRANCH"
    new_commit=$(git -C "$w" rev-parse HEAD)
    echo "  new commit: $new_commit" >&2

    echo "  incremental sync ..." >&2
    "$RIPCLONE" --server "$url" sync "$REPO" >/dev/null
    wait_for_full_build "$url" "$REPO"
    stop_server_local

    parse_sync_bench "$cas_dir/server.log" | \
      python3 -c "
import sys, json
target = '$new_commit'
for line in sys.stdin:
    c = json.loads(line).get('commit', '')
    if c and target.startswith(c):
        print(line, end='')
" > "$BASE_DIR/inc-$run.json"
  done
}

# ---------------------------------------------------------------------------
# Remote (Fly) mode
# ---------------------------------------------------------------------------

sync_url() {
  local repo="$1" rev="${2:-}" branch="${3:-}"
  local url="${RIPCLONE_URL%/}/v1/repos/github/${repo}/sync"
  local sep="?"
  if [ -n "$branch" ]; then
    url="${url}${sep}branch=${branch}"
    sep="&"
  fi
  if [ -n "$rev" ]; then
    url="${url}${sep}rev=${rev}"
  fi
  printf '%s\n' "$url"
}

# Add a repo exactly once, before its state is wiped. The added-repos record
# lives under a different S3 prefix than the keys wipe_remote_repo_state clears,
# so it survives the wipe and the cold sync that follows is still cold. Calling
# add after the wipe would let add's own initial build do the cold work and turn
# the measured sync into a warm one.
#
# Waits for 200 so add's build cannot race the wipe. Servers predating the
# added-repos model have no /add route and answer a plain 404; that is not an
# error, they need no add.
REMOTE_ADDED=""
ensure_remote_added() {
  local repo="$1"
  case " $REMOTE_ADDED " in *" $repo "*) return 0 ;; esac
  local url out status attempt max_attempts=240
  url="${RIPCLONE_URL%/}/v1/repos/github/${repo}/add?source=api"
  out="$BASE_DIR/add-${repo//\//_}.response.json"
  for attempt in $(seq 1 "$max_attempts"); do
    status=$(post_sync_once "$url" "$out")
    case "$status" in
      200|201|204)
        echo "  repo $repo is added" >&2
        REMOTE_ADDED="$REMOTE_ADDED $repo"; return 0 ;;
      202|503)
        echo "  add attempt $attempt returned $status, retrying ..." >&2
        sleep 2 ;;
      404|405)
        if grep -q 'unknown provider' "$out" 2>/dev/null; then
          echo "error: add returned HTTP $status" >&2; cat "$out" >&2; return 1
        fi
        echo "  server has no /add route (pre-added-repos build); continuing" >&2
        REMOTE_ADDED="$REMOTE_ADDED $repo"; return 0 ;;
      *)
        echo "error: add returned HTTP $status" >&2
        cat "$out" >&2
        return 1 ;;
    esac
  done
  echo "error: add did not complete" >&2
  return 1
}

post_sync() {
  local repo="$1" rev="${2:-}" out="$3" branch="${4:-}"
  local url status attempt max_attempts=240
  url=$(sync_url "$repo" "$rev" "$branch")
  for attempt in $(seq 1 "$max_attempts"); do
    status=$(post_sync_once "$url" "$out")
    if [ "$status" = "200" ]; then return 0; fi
    if [ "$status" = "202" ] || [ "$status" = "503" ]; then
      echo "  sync attempt $attempt returned $status, retrying ..." >&2
      sleep 2
      continue
    fi
    echo "error: sync returned HTTP $status" >&2
    cat "$out" >&2
    return 1
  done
  echo "error: sync did not complete" >&2
  return 1
}

post_sync_once() {
  local url="$1" out="$2"
  if [ -z "$CLIENT_APP" ]; then
    curl -s -o "$out" -w '%{http_code}' -X POST \
      -H "Authorization: Ripclone $TOKEN_HASH" "$url"
    return
  fi

  local remote_cmd result status
  remote_cmd="curl -sS -w '\n%{http_code}' -X POST -H $(printf '%q' "Authorization: Ripclone $TOKEN_HASH") $(printf '%q' "$url")"
  if ! result=$(fly ssh console -a "$CLIENT_APP" -C "/bin/bash -lc $(printf '%q' "$remote_cmd")"); then
    printf '%s\n' "$result" > "$out"
    printf '000'
    return
  fi
  status=$(printf '%s\n' "$result" | tail -n 1)
  printf '%s\n' "$result" | sed '$d' > "$out"
  printf '%s' "$status"
}

start_fly_logs() {
  local out="$1"
  fly logs -a "$FLY_APP" > "$out" 2>&1 &
  FLY_LOGS_PID=$!
  sleep 2
}

stop_fly_logs() {
  [ -n "${FLY_LOGS_PID:-}" ] || return 0
  kill "$FLY_LOGS_PID" 2>/dev/null || true
  wait "$FLY_LOGS_PID" 2>/dev/null || true
  FLY_LOGS_PID=""
  sleep 1
}

find_sync_bench_for_commit() {
  local log="$1" repo="$2" commit_prefix="$3" out="$4"
  parse_sync_bench "$log" | \
    python3 -c "
import sys, json
target_repo = '$repo'
target_commit = '$commit_prefix'
found = None
for line in sys.stdin:
    try:
        d = json.loads(line)
    except Exception:
        continue
    if d.get('repo') != target_repo:
        continue
    c = d.get('commit', '')
    if c and target_commit.startswith(c):
        found = line
if found:
    print(found, end='')
" > "$out"
  [ -s "$out" ]
}

wipe_remote_repo_state() {
  local repo="$1"
  local owner name mirror lock
  owner=$(echo "$repo" | cut -d/ -f1)
  name=$(echo "$repo" | cut -d/ -f2)
  mirror="/data/repos/${owner}_${name}.git"
  lock="${mirror}.lock"

  echo "  wiping remote state for $repo ..." >&2
  fly ssh console -a "$FLY_APP" -C "rm -rf $mirror $lock" >/dev/null 2>&1 || true

  if [ -n "${BUCKET_NAME:-}" ] && [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && command -v aws >/dev/null; then
    local key
    for key in "s3://$BUCKET_NAME/refs/$owner/$name.json" "s3://$BUCKET_NAME/refs/$owner/$name/" "s3://$BUCKET_NAME/repo-config/$owner/$name.json" "s3://$BUCKET_NAME/repo-config/$owner/$name/"; do
      aws s3 rm "$key" --recursive >/dev/null 2>&1 || true
    done
  else
    echo "  (S3 metadata wipe skipped; AWS/BUCKET_NAME envs missing)" >&2
  fi
}

current_github_user() {
  if [ -n "${FORK_OWNER:-}" ]; then
    printf '%s\n' "$FORK_OWNER"
  else
    gh api user -q .login 2>/dev/null
  fi
}

setup_fork_origin() {
  echo "--- setting up GitHub fork origin for $REPO ---" >&2
  local fork_owner fork_repo w
  fork_owner=$(current_github_user)
  [ -n "$fork_owner" ] || { echo "error: could not determine GitHub user; set FORK_OWNER" >&2; exit 1; }
  fork_repo="$fork_owner/$NAME"

  if ! gh repo view "$fork_repo" >/dev/null 2>&1; then
    echo "  forking $REPO to $fork_owner ..." >&2
    gh repo fork "$REPO" >&2 || { echo "error: failed to fork $REPO" >&2; exit 1; }
    for _ in $(seq 1 60); do
      gh repo view "$fork_repo" >/dev/null 2>&1 && break
      sleep 2
    done
    gh repo view "$fork_repo" >/dev/null 2>&1 || {
      echo "error: fork $fork_repo did not become visible" >&2
      exit 1
    }
  else
    echo "  fork $fork_repo already exists" >&2
  fi

  w="$WORK/$NAME"
  rm -rf "$w"
  gh repo clone "$fork_repo" "$w" --no-upstream -- --quiet --filter=blob:none --no-checkout >&2
  git -C "$w" config user.email "b4@ripclone.local"
  git -C "$w" config user.name "B4 Measurement"
  git -C "$w" cat-file -e "$BENCH_REF^{commit}" 2>/dev/null || git -C "$w" fetch -q origin "$BENCH_REF"
  git -C "$w" update-ref "refs/heads/$FORK_BRANCH" "$BENCH_REF"
  git -C "$w" symbolic-ref HEAD "refs/heads/$FORK_BRANCH"
  git -C "$w" reset -q --mixed "$BENCH_REF"
  git -C "$w" push -q -f origin "$FORK_BRANCH"
  printf '%s\n' "$fork_repo"
}

run_cold_remote() {
  local run response bench
  ensure_remote_added "$REPO"
  for ((run = 1; run <= COLD_RUNS; run++)); do
    echo "--- cold run $run ---" >&2
    wipe_remote_repo_state "$REPO"
    response="$BASE_DIR/cold-$run.response.json"
    bench="$BASE_DIR/cold-$run.bench.json"
    start_fly_logs "$BASE_DIR/cold-$run.fly.log"
    post_sync "$REPO" "$BENCH_REF" "$response"
    sleep 3
    stop_fly_logs
    if ! find_sync_bench_for_commit "$BASE_DIR/cold-$run.fly.log" "$REPO" "$BENCH_REF" "$bench"; then
      echo "  warning: no sync-bench log line found for cold run $run" >&2
    fi
    merge_report "$response" "$bench" "$BASE_DIR/cold-$run.json"
    wait_for_full_build "$RIPCLONE_URL" "$REPO" "$BENCH_REF"
  done
}

run_incremental_remote() {
  local fork_repo run response bench w new_commit
  fork_repo=$(setup_fork_origin)
  ensure_remote_added "$fork_repo"
  wipe_remote_repo_state "$fork_repo"

  w="$WORK/$NAME"

  echo "--- incremental warm-up at $BENCH_REF on $fork_repo ---" >&2
  response="$BASE_DIR/inc-warm.response.json"
  bench="$BASE_DIR/inc-warm.bench.json"
  start_fly_logs "$BASE_DIR/inc-warm.fly.log"
  post_sync "$fork_repo" "$BENCH_REF" "$response" "$FORK_BRANCH"
  sleep 3
  stop_fly_logs
  find_sync_bench_for_commit "$BASE_DIR/inc-warm.fly.log" "$fork_repo" "$BENCH_REF" "$bench" || true
  merge_report "$response" "$bench" "$BASE_DIR/inc-warm.json"
  wait_for_full_build "$RIPCLONE_URL" "$fork_repo" "$BENCH_REF" "$FORK_BRANCH"

  for ((run = 1; run <= INCREMENTAL_RUNS; run++)); do
    echo "--- incremental run $run ---" >&2
    printf 'B4 synthetic commit run %d\n' "$run" > "$w/b4-measure-$run.txt"
    git -C "$w" add "b4-measure-$run.txt"
    git -C "$w" commit -q -m "B4 synthetic commit run $run [skip ci]"
    git -C "$w" push -q origin "$FORK_BRANCH"
    new_commit=$(git -C "$w" rev-parse HEAD)
    echo "  new commit: $new_commit" >&2

    response="$BASE_DIR/inc-$run.response.json"
    bench="$BASE_DIR/inc-$run.bench.json"
    start_fly_logs "$BASE_DIR/inc-$run.fly.log"
    post_sync "$fork_repo" "" "$response" "$FORK_BRANCH"
    sleep 3
    stop_fly_logs
    if ! find_sync_bench_for_commit "$BASE_DIR/inc-$run.fly.log" "$fork_repo" "$new_commit" "$bench"; then
      echo "  warning: no sync-bench log line found for incremental run $run" >&2
    fi
    merge_report "$response" "$bench" "$BASE_DIR/inc-$run.json"
    wait_for_full_build "$RIPCLONE_URL" "$fork_repo" "" "$FORK_BRANCH"
  done
}

# ---------------------------------------------------------------------------
# Markdown output
# ---------------------------------------------------------------------------

print_markdown() {
  local cold_p1 inc_p1
  cold_p1=""
  if [ "$COLD_RUNS" -gt 0 ]; then
    cold_p1=$(stage_values cold publish_p1_ms)
  fi
  inc_p1=$(stage_values incremental publish_p1_ms)
  local under_over
  if awk "BEGIN {exit ($inc_p1 < 5000) ? 0 : 1}"; then
    under_over="UNDER"
  else
    under_over="OVER"
  fi

  cat <<EOF
## $REPO (${GIT_REF:-$BENCH_REF})

Measured on $(date -Iseconds). Server: ${RIPCLONE_URL:-local release binary, file-backed storage}.
EOF
  if [ -n "$CLIENT_APP" ]; then
    echo "Client: Fly app ${CLIENT_APP} (sync POSTs and readiness probes run from this app)."
  fi
  if [ -n "$RIPCLONE_URL" ]; then
    cat <<'EOF'
Cold origin: live GitHub. Incremental origin: fork on GitHub with one small
synthetic commit pushed after a full warm sync (real single-commit GitHub fetch).
EOF
  else
    cat <<'EOF'
Cold origin: live GitHub. Incremental origin: local file:// bare repo
(mirror-fetch cost is understated vs a real GitHub single-commit fetch).
EOF
  fi

  if [ "$COLD_RUNS" -gt 0 ]; then
    cat <<EOF

### Cold sync stage timings (ms, median of $COLD_RUNS runs)

| Stage | ms |
|-------|-----:|
| mirror fetch | $(stage_values cold mirror_fetch_ms) |
| commit graph | $(stage_values cold commit_graph_ms) |
| HEAD packs | $(stage_values cold head_packs_ms) |
| skeleton build | $(stage_values cold skeleton_build_ms) |
| files table | $(stage_values cold files_table_ms) |
| prebuilt index | $(stage_values cold prebuilt_index_ms) |
| upload p1 | $(stage_values cold upload_p1_ms) |
| ref publish | $(stage_values cold ref_publish_ms) |
| **push→clonable** | **$cold_p1** |

$(amplification_table cold)
EOF
  else
    cat <<'EOF'

### Cold sync stage timings

Not run in this pass.
EOF
  fi

  cat <<EOF

### Incremental sync stage timings (ms, median of $INCREMENTAL_RUNS runs)

| Stage | ms |
|-------|-----:|
| mirror fetch | $(stage_values incremental mirror_fetch_ms) |
| commit graph | $(stage_values incremental commit_graph_ms) |
| HEAD packs | $(stage_values incremental head_packs_ms) |
| skeleton build | $(stage_values incremental skeleton_build_ms) |
| files table | $(stage_values incremental files_table_ms) |
| prebuilt index | $(stage_values incremental prebuilt_index_ms) |
| upload p1 | $(stage_values incremental upload_p1_ms) |
| ref publish | $(stage_values incremental ref_publish_ms) |
| **push→clonable** | **$inc_p1** |

$(amplification_table incremental)

- **TRIPWIRE: incremental push→clonable p50 = ${inc_p1}ms — $under_over the 5 s threshold**
EOF
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "Measuring $REPO @ ${BENCH_REF}${GIT_REF:+ ($GIT_REF)}, cold=$COLD_RUNS incremental=$INCREMENTAL_RUNS" >&2
if [ -n "$RIPCLONE_URL" ]; then
  echo "Remote server: $RIPCLONE_URL" >&2
  require_remote_tools
  run_cold_remote
  run_incremental_remote
else
  echo "Local server mode" >&2
  require_local_tools
  # Git transport tuning for the local server's upstream fetches.
  GITCONFIG="$BASE_DIR/gitconfig"
  cat > "$GITCONFIG" <<'EOF'
[http]
	postBuffer = 2147483648
	version = HTTP/1.1
[filter "lfs"]
	clean = cat
	smudge = cat
	process = 
	required = false
EOF
  export GIT_CONFIG_GLOBAL="$GITCONFIG"
  run_cold_local
  setup_local_origin
  run_incremental_local
fi

echo "" >&2
echo "=== STAGE MEDIANS (ms) ===" >&2
for key in mirror_fetch_ms commit_graph_ms head_packs_ms skeleton_build_ms files_table_ms prebuilt_index_ms upload_p1_ms ref_publish_ms publish_p1_ms; do
  printf "  %-20s cold=%6s inc=%6s\n" "$key" "$(stage_values cold "$key")" "$(stage_values incremental "$key")" >&2
done

echo "" >&2
echo "=== AMPLIFICATION ===" >&2
if [ "$COLD_RUNS" -gt 0 ]; then
  echo "cold:" >&2
  amplification_table cold >&2
else
  echo "cold: not run" >&2
fi
echo "incremental:" >&2
amplification_table incremental >&2

print_markdown
