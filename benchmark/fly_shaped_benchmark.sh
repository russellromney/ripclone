#!/usr/bin/env bash
set -euo pipefail

# Single-rate shaped clone benchmark for a remote ripclone server.
#
# Usage (run inside the Fly client machine or any Linux host with CAP_NET_ADMIN):
#   RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
#   RIPCLONE_SERVER_TOKEN=... \
#   ./benchmark/fly_shaped_benchmark.sh <owner/repo> <rate_mbps> [runs] [target_dir]
#
# Set RIPCLONE_BENCH_PROVIDER for non-GitHub provider routes.
# Set BENCH_MODES to a space-separated subset of full, depth1, files,
# git-full, git-depth1, and github-files. The default remains the complete
# editable-clone matrix. Set
# RIPCLONE_BENCH_READY_CLONEPACK=shallow for a depth-one-only run so readiness
# does not wait for background full-history artifacts.
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
BENCH_MODES="${BENCH_MODES:-full depth1 files git-full git-depth1}"
READY_CLONEPACK="${RIPCLONE_BENCH_READY_CLONEPACK:-full}"

case "$READY_CLONEPACK" in
  full|shallow) ;;
  *) echo "error: RIPCLONE_BENCH_READY_CLONEPACK must be full or shallow" >&2; exit 2 ;;
esac

mode_enabled() {
  case " $BENCH_MODES " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

for mode in $BENCH_MODES; do
  case "$mode" in
    full|depth1|files|git-full|git-depth1|github-files) ;;
    *) echo "error: unknown BENCH_MODES entry: $mode" >&2; exit 2 ;;
  esac
done

SERVER_URL="${RIPCLONE_URL:-https://ripclone-server-dev.fly.dev}"
TOKEN="${RIPCLONE_SERVER_TOKEN:-${RIPCLONE_TOKEN:-}}"
RIPCLONE="${RIPCLONE:-ripclone}"
PROVIDER="${RIPCLONE_BENCH_PROVIDER:-github}"

REPO_NAME="$(basename "$REPO")"
REF_STATE_DIR="$TARGET/shaped_logs/${REPO_NAME}"
LOG_DIR="$REF_STATE_DIR/${RATE_MBPS}Mbps"
mkdir -p "$LOG_DIR"
# Keep one pin across a multi-rate sweep, but segregate root/non-root runs so
# sticky-directory ownership rules cannot make a rerun fail to update it.
RESOLVED_REF_FILE="$REF_STATE_DIR/resolved-ref-$(id -u)"

if [ -z "$TOKEN" ]; then
  echo "warning: RIPCLONE_SERVER_TOKEN not set; server auth may fail" >&2
fi
export RIPCLONE_SERVER_TOKEN="$TOKEN"
export RIPCLONE_NO_CACHE=1

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }

median() {
  sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}'
}

p90() {
  sort -n | awk '{a[NR]=$1} END{if (NR) {i=int((9*NR+9)/10); print a[i]}}'
}

worktree_digest() {
  python3 - "$1" <<'PY'
import hashlib, os, stat, sys
root = os.fsencode(sys.argv[1])
digest = hashlib.sha256()
for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
    dirs[:] = sorted(d for d in dirs if d != b'.git')
    rel_dir = os.path.relpath(current, root)
    symlink_dirs = []
    for name in list(dirs):
        path = os.path.join(current, name)
        if os.path.islink(path):
            dirs.remove(name)
            symlink_dirs.append(name)
    for name in sorted(files + symlink_dirs):
        path = os.path.join(current, name)
        rel = name if rel_dir == b'.' else os.path.join(rel_dir, name)
        mode = os.lstat(path).st_mode
        if stat.S_ISLNK(mode):
            kind, payload = b'l', os.readlink(path)
        elif stat.S_ISREG(mode):
            kind = b'f'
            with open(path, 'rb') as handle:
                payload = handle.read()
        else:
            raise SystemExit(f"unsupported worktree entry: {os.fsdecode(rel)}")
        executable = b'x' if mode & 0o111 else b'-'
        digest.update(kind + b'\0' + executable + b'\0' + rel + b'\0')
        digest.update(hashlib.sha256(payload).digest())
print(digest.hexdigest())
PY
}

sha256_hex() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum | awk '{print $1}'
  else shasum -a 256 | awk '{print $1}'; fi
}

auth_header() {
  printf 'Authorization: Ripclone %s' \
    "$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | sha256_hex)"
}

repo_owner() { echo "$REPO" | cut -d/ -f1; }
repo_name()  { echo "$REPO" | cut -d/ -f2; }

# ---------------------------------------------------------------------------
# Server warm-up / keep-alive
# ---------------------------------------------------------------------------

wait_for_server() {
  local url="$1" timeout="${2:-120}"
  local start end
  start=$(now_ms)
  while true; do
    if curl --connect-timeout 2 --max-time 5 -fsS "${url%/}/healthz" >/dev/null 2>&1; then return 0; fi
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
    curl --connect-timeout 2 --max-time 5 -fsS "${url%/}/healthz" >/dev/null 2>&1 || true
    sleep 15
  done
}

# A repo must be `add`ed before the server will serve `/refs`, `/sync` or a
# clone for it; otherwise every request answers 404 with {"code":"repo_not_added"}.
# `add` is idempotent (it overwrites the added-repos record), so re-running the
# benchmark against an already-added repo is fine. Servers predating the
# added-repos model have no `/add` route and answer a plain 404 "not found" —
# treat that as "nothing to add" so the harness keeps working against them.
#
# Memoized: `add` triggers an initial build, so it must not be re-POSTed from
# inside a poll loop. Keep the exact admission returned by `add`; after a 202,
# warm_server must poll that commit rather than immediately probing a moving
# branch row or POSTing a second moving sync. All progress goes to stderr
# because the callers downstream of this run inside command substitutions that
# capture stdout.
REPO_ADDED=0
ADMITTED_COMMIT=""
ADMITTED_BRANCH=""

record_admission() {
  local body="$1" fields
  fields="$(printf '%s' "$body" | python3 -c '
import json, sys
try:
    value = json.load(sys.stdin)
except (json.JSONDecodeError, ValueError):
    raise SystemExit(0)
commit = value.get("commit") or ""
branch = value.get("branch") or value.get("default_branch") or ""
if commit:
    print(f"{commit}\t{branch}")
' 2>/dev/null || true)"
  if [ -n "$fields" ]; then
    ADMITTED_COMMIT="${fields%%$'\t'*}"
    ADMITTED_BRANCH="${fields#*$'\t'}"
  fi
}

ensure_repo_added() {
  if [ "$REPO_ADDED" = "1" ]; then return 0; fi
  if [ "${SKIP_ADD:-0}" = "1" ]; then
    REPO_ADDED=1
    return 0
  fi
  local url status body tmp attempt
  url="${SERVER_URL%/}/v1/repos/$PROVIDER/$(repo_owner)/$(repo_name)/add?source=api"
  tmp="$(mktemp)"
  for attempt in $(seq 1 5); do
    status="000"
    status=$(curl --connect-timeout 5 --max-time 30 -s -o "$tmp" -w '%{http_code}' -X POST -H "$(auth_header)" "$url") || status="000"
    case "$status" in
      200|201|204)
        body="$(cat "$tmp")"
        record_admission "$body"
        echo "  repo $REPO is added" >&2
        REPO_ADDED=1; rm -f "$tmp"; return 0 ;;
      202)
        body="$(cat "$tmp")"
        record_admission "$body"
        if [ -z "$ADMITTED_COMMIT" ]; then
          echo "  add attempt $attempt: HTTP 202 omitted the admitted commit, retrying ..." >&2
          sleep 2
          continue
        fi
        echo "  repo $REPO is added; initial build in progress (HTTP $status)" >&2
        REPO_ADDED=1; rm -f "$tmp"; return 0 ;;
      503)
        echo "  add attempt $attempt: HTTP 503, retrying ..." >&2
        sleep 2 ;;
      404|405)
        body="$(cat "$tmp")"
        if printf '%s' "$body" | grep -q 'unknown provider'; then
          echo "error: unknown provider '$PROVIDER' for $REPO" >&2
          rm -f "$tmp"; return 1
        fi
        echo "  server has no /add route (pre-added-repos build); continuing" >&2
        REPO_ADDED=1; rm -f "$tmp"; return 0 ;;
      000)
        echo "  add attempt $attempt: no response from $SERVER_URL, retrying ..." >&2
        sleep 2 ;;
      *)
        echo "error: add returned HTTP $status" >&2
        cat "$tmp" >&2
        rm -f "$tmp"; return 1 ;;
    esac
  done
  echo "error: add did not complete after 5 attempts" >&2
  rm -f "$tmp"
  return 1
}

get_default_branch() {
  curl --connect-timeout 5 --max-time 30 -fsS -H "$(auth_header)" "${SERVER_URL%/}/v1/repos/$PROVIDER/$(repo_owner)/$(repo_name)/refs/HEAD" 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("default_branch","HEAD"))'
}

head_ref_json() {
  local branch="${1:-HEAD}"
  # The server path already includes /refs/, so strip a leading "refs/" from
  # the caller's branch name (e.g. "refs/tags/v2.2.2" -> "tags/v2.2.2").
  branch="${branch#refs/}"
  curl --connect-timeout 5 --max-time 30 -fsS -H "$(auth_header)" "${SERVER_URL%/}/v1/repos/$PROVIDER/$(repo_owner)/$(repo_name)/refs/$branch" 2>/dev/null
}

probe_ready_clone() {
  local dir="$TARGET/probe.$$"
  rm -rf "$dir"
  local depth=0
  if [ "$READY_CLONEPACK" = "shallow" ]; then depth=1; fi
  if "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --at "$REF" --depth "$depth" --dir "$dir" >/dev/null 2>&1; then
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
  echo "  waiting for $READY_CLONEPACK clonepack artifacts to be consistent ..."
  while true; do
    if probe_ready_clone; then
      echo "  artifacts ready ($READY_CLONEPACK clone succeeded)"
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
# tip. This is only used for legacy/pre-added-repos servers. Current servers
# pass the exact commit admitted by /add and use the pinned branch below, which
# cannot drift while the upstream branch moves.
wait_for_ref_ready() {
  local branch="${1:-HEAD}"
  local timeout="${2:-1200}"
  local pinned="${3:-}"
  local start end
  start=$(now_ms)
  echo "  waiting for $READY_CLONEPACK clonepack artifacts to be consistent ..." >&2
  while true; do
    local out commit ready status tmp
    if [ -n "$pinned" ]; then
      tmp="$(mktemp)"
      status="000"
      status=$(curl --connect-timeout 5 --max-time 30 -sS -o "$tmp" -w '%{http_code}' \
        -H "$(auth_header)" \
        -H 'x-ripclone-protocol: 2' \
        "${SERVER_URL%/}/v1/repos/$PROVIDER/$(repo_owner)/$(repo_name)/refs/${branch#refs/}?clonepack=$READY_CLONEPACK&pinned=$pinned") || status="000"
      out="$(cat "$tmp")"
      rm -f "$tmp"
      if [ "$status" = "200" ]; then
        commit="$(printf '%s' "$out" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("commit",""))' 2>/dev/null || true)"
        ready="$(printf '%s' "$out" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("1" if d.get("clonepack_manifest") else "")' 2>/dev/null || true)"
        if [ "$commit" = "$pinned" ] && [ -n "$ready" ]; then
          echo "  artifacts ready for admitted $commit" >&2
          echo "$commit"
          return 0
        fi
      elif [ "$status" != "202" ] && [ "$status" != "404" ]; then
        echo "error: pinned ref lookup returned HTTP $status" >&2
        printf '%s\n' "$out" >&2
        return 1
      fi
    else
      if [ "$READY_CLONEPACK" = "shallow" ]; then
        out="$(curl --connect-timeout 5 --max-time 30 -fsS \
          -H "$(auth_header)" \
          -H 'x-ripclone-protocol: 2' \
          "${SERVER_URL%/}/v1/repos/$PROVIDER/$(repo_owner)/$(repo_name)/refs/${branch#refs/}?clonepack=shallow" \
          2>/dev/null || true)"
      else
        out="$(head_ref_json "$branch" || true)"
      fi
      commit="$(printf '%s' "$out" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("commit",""))' 2>/dev/null || true)"
      # A full editable clone is ready when the server advertises full-history
      # artifacts for the tip. Field names have drifted across server versions,
      # so accept any of them: full_pack (legacy single pack), pack_chunk_urls /
      # idx_bundle_url (older LSM full history), or clonepack_manifest with
      # archive_ready (current). Empty strings count as absent.
      ready="$(printf '%s' "$out" | READY_CLONEPACK="$READY_CLONEPACK" python3 -c 'import os,sys,json; d=json.load(sys.stdin); mode=os.environ["READY_CLONEPACK"]; print("1" if (d.get("clonepack_manifest") and (mode == "shallow" or d.get("archive_ready"))) or (mode == "full" and (d.get("full_pack") or d.get("pack_chunk_urls") or d.get("idx_bundle_url"))) else "")' 2>/dev/null || true)"
      if [ -n "$commit" ] && [ -n "$ready" ]; then
        echo "  artifacts ready for $commit" >&2
        echo "$commit"
        return 0
      fi
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
  local owner name branch_or_ref
  ensure_repo_added
  owner=$(repo_owner)
  name=$(repo_name)
  if [ -n "${BENCH_REF:-}" ]; then
    branch_or_ref="$BENCH_REF"
  elif [ -n "$ADMITTED_BRANCH" ]; then
    branch_or_ref="$ADMITTED_BRANCH"
  elif [ -n "$ADMITTED_COMMIT" ]; then
    # A valid admission always carries the exact commit. HEAD is a stable
    # selector for the pinned lookup, so do not probe moving HEAD just to learn
    # the branch name.
    branch_or_ref="HEAD"
  else
    branch_or_ref="$(get_default_branch)"
  fi

  # CLONE_REF is the branch/tag name passed to `ripclone clone --branch`.
  # AT_REF is an optional `--at <rev>` override; we only use it for explicit
  # commit SHAs because branch/tag builds are keyed by the branch/tag name.
  CLONE_REF="$branch_or_ref"
  AT_REF=""

  if [ "${SKIP_SYNC:-0}" = "1" ]; then
    REF="${BENCH_REF:-${ADMITTED_COMMIT:-$(cat "$RESOLVED_REF_FILE" 2>/dev/null || get_default_branch)}}"
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

  # A current server returns the exact commit from /add. That admission already
  # queued the initial build, so wait on its pinned metadata without a second
  # moving sync POST. Keep the commit as --at for benchmark clones so a branch
  # advance after readiness cannot change what is measured.
  if [ -n "$ADMITTED_COMMIT" ] && [ -z "${BENCH_REF:-}" ]; then
    REF="$(wait_for_ref_ready "$branch_or_ref" 1200 "$ADMITTED_COMMIT")"
    CLONE_REF="$branch_or_ref"
    AT_REF="$REF"
    echo "  resolved admitted $branch_or_ref -> $REF"
    printf '%s\n' "$REF" > "$RESOLVED_REF_FILE"
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
    curl --connect-timeout 5 --max-time 30 -fsS -X POST \
      -H "$(auth_header)" \
      "${SERVER_URL%/}/v1/repos/$PROVIDER/$owner/$name/sync?rev=$REF" >/dev/null 2>&1
    wait_for_artifacts
  else
    echo "  warming server mirror for $REPO @ $branch_or_ref ..."
    curl --connect-timeout 5 --max-time 30 -fsS -X POST \
      -H "$(auth_header)" \
      "${SERVER_URL%/}/v1/repos/$PROVIDER/$owner/$name/sync?branch=$branch_or_ref" >/dev/null 2>&1
    REF=$(wait_for_ref_ready "$branch_or_ref")
    CLONE_REF="$branch_or_ref"
    # Every timed run must consume the exact artifact selected before the
    # sample. Upstream branches can advance even during ten repetitions.
    AT_REF="$REF"
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
  local label="$1" cmd_log="$2" deep_verify="$3"; shift 3
  local dir="$TARGET/bench-${label// /_}-${RATE_MBPS}Mbps.$$"
  rm -rf "$dir"
  local s e elapsed
  s=$(now_ms)
  if "$@" "$dir" >"$cmd_log" 2>&1; then
    e=$(now_ms)
    elapsed=$((e - s))
    # Correctness is load-bearing, but it is not part of clone latency. In
    # particular, `git status` can scan or refresh thousands of paths and used
    # to inflate both ripclone and native-Git cells inside the timer.
    if ! validate_result "$label" "$dir" "$deep_verify" >>"$cmd_log" 2>&1; then
      rm -rf "$dir"
      echo "FAILED"
      return
    fi
    rm -rf "$dir"
    echo "$elapsed"
  else
    rm -rf "$dir"
    echo "FAILED"
  fi
}

bench_cmd() {
  local label="$1"; shift
  local times=()
  local i sample_count="$RUNS"
  case "$label" in
    "ripclone "*) sample_count="${RIPCLONE_RUNS:-$RUNS}" ;;
    *) sample_count="${NATIVE_RUNS:-$RUNS}" ;;
  esac
  for i in $(seq 1 "$sample_count"); do
    local log="$LOG_DIR/${label}-run${i}.log"
    local t
    # Exact revision/shape checks run after every sample. The first sample also
    # performs the expensive whole-tree digest and object connectivity proof;
    # repeating those full reads does not strengthen the latency distribution.
    local deep_verify=0
    if [ "$i" -eq 1 ] || [ "${VERIFY_EVERY_RUN:-0}" = "1" ]; then
      deep_verify=1
    fi
    t=$(run_one "$label" "$log" "$deep_verify" "$@")
    if [ "$t" = "FAILED" ]; then
      echo "  $label: FAILED (run $i) — see $log"
      return 1
    fi
    times+=("$t")
  done
  local med tail90
  med=$(printf '%s\n' "${times[@]}" | median)
  tail90=$(printf '%s\n' "${times[@]}" | p90)
  printf '  %-26s p50=%5dms p90=%5dms   runs=[%s]\n' "$label" "$med" "$tail90" "$(IFS=,; echo "${times[*]}")"
}

rc_full()  {
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" --server "$SERVER_URL" --provider "$PROVIDER" clone "$REPO" --branch "$CLONE_REF" --at "$AT_REF" --depth 0 --dir "$1"
  else
    "$RIPCLONE" --server "$SERVER_URL" --provider "$PROVIDER" clone "$REPO" --branch "$CLONE_REF" --depth 0 --dir "$1"
  fi
}
rc_depth1(){
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" --server "$SERVER_URL" --provider "$PROVIDER" clone "$REPO" --branch "$CLONE_REF" --at "$AT_REF" --depth 1 --dir "$1"
  else
    "$RIPCLONE" --server "$SERVER_URL" --provider "$PROVIDER" clone "$REPO" --branch "$CLONE_REF" --depth 1 --dir "$1"
  fi
}
rc_files() {
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" --server "$SERVER_URL" --provider "$PROVIDER" clone "$REPO" --branch "$CLONE_REF" --at "$AT_REF" --depth 1 --mode files --dir "$1"
  else
    "$RIPCLONE" --server "$SERVER_URL" --provider "$PROVIDER" clone "$REPO" --branch "$CLONE_REF" --depth 1 --mode files --dir "$1"
  fi
}

verify_git_result() {
  local dir="$1" actual
  actual="$(git -C "$dir" rev-parse HEAD)"
  if [ -n "${REF:-}" ] && [ "$actual" != "$REF" ]; then
    echo "error: expected $REF, got $actual" >&2
    return 1
  fi
  test -z "$(git -C "$dir" status --porcelain)"
}

validate_result() {
  local label="$1" dir="$2" deep_verify="$3" actual_digest expected_digest
  case "$label" in
    "ripclone files"|"github archive files")
      actual_digest="$(worktree_digest "$dir")"
      expected_digest="$EXPECTED_WORKTREE_DIGEST"
      if [ "$label" = "github archive files" ]; then
        expected_digest="$EXPECTED_ARCHIVE_DIGEST"
      fi
      if [ "$actual_digest" != "$expected_digest" ]; then
        echo "error: $label worktree digest $actual_digest != $expected_digest" >&2
        return 1
      fi
      test ! -e "$dir/.git"
      ;;
    *)
      verify_git_result "$dir"
      if [ "$deep_verify" = "1" ]; then
        actual_digest="$(worktree_digest "$dir")"
        if [ "$actual_digest" != "$EXPECTED_WORKTREE_DIGEST" ]; then
          echo "error: $label worktree digest $actual_digest != $EXPECTED_WORKTREE_DIGEST" >&2
          return 1
        fi
        git -C "$dir" fsck --connectivity-only HEAD >/dev/null
      fi
      case "$label" in
        "ripclone depth=1"|"git clone --depth 1")
          test -s "$dir/.git/shallow"
          ;;
        "ripclone full (depth=0)"|"git clone full")
          test ! -e "$dir/.git/shallow"
          ;;
      esac
      ;;
  esac
}

github_files() {
  mkdir -p "$1"
  curl --connect-timeout 5 --max-time 600 --fail --silent --show-error --location \
    "https://api.github.com/repos/$REPO/tarball/$REF" \
    | tar -xz --strip-components=1 -C "$1"
}

prepare_expected_tree() {
  EXPECTED_DIR="$(mktemp -d "$TARGET/ripclone-benchmark-expected.XXXXXX")"
  if [ "$CLONE_REF" != "HEAD" ]; then
    git clone --quiet --depth 1 --branch "$CLONE_REF" "https://github.com/$REPO.git" "$EXPECTED_DIR"
  else
    git clone --quiet --depth 1 "https://github.com/$REPO.git" "$EXPECTED_DIR"
  fi
  if [ "$(git -C "$EXPECTED_DIR" rev-parse HEAD)" != "$REF" ]; then
    git -C "$EXPECTED_DIR" fetch --quiet --depth 1 origin "$REF"
    git -C "$EXPECTED_DIR" checkout --quiet --detach "$REF"
  fi
  test "$(git -C "$EXPECTED_DIR" rev-parse HEAD)" = "$REF"
  EXPECTED_WORKTREE_DIGEST="$(worktree_digest "$EXPECTED_DIR")"
  echo "  correctness fixture: commit=$REF digest=$EXPECTED_WORKTREE_DIGEST"

  if mode_enabled github-files; then
    EXPECTED_ARCHIVE_DIR="$(mktemp -d "$TARGET/ripclone-benchmark-archive.XXXXXX")"
    github_files "$EXPECTED_ARCHIVE_DIR"
    EXPECTED_ARCHIVE_DIGEST="$(worktree_digest "$EXPECTED_ARCHIVE_DIR")"
    echo "  archive fixture: commit=$REF digest=$EXPECTED_ARCHIVE_DIGEST"
  fi
}

git_depth1(){
  if [ -n "${GIT_REF:-}" ]; then
    git clone --branch "$GIT_REF" --depth 1 "https://github.com/$REPO.git" "$1"
  elif [ -n "${REF:-}" ]; then
    # Fetch the same immutable commit that admission selected. A branch clone
    # can move during a long sample and would then compare different trees.
    git init --quiet "$1"
    git -C "$1" remote add origin "https://github.com/$REPO.git"
    git -C "$1" fetch --quiet --depth 1 origin "$REF"
    git -C "$1" checkout --quiet --detach FETCH_HEAD
  else
    git clone --branch "$CLONE_REF" --depth 1 "https://github.com/$REPO.git" "$1"
  fi
}
git_full() {
  if [ -n "${GIT_REF:-}" ]; then
    git clone --branch "$GIT_REF" "https://github.com/$REPO.git" "$1"
  elif [ -n "${REF:-}" ]; then
    git clone --no-checkout "https://github.com/$REPO.git" "$1"
    if ! git -C "$1" cat-file -e "$REF^{commit}" 2>/dev/null; then
      git -C "$1" fetch --quiet origin "$REF"
    fi
    git -C "$1" checkout --quiet --detach "$REF"
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
  if [ -n "${EXPECTED_DIR:-}" ]; then
    rm -rf "$EXPECTED_DIR"
  fi
  if [ -n "${EXPECTED_ARCHIVE_DIR:-}" ]; then
    rm -rf "$EXPECTED_ARCHIVE_DIR"
  fi
}
trap cleanup EXIT

# The repo has to be added before the server will answer /refs, /sync or a clone
# for it. Admission and the entire artifact-readiness wait happen before any
# clone timer in run_one starts; build time is reported separately, never
# included in a clone result.
ensure_repo_added

warm_server
prepare_expected_tree

echo "=== repo=$REPO commit=${REF:-latest} rate=${RATE_MBPS}Mbps ripclone_runs=${RIPCLONE_RUNS:-$RUNS} native_runs=${NATIVE_RUNS:-$RUNS} modes=[$BENCH_MODES] ready=$READY_CLONEPACK shaped=${SHAPED:-1} host=$(hostname) cpus=$(nproc 2>/dev/null || echo ?) ==="
if [ "${SHAPED:-1}" = "1" ]; then
  apply_shape "$RATE_MBPS"
else
  echo "  running unshaped"
fi

echo "--- rate=${RATE_MBPS}Mbps ---"
BENCH_FAILED=0
if [ "${SKIP_RIPCLONE:-0}" != "1" ] && mode_enabled full; then
  bench_cmd "ripclone full (depth=0)" rc_full || BENCH_FAILED=1
fi
if [ "${SKIP_RIPCLONE:-0}" != "1" ] && mode_enabled depth1; then
  bench_cmd "ripclone depth=1"        rc_depth1 || BENCH_FAILED=1
fi
if [ "${SKIP_RIPCLONE:-0}" != "1" ] && mode_enabled files; then
  bench_cmd "ripclone files"          rc_files || BENCH_FAILED=1
fi
if [ "${SKIP_GIT:-0}" != "1" ] && mode_enabled git-full; then
  bench_cmd "git clone full"          git_full || BENCH_FAILED=1
fi
if [ "${SKIP_GIT:-0}" != "1" ] && mode_enabled git-depth1; then
  bench_cmd "git clone --depth 1"     git_depth1 || BENCH_FAILED=1
fi
if [ "${SKIP_GIT:-0}" != "1" ] && mode_enabled github-files; then
  bench_cmd "github archive files"   github_files || BENCH_FAILED=1
fi
exit "$BENCH_FAILED"
