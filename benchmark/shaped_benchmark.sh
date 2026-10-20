#!/usr/bin/env bash
set -euo pipefail

# Single-rate shaped clone benchmark for a remote ripclone server.
#
# Usage (run on a Linux client with CAP_NET_ADMIN):
#   RIPCLONE_URL=https://your-ripclone-server.example.com \
#   RIPCLONE_SERVER_TOKEN=... \
#   ./benchmark/shaped_benchmark.sh <owner/repo> <rate_mbps> [runs] [target_dir]
#
# Set RIPCLONE_BENCH_PROVIDER for non-GitHub provider routes.
# Set BENCH_MODES to a space-separated subset of full, depth1, files,
# git-full, git-depth1, and github-files. The default remains the complete
# editable-clone matrix. Set
# RIPCLONE_BENCH_READY_RESULTS=head for a depth-one-only run so readiness does
# not wait for background Full or Files artifacts.
#
# Readiness: timing starts only after every requested output of the pinned
# commit is ready. Waiting on `full` also waits until the repo's refs are
# published (the Full carries `branches`); before that a full clone fetches
# every branch from upstream and times git, not ripclone.
#
# SKIP_ADD=1 makes the harness read-only on the server: it does not add the
# repo or admit builds. It pins upstream's default-branch tip (or BENCH_REF,
# which must then be a full commit SHA) and waits for the server to have it.
# The clones themselves still ask the server for the pinned commit, as any
# clone does.
#
# Set BENCH_RESULTS to a file to append one "repo commit label run ms" TSV
# row per sample.
#
# Samples are taken round-robin: run 1 of every mode, then run 2, and so on,
# so a slow minute upstream lands on all modes alike.
#
# Compared modes (each run uses a fresh dir and HOME, client cache disabled):
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
READY_RESULTS="${RIPCLONE_BENCH_READY_RESULTS:-}"

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

if [ -z "$READY_RESULTS" ]; then
  mode_enabled depth1 && READY_RESULTS="head"
  mode_enabled full && READY_RESULTS="${READY_RESULTS:+$READY_RESULTS }full"
  mode_enabled files && READY_RESULTS="${READY_RESULTS:+$READY_RESULTS }files"
  READY_RESULTS="${READY_RESULTS:-full}"
fi
for result in $READY_RESULTS; do
  case "$result" in
    head|full|files) ;;
    *) echo "error: RIPCLONE_BENCH_READY_RESULTS entries must be head, full, or files" >&2; exit 2 ;;
  esac
done

: "${RIPCLONE_URL:?RIPCLONE_URL must name the server under test}"
SERVER_URL="$RIPCLONE_URL"
TOKEN="${RIPCLONE_SERVER_TOKEN:-}"
RIPCLONE="${RIPCLONE:-ripclone}"
PROVIDER="${RIPCLONE_BENCH_PROVIDER:-github}"

REPO_NAME="$(basename "$REPO")"
SOURCE_URL="${BENCH_SOURCE_URL:-https://github.com/$REPO.git}"
REF_STATE_DIR="$TARGET/shaped_logs/${REPO_NAME}"
LOG_DIR="$REF_STATE_DIR/${RATE_MBPS}Mbps"
mkdir -p "$LOG_DIR"
# Keep one pin across a multi-rate sweep, but segregate root/non-root runs so
# sticky-directory ownership rules cannot make a rerun fail to update it.
RESOLVED_REF_FILE="$REF_STATE_DIR/resolved-ref-$(id -u)"

# Without a token, the harness and the clones read the server anonymously,
# as a user without a token would. Adding a repo needs the token.
if [ -n "$TOKEN" ]; then
  export RIPCLONE_SERVER_TOKEN="$TOKEN"
else
  echo "warning: RIPCLONE_SERVER_TOKEN not set; reading the server anonymously" >&2
  unset RIPCLONE_SERVER_TOKEN
fi
export RIPCLONE_SERVER="$SERVER_URL"
unset RIPCLONE_CACHE_DIR

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

# curl arguments that authenticate as the server token, if there is one.
AUTH=()
if [ -n "$TOKEN" ]; then
  AUTH=(-H "Authorization: Ripclone $(printf '%s' "$TOKEN" | sha256_hex)")
fi

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

# A repo must be `add`ed before the server builds or serves it; otherwise
# every request answers 404 with {"code":"repo_not_added"}. `add` is
# idempotent, so re-running the benchmark against an added repo is fine.
#
# Memoized: `add` admits the default branch's tip, so it must not be re-POSTed
# from inside a poll loop. Keep the exact commit it returns and poll that
# commit. All progress goes to stderr because the callers downstream of this
# run inside command substitutions that capture stdout.
REPO_ADDED=0
ADMITTED_COMMIT=""
ADMITTED_BRANCH=""

api_repo_url() {
  echo "${SERVER_URL%/}/v1/repos/$PROVIDER/$(repo_owner)/$(repo_name)"
}

# Print "commit<TAB>checkout_name" from a build summary.
summary_fields() {
  python3 -c '
import json, sys
try:
    value = json.load(sys.stdin)
except (json.JSONDecodeError, ValueError):
    raise SystemExit(0)
commit = value.get("commit") or ""
name = value.get("checkout_name") or ""
if commit:
    print(f"{commit}\t{name}")
' 2>/dev/null || true
}

ensure_repo_added() {
  if [ "$REPO_ADDED" = "1" ]; then return 0; fi
  if [ "${SKIP_ADD:-0}" = "1" ]; then
    REPO_ADDED=1
    return 0
  fi
  local status fields tmp attempt
  tmp="$(mktemp)"
  for attempt in $(seq 1 5); do
    status="000"
    status=$(curl --connect-timeout 5 --max-time 30 -s -o "$tmp" -w '%{http_code}' -X POST ${AUTH[@]+"${AUTH[@]}"} "$(api_repo_url)/add") || status="000"
    case "$status" in
      200)
        fields="$(summary_fields < "$tmp")"
        if [ -z "$fields" ]; then
          echo "error: add returned no commit" >&2
          cat "$tmp" >&2
          rm -f "$tmp"; return 1
        fi
        ADMITTED_COMMIT="${fields%%$'\t'*}"
        ADMITTED_BRANCH="${fields#*$'\t'}"
        echo "  repo $REPO is added at $ADMITTED_COMMIT" >&2
        REPO_ADDED=1; rm -f "$tmp"; return 0 ;;
      502|503|000)
        echo "  add attempt $attempt: HTTP $status, retrying ..." >&2
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

# Admit a selector (HEAD, a full SHA, a branch or a tag) and print
# "commit<TAB>checkout_name".
post_build() {
  local selector="$1" body
  body="$(SELECTOR="$selector" python3 -c 'import json, os; print(json.dumps({"selector": os.environ["SELECTOR"], "retry": True}))')"
  curl --connect-timeout 5 --max-time 30 -fsS -X POST ${AUTH[@]+"${AUTH[@]}"} \
    -H 'Content-Type: application/json' -d "$body" "$(api_repo_url)/builds" | summary_fields
}

get_default_branch() {
  local fields
  fields="$(post_build HEAD)"
  echo "${fields#*$'\t'}"
}

# Poll one output of an admitted commit until it is ready. A failed output
# stops the benchmark. A ready Full counts only once it carries `branches`:
# until the repo's refs are published, a full clone fetches every branch
# from upstream, which made one bun full clone take 146 s instead of 3 s.
wait_for_one_result_ready() {
  local commit="$1"
  local timeout="$2"
  local kind="$3"
  local start end out state status tmp why
  start=$(now_ms)
  echo "  waiting for $kind of $commit ..." >&2
  while true; do
    tmp="$(mktemp)"
    status="000"
    status=$(curl --connect-timeout 5 --max-time 30 -sS -o "$tmp" -w '%{http_code}' \
      ${AUTH[@]+"${AUTH[@]}"} "$(api_repo_url)/commits/$commit?kind=$kind") || status="000"
    out="$(cat "$tmp")"
    rm -f "$tmp"
    state=""
    if [ "$status" = "404" ] && [ "${SKIP_ADD:-0}" = "1" ]; then
      # Read-only: the server admits upstream's new tip on its own poll.
      state="not_admitted"
    elif [ "$status" != "200" ]; then
      echo "error: commit read returned HTTP $status" >&2
      printf '%s\n' "$out" >&2
      return 1
    else
      state="$(printf '%s' "$out" | KIND="$kind" python3 -c '
import json, os, sys
value = json.load(sys.stdin)
state = value.get("state", "")
if state == "ready" and os.environ["KIND"] == "full" and "branches" not in value:
    state = "refs_unpublished"
print(state)' 2>/dev/null || true)"
    fi
    case "$state" in
      ready)
        echo "  artifacts ready for admitted $commit" >&2
        return 0 ;;
      failed)
        echo "error: $kind of $commit failed" >&2
        printf '%s\n' "$out" >&2
        return 1 ;;
      refs_unpublished) why="full is ready but the repo's refs are not published" ;;
      not_admitted) why="the server has not admitted it" ;;
      *) why="${state:-unknown state}" ;;
    esac
    end=$(now_ms)
    if [ $((end - start)) -ge $((timeout * 1000)) ]; then
      echo "error: $kind of $commit not ready after ${timeout}s: $why" >&2
      return 1
    fi
    echo "    not ready yet ($why), retrying in 10s ..." >&2
    sleep 10
  done
}

# Read-only pin: upstream's default-branch tip, or BENCH_REF as a full SHA.
pin_upstream() {
  local listing
  if [ -n "${BENCH_REF:-}" ]; then
    if [[ ! "$BENCH_REF" =~ ^[0-9a-f]{40}$ ]]; then
      echo "error: with SKIP_ADD=1, BENCH_REF must be a full commit SHA" >&2
      return 1
    fi
    REF="$BENCH_REF"
    CLONE_REF="HEAD"
    return 0
  fi
  listing="$(git ls-remote --symref "$SOURCE_URL" HEAD)" || return 1
  REF="$(printf '%s\n' "$listing" | awk '$2 == "HEAD" && $1 ~ /^[0-9a-f]{40}$/ { print $1; exit }')"
  CLONE_REF="$(printf '%s\n' "$listing" | awk '$1 == "ref:" && $3 == "HEAD" { sub("^refs/heads/", "", $2); print $2; exit }')"
  CLONE_REF="${CLONE_REF:-HEAD}"
  [ -n "$REF" ] || { echo "error: could not read upstream HEAD of $SOURCE_URL" >&2; return 1; }
}

wait_for_commit_ready() {
  local commit="$1"
  local timeout="${2:-1200}"
  local result
  for result in $READY_RESULTS; do
    wait_for_one_result_ready "$commit" "$timeout" "$result" || return 1
  done
}

warm_server() {
  local branch_or_ref fields
  ensure_repo_added

  # CLONE_REF is the branch/tag name passed to `ripclone clone --branch`.
  # AT_REF is the exact commit passed to `--at`, so a branch advance after
  # readiness cannot change what is measured.
  if [ "${SKIP_SYNC:-0}" = "1" ]; then
    REF="${BENCH_REF:-${ADMITTED_COMMIT:-$(cat "$RESOLVED_REF_FILE" 2>/dev/null || true)}}"
    [ -n "$REF" ] || REF="$(get_default_branch)"
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

  if [ "${SKIP_ADD:-0}" = "1" ]; then
    pin_upstream || return 1
    echo "  pinned upstream $CLONE_REF -> $REF (read-only, no admission)"
  elif [ -n "${BENCH_REF:-}" ]; then
    # A full SHA, branch or tag: admit it and wait on the commit it names.
    branch_or_ref="${BENCH_REF#refs/heads/}"
    branch_or_ref="${branch_or_ref#refs/tags/}"
    echo "  warming server for $REPO @ $branch_or_ref ..."
    fields="$(post_build "$branch_or_ref")"
    [ -n "$fields" ] || { echo "error: could not admit $branch_or_ref" >&2; return 1; }
    REF="${fields%%$'\t'*}"
    if [[ "$branch_or_ref" =~ ^[0-9a-f]{40}$ ]]; then
      CLONE_REF="HEAD"
    else
      CLONE_REF="$branch_or_ref"
    fi
  else
    # `add` already admitted the default branch's tip.
    REF="$ADMITTED_COMMIT"
    [ -n "$REF" ] || { fields="$(post_build HEAD)"; REF="${fields%%$'\t'*}"; ADMITTED_BRANCH="${fields#*$'\t'}"; }
    CLONE_REF="${ADMITTED_BRANCH:-HEAD}"
  fi
  wait_for_commit_ready "$REF" 1200 || return 1
  AT_REF="$REF"
  echo "  resolved admitted $CLONE_REF -> $REF"

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
  local s e elapsed status=0
  # A fresh, empty HOME per sample: no saved login, git config or cache.
  rm -rf "$dir.home"
  mkdir -p "$dir.home"
  s=$(now_ms)
  (
    export HOME="$dir.home" XDG_CONFIG_HOME="$dir.home/.config" XDG_CACHE_HOME="$dir.home/.cache"
    "$@" "$dir"
  ) >"$cmd_log" 2>&1 || status=$?
  e=$(now_ms)
  rm -rf "$dir.home"
  if [ "$status" = "0" ]; then
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

# Queue one mode. run_cases samples the queued modes round-robin.
CASE_LABELS=()
CASE_FUNCS=()
CASE_TIMES=()
CASE_FAILED=()
add_case() {
  CASE_LABELS+=("$1")
  CASE_FUNCS+=("$2")
  CASE_TIMES+=("")
  CASE_FAILED+=(0)
}

case_runs() {
  case "$1" in
    "ripclone "*) echo "${RIPCLONE_RUNS:-$RUNS}" ;;
    *) echo "${NATIVE_RUNS:-$RUNS}" ;;
  esac
}

run_cases() {
  local i idx label count max_runs=0 log t deep_verify failed=0
  for label in "${CASE_LABELS[@]}"; do
    count="$(case_runs "$label")"
    if [ "$count" -gt "$max_runs" ]; then max_runs="$count"; fi
  done
  for i in $(seq 1 "$max_runs"); do
    for idx in "${!CASE_LABELS[@]}"; do
      label="${CASE_LABELS[$idx]}"
      [ "${CASE_FAILED[$idx]}" = "0" ] || continue
      [ "$i" -le "$(case_runs "$label")" ] || continue
      log="$LOG_DIR/${label}-run${i}.log"
      # Exact revision/shape checks run after every sample. The first sample
      # also performs the expensive whole-tree digest and object connectivity
      # proof; repeating those full reads does not strengthen the latency
      # distribution.
      deep_verify=0
      if [ "$i" -eq 1 ] || [ "${VERIFY_EVERY_RUN:-0}" = "1" ]; then
        deep_verify=1
      fi
      t=$(run_one "$label" "$log" "$deep_verify" "${CASE_FUNCS[$idx]}")
      if [ "$t" = "FAILED" ]; then
        echo "  $label: FAILED (run $i) — see $log"
        CASE_FAILED[idx]=1
        failed=1
        continue
      fi
      CASE_TIMES[idx]="${CASE_TIMES[$idx]} $t"
      if [ -n "${BENCH_RESULTS:-}" ]; then
        printf '%s\t%s\t%s\t%s\t%s\n' "$REPO" "$REF" "$label" "$i" "$t" >> "$BENCH_RESULTS"
      fi
    done
  done
  local med tail90 times
  for idx in "${!CASE_LABELS[@]}"; do
    [ "${CASE_FAILED[$idx]}" = "0" ] || continue
    read -r -a times <<< "${CASE_TIMES[$idx]}"
    med=$(printf '%s\n' "${times[@]}" | median)
    tail90=$(printf '%s\n' "${times[@]}" | p90)
    printf '  %-26s p50=%5dms p90=%5dms   runs=[%s]\n' "${CASE_LABELS[$idx]}" "$med" "$tail90" "$(IFS=,; echo "${times[*]}")"
  done
  return "$failed"
}

rc_full()  {
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" clone "$PROVIDER:$REPO" "$1" --branch "$CLONE_REF" --at "$AT_REF" --depth 0
  else
    "$RIPCLONE" clone "$PROVIDER:$REPO" "$1" --branch "$CLONE_REF" --depth 0
  fi
}
rc_depth1(){
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" clone "$PROVIDER:$REPO" "$1" --branch "$CLONE_REF" --at "$AT_REF" --depth 1
  else
    "$RIPCLONE" clone "$PROVIDER:$REPO" "$1" --branch "$CLONE_REF" --depth 1
  fi
}
rc_files() {
  if [ -n "$AT_REF" ]; then
    "$RIPCLONE" clone "$PROVIDER:$REPO" "$1" --branch "$CLONE_REF" --at "$AT_REF" --depth 1 --mode files
  else
    "$RIPCLONE" clone "$PROVIDER:$REPO" "$1" --branch "$CLONE_REF" --depth 1 --mode files
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
    git clone --quiet --depth 1 --branch "$CLONE_REF" "$SOURCE_URL" "$EXPECTED_DIR"
  else
    git clone --quiet --depth 1 "$SOURCE_URL" "$EXPECTED_DIR"
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
    git clone --branch "$GIT_REF" --depth 1 "$SOURCE_URL" "$1"
  elif [ -n "${REF:-}" ]; then
    # Fetch the same immutable commit that admission selected. A branch clone
    # can move during a long sample and would then compare different trees.
    git init --quiet "$1"
    git -C "$1" remote add origin "$SOURCE_URL"
    git -C "$1" fetch --quiet --depth 1 origin "$REF"
    git -C "$1" checkout --quiet --detach FETCH_HEAD
  else
    git clone --branch "$CLONE_REF" --depth 1 "$SOURCE_URL" "$1"
  fi
}
git_full() {
  if [ -n "${GIT_REF:-}" ]; then
    git clone --branch "$GIT_REF" "$SOURCE_URL" "$1"
  elif [ -n "${REF:-}" ]; then
    git clone --no-checkout "$SOURCE_URL" "$1"
    if ! git -C "$1" cat-file -e "$REF^{commit}" 2>/dev/null; then
      git -C "$1" fetch --quiet origin "$REF"
    fi
    git -C "$1" checkout --quiet --detach "$REF"
  else
    git clone --branch "$CLONE_REF" "$SOURCE_URL" "$1"
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

# The repo has to be added before the server will build or serve a clone
# for it. Admission and the entire artifact-readiness wait happen before any
# clone timer in run_one starts; build time is reported separately, never
# included in a clone result.
ensure_repo_added

warm_server
if [ -z "$TOKEN" ]; then
  # A server may require sign-in for a clone that names a commit or branch.
  # Without a token, clone the default branch, as `ripclone clone owner/repo`
  # does; every sample still checks that it got $REF.
  echo "  no token: cloning the default branch without --at; samples must still be $REF"
  CLONE_REF="HEAD"
  AT_REF=""
fi
prepare_expected_tree

echo "=== repo=$REPO commit=${REF:-latest} rate=${RATE_MBPS}Mbps ripclone_runs=${RIPCLONE_RUNS:-$RUNS} native_runs=${NATIVE_RUNS:-$RUNS} modes=[$BENCH_MODES] ready=[$READY_RESULTS] shaped=${SHAPED:-1} host=$(hostname) cpus=$(nproc 2>/dev/null || echo ?) ==="
if [ "${SHAPED:-1}" = "1" ]; then
  apply_shape "$RATE_MBPS"
else
  echo "  running unshaped"
fi

echo "--- rate=${RATE_MBPS}Mbps ---"
if [ "${SKIP_RIPCLONE:-0}" != "1" ]; then
  if mode_enabled full;   then add_case "ripclone full (depth=0)" rc_full; fi
  if mode_enabled depth1; then add_case "ripclone depth=1"        rc_depth1; fi
  if mode_enabled files;  then add_case "ripclone files"          rc_files; fi
fi
if [ "${SKIP_GIT:-0}" != "1" ]; then
  if mode_enabled git-full;     then add_case "git clone full"       git_full; fi
  if mode_enabled git-depth1;   then add_case "git clone --depth 1"  git_depth1; fi
  if mode_enabled github-files; then add_case "github archive files" github_files; fi
fi
BENCH_FAILED=0
if [ "${#CASE_LABELS[@]}" -gt 0 ]; then
  run_cases || BENCH_FAILED=1
fi
exit "$BENCH_FAILED"
