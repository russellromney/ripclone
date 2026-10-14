#!/usr/bin/env bash
set -euo pipefail

# Smoke test for benchmark/shaped_benchmark.sh.
#
# The benchmark harness talks to the server over raw HTTP, so it does not
# recompile when the server's contract changes — it just silently stops working
# against the next deploy. This test runs the real harness end-to-end against a
# real local ripclone server so a contract change breaks CI instead.
#
# A real current ripclone-server enforces the added-repos gate. The test first
# proves the gate is live (a sync without an add is rejected with
# repo_not_added), then requires the harness to run clean and produce a timing
# row. A harness that forgets to `add` fails here.
#
# Runs unshaped (no CAP_NET_ADMIN), one run per mode, no native-git baseline,
# against a file:// origin — offline and a few seconds of work.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BENCH="$ROOT_DIR/benchmark/shaped_benchmark.sh"
# shellcheck source=server_url.sh
source "$SCRIPT_DIR/server_url.sh"

# Debug binaries are enough: this asserts on the request/response contract, not
# on clone throughput.
SERVER_BIN="${SERVER_BIN:-$ROOT_DIR/rust/target/debug/ripclone-server}"
CLI_BIN="${CLI_BIN:-$ROOT_DIR/rust/target/debug/ripclone}"
for bin in "$SERVER_BIN" "$CLI_BIN"; do
  [ -x "$bin" ] || { echo "error: missing binary $bin (cargo build --bins)" >&2; exit 1; }
done

export RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-bench-smoke-token}"

sha256() { if command -v sha256sum >/dev/null 2>&1; then sha256sum | awk '{print $1}'; else shasum -a 256 | awk '{print $1}'; fi; }
TOKEN_HASH="$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | sha256)"

OWNER="bench"
NAME="tiny"
REPO="$OWNER/$NAME"

BASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-bench-smoke.XXXXXX")"
ORIGIN_ROOT="$BASE_DIR/origins"
SERVER_PID=""

# Reap the background server quietly. `wait` after `kill` absorbs the
# shell's async "Terminated" job notice so it does not clutter CI logs.
cleanup() {
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  ok: $*"; }

# --- fixture: a bare origin the built-in github provider fetches over file:// --
make_origin() {
  local work="$BASE_DIR/work"
  local bare="$ORIGIN_ROOT/$OWNER/$NAME.git"
  mkdir -p "$work" "$(dirname "$bare")"
  git init --bare -q -b main "$bare"
  git init -q -b main "$work"
  git -C "$work" config user.email smoke@ripclone.local
  git -C "$work" config user.name "bench smoke"
  printf 'one\n' > "$work/a.txt"
  git -C "$work" add -A && git -C "$work" commit -q -m c1
  printf 'two\n' > "$work/a.txt"
  git -C "$work" add -A && git -C "$work" commit -q -m c2
  git -C "$work" push -q --force "$bare" main
  git -C "$bare" symbolic-ref HEAD refs/heads/main
}

run_current_server() {
  echo "==> benchmark harness against a real ripclone-server"
  local url log body

  RUST_LOG=warn,ripclone::server=info RIPCLONE_CONFIG="$BASE_DIR/missing-config.toml" \
    RIPCLONE_ORIGIN_BASE="file://$ORIGIN_ROOT" \
    "$SERVER_BIN" --cas-dir "$BASE_DIR/cas" --repo-root "$BASE_DIR/repos" \
    --host 127.0.0.1 --port 0 >"$BASE_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  url="$(server_url "$BASE_DIR/server.log" "$SERVER_PID")" ||
    { cat "$BASE_DIR/server.log"; fail "server not ready"; }

  # Control: the gate must actually be live on the server under test, otherwise
  # the assertion below proves nothing. A build of a repo that was never added
  # has to be rejected with repo_not_added.
  body="$(curl -s -X POST -H "Authorization: Ripclone $TOKEN_HASH" \
    -H 'Content-Type: application/json' -d '{"selector":"main"}' \
    "$url/v1/repos/github/$REPO/builds")"
  case "$body" in
    *repo_not_added*) pass "added-repos gate is live (build without add -> repo_not_added)" ;;
    *) fail "server under test does not enforce the added-repos gate; this smoke test cannot catch the regression (got: $body)" ;;
  esac

  log="$BASE_DIR/current-server.log"
  if ! env -u BENCH_REF SHAPED=0 RUNS=1 SKIP_GIT=1 \
      RIPCLONE_URL="$url" RIPCLONE="$CLI_BIN" \
      RIPCLONE_CONFIG="$BASE_DIR/missing-config.toml" \
      BENCH_SOURCE_URL="file://$ORIGIN_ROOT/$REPO.git" \
      bash "$BENCH" "$REPO" 1000 1 "$BASE_DIR/target2" >"$log" 2>&1; then
    cat "$log" >&2
    # The harness swallows response bodies, so name the likely cause here.
    if ! grep -q 'repo .* is added' "$log"; then
      echo "HINT: the harness never added $REPO. The server rejects builds and clones" >&2
      echo "      for a repo that was never added: 404 {\"code\":\"repo_not_added\"}." >&2
    fi
    fail "benchmark harness exited non-zero against a current ripclone-server"
  fi

  if grep -q 'repo_not_added' "$log"; then cat "$log" >&2; fail "harness hit repo_not_added"; fi
  if grep -q 'FAILED' "$log"; then cat "$log" >&2; fail "a benchmark run failed"; fi
  grep -q 'repo .* is added' "$log" || { cat "$log" >&2; fail "harness never added the repo"; }
  grep -qE 'artifacts ready for admitted [0-9a-f]{40}' "$log" \
    || { cat "$log" >&2; fail "harness did not poll the admitted commit"; }
  grep -qE 'resolved admitted .* -> [0-9a-f]{40}' "$log" \
    || { cat "$log" >&2; fail "harness did not retain the admitted commit"; }
  grep -qE 'ripclone full \(depth=0\) +p50= *[0-9]+ms +p90= *[0-9]+ms' "$log" \
    || { cat "$log" >&2; fail "harness produced no timing row"; }

  echo "--- harness output ---"
  cat "$log"
  pass "harness added, warmed, and benchmarked $REPO"
}

make_origin
run_current_server
echo "benchmark_smoke.sh: OK"
