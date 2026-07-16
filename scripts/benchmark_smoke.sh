#!/usr/bin/env bash
set -euo pipefail

# Smoke test for benchmark/fly_shaped_benchmark.sh.
#
# The benchmark harness talks to the server over raw HTTP, so it does not
# recompile when the server's contract changes — it just silently stops working
# against the next deploy. This test runs the real harness end-to-end against a
# real local ripclone server so a contract change breaks CI instead.
#
# Two phases:
#
#   1. pre-added-repos server: an HTTP stub with no /add route. The harness must
#      tolerate the 404 and still warm up, so the currently-deployed benchmark
#      server keeps working.
#   2. current server: a real ripclone-server, which enforces the added-repos
#      gate. The test first proves the gate is live (a sync without an add is
#      rejected with repo_not_added), then requires the harness to run clean and
#      produce a timing row. A harness that forgets to `add` fails here.
#
# Runs unshaped (no CAP_NET_ADMIN), one run per mode, no native-git baseline,
# against a file:// origin — offline and a few seconds of work.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BENCH="$ROOT_DIR/benchmark/fly_shaped_benchmark.sh"

# Debug binaries are enough: this asserts on the request/response contract, not
# on clone throughput.
SERVER_BIN="${SERVER_BIN:-$ROOT_DIR/rust/target/debug/ripclone-server}"
CLI_BIN="${CLI_BIN:-$ROOT_DIR/rust/target/debug/ripclone}"
for bin in "$SERVER_BIN" "$CLI_BIN"; do
  [ -x "$bin" ] || { echo "error: missing binary $bin (cargo build --bins)" >&2; exit 1; }
done

export RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-bench-smoke-token}"
# Per-repo access enforcement probes the provider over HTTP and cannot reach a
# file:// origin. Single-tenant local run; the shared token is the only auth.
export RIPCLONE_TRUST_GATEWAY=1

sha256() { if command -v sha256sum >/dev/null 2>&1; then sha256sum | awk '{print $1}'; else shasum -a 256 | awk '{print $1}'; fi; }
TOKEN_HASH="$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | sha256)"

OWNER="bench"
NAME="tiny"
REPO="$OWNER/$NAME"

BASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-bench-smoke.XXXXXX")"
ORIGIN_ROOT="$BASE_DIR/origins"
SERVER_PID=""
STUB_PID=""

# Reap the background server/stub quietly. `wait` after `kill` absorbs the
# shell's async "Terminated" job notice so it does not clutter CI logs.
cleanup() {
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  if [ -n "$STUB_PID" ]; then kill "$STUB_PID" 2>/dev/null || true; wait "$STUB_PID" 2>/dev/null || true; fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  ok: $*"; }

free_port() { echo $(( 20000 + RANDOM % 40000 )); }

wait_healthy() { # url pid
  local _
  for _ in $(seq 1 200); do
    curl -fsS -o /dev/null "$1/healthz" 2>/dev/null && return 0
    kill -0 "$2" 2>/dev/null || return 1
    sleep 0.1
  done
  return 1
}

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

# A stand-in for the deployed benchmark server, which predates the added-repos
# model: it serves refs and sync but has no /add route, so /add falls through to
# the router's plain 404.
write_stub_server() {
  cat > "$BASE_DIR/stub.py" <<'PY'
import json, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

COMMIT = "0" * 39 + "1"
PREFIX = "/v1/repos/github/bench/tiny"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def path_only(self):
        return self.path.split("?", 1)[0]

    def do_GET(self):
        path = self.path_only()
        if path == "/healthz":
            return self.reply(200, {"status": "ok"})
        if path == PREFIX + "/refs/HEAD":
            return self.reply(200, {"default_branch": "main", "commit": COMMIT,
                                    "full_pack": "stub"})
        if path == PREFIX + "/refs/main":
            return self.reply(200, {"commit": COMMIT, "full_pack": "stub"})
        return self.reply(404, {"error": "not found"})

    def do_POST(self):
        path = self.path_only()
        if path == PREFIX + "/sync":
            return self.reply(200, {"commit": COMMIT})
        # No /add route on this build — exactly what the old router answers.
        return self.reply(404, {"error": "not found"})


ThreadingHTTPServer(("127.0.0.1", int(os.environ["RIPCLONE_STUB_PORT"])),
                    Handler).serve_forever()
PY
}

# ===========================================================================
# Phase 1 — a server with no /add route (pre-added-repos). The harness must not
# treat the 404 as fatal.
# ===========================================================================
phase_pre_add_server() {
  echo "==> phase 1: harness against a server with no /add route"
  local port url log
  port="$(free_port)"
  url="http://127.0.0.1:$port"

  write_stub_server
  RIPCLONE_STUB_PORT="$port" python3 "$BASE_DIR/stub.py" \
    >"$BASE_DIR/stub.log" 2>&1 &
  STUB_PID=$!
  wait_healthy "$url" "$STUB_PID" || { cat "$BASE_DIR/stub.log"; fail "stub server not ready"; }

  log="$BASE_DIR/phase1.log"
  # Compat check only: the harness must warm up and reach the benchmark header
  # against a server that has no /add route, so the currently-deployed benchmark
  # server keeps working. No clones — the stub serves metadata only. This must
  # pass for BOTH the fixed harness and the old sync-only one (neither is
  # rejected by a server with no gate), so the B5 regression is caught in phase
  # 2, not here.
  if ! env -u BENCH_REF SHAPED=0 RUNS=1 SKIP_GIT=1 SKIP_RIPCLONE=1 \
      RIPCLONE_URL="$url" RIPCLONE="$CLI_BIN" \
      bash "$BENCH" "$REPO" 1000 1 "$BASE_DIR/target1" >"$log" 2>&1; then
    cat "$log" >&2
    fail "harness must survive a server with no /add route (pre-added-repos deploy)"
  fi
  grep -q "repo=$REPO" "$log" || { cat "$log" >&2; fail "harness did not reach the benchmark header"; }
  pass "harness tolerates a pre-added-repos server (404 on /add)"

  kill "$STUB_PID" 2>/dev/null || true
  wait "$STUB_PID" 2>/dev/null || true
  STUB_PID=""
}

# ===========================================================================
# Phase 2 — a real server, which enforces the added-repos gate.
# ===========================================================================
phase_real_server() {
  echo "==> phase 2: harness against a real ripclone-server"
  local port url log body
  port="$(free_port)"
  url="http://127.0.0.1:$port"

  RUST_LOG=warn RIPCLONE_ORIGIN_BASE="file://$ORIGIN_ROOT" \
    "$SERVER_BIN" --cas-dir "$BASE_DIR/cas" --repo-root "$BASE_DIR/repos" \
    --host 127.0.0.1 --port "$port" >"$BASE_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  wait_healthy "$url" "$SERVER_PID" || { cat "$BASE_DIR/server.log"; fail "server not ready"; }

  # Control: the gate must actually be live on the server under test, otherwise
  # the assertion below proves nothing. A sync on a repo that was never added
  # has to be rejected with repo_not_added.
  body="$(curl -s -X POST -H "Authorization: Ripclone $TOKEN_HASH" \
    "$url/v1/repos/github/$REPO/sync?branch=main")"
  case "$body" in
    *repo_not_added*) pass "added-repos gate is live (sync without add -> repo_not_added)" ;;
    *) fail "server under test does not enforce the added-repos gate; this smoke test cannot catch the regression (got: $body)" ;;
  esac

  log="$BASE_DIR/phase2.log"
  if ! env -u BENCH_REF SHAPED=0 RUNS=1 SKIP_GIT=1 \
      RIPCLONE_URL="$url" RIPCLONE="$CLI_BIN" \
      bash "$BENCH" "$REPO" 1000 1 "$BASE_DIR/target2" >"$log" 2>&1; then
    cat "$log" >&2
    # The harness swallows response bodies, so name the likely cause here.
    if ! grep -Eq 'repo .* (is )?added' "$log"; then
      echo "HINT: the harness never added $REPO. The server rejects sync/refs/clone" >&2
      echo "      for a repo that was never added: 404 {\"code\":\"repo_not_added\"}." >&2
    fi
    fail "benchmark harness exited non-zero against a current ripclone-server"
  fi

  if grep -q 'repo_not_added' "$log"; then cat "$log" >&2; fail "harness hit repo_not_added"; fi
  if grep -q 'FAILED' "$log"; then cat "$log" >&2; fail "a benchmark run failed"; fi
  grep -Eq 'repo .* (is )?added' "$log" || { cat "$log" >&2; fail "harness never added the repo"; }
  grep -qE 'ripclone full \(depth=0\) +median= *[0-9]+ms' "$log" \
    || { cat "$log" >&2; fail "harness produced no timing row"; }

  echo "--- harness output ---"
  cat "$log"
  pass "harness added, warmed, and benchmarked $REPO"
}

make_origin
phase_pre_add_server
phase_real_server
echo "benchmark_smoke.sh: OK"
