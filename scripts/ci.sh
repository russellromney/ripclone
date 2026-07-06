#!/usr/bin/env bash
# Single source of truth for the CI checks. Run `scripts/ci.sh` locally before
# pushing and you run exactly what CI runs (same commands, same flags) — no more
# "passed locally, failed in CI". CI invokes individual stages in parallel jobs.
#
# Usage: scripts/ci.sh [lint|test|e2e|flake|all]   (default: all)
#
# All cargo commands use --locked so a stale/drifting Cargo.lock fails fast
# instead of silently resolving new dependencies.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="${1:-all}"
export RIPCLONE_TOKEN="${RIPCLONE_TOKEN:-ci-e2e-token}"

lint() {
  ( cd "$ROOT/rust"
    cargo fmt --all --check
    cargo clippy --all-targets --locked -- -D warnings )
}

# Unit + integration tests, parallel (the default) so cross-test races surface.
# (cargo test runs the test binaries sequentially, which keeps concurrent
# io_uring queue allocation bounded — nextest's all-binaries-at-once parallelism
# exhausts the runner's locked-memory limit while io_uring is the default writer.)
run_tests() {
  ( cd "$ROOT/rust" && cargo test --profile ci --all-targets --locked )
}

e2e() {
  ( cd "$ROOT/rust" && cargo build --release --bins )
  bash "$ROOT/scripts/e2e_local.sh"
}

# Tests + flake guard in one pass: compile once (ci profile), then run the suite
# a couple of times to catch nondeterministic races/ordering bugs a single run
# can miss. The ci profile is release-like but with more codegen units, so the
# compile is faster while tests still run optimized enough for the gate.
flake() {
  ( cd "$ROOT/rust"
    for i in 1 2; do
      echo "== test run $i/2 =="
      cargo test --profile ci --all-targets --locked
    done )
}

# Real network databases for the queue + metadata adapters the default suite can
# only compile-check: Postgres + MySQL via throwaway docker containers, and
# libsql against a local `sqld`. Needs docker; the libsql leg also needs `sqld`
# on PATH (the test auto-skips without it).
databases() {
  bash "$ROOT/scripts/test-queue-sql.sh"
  ( cd "$ROOT/rust" && cargo test --release --locked --test e2e_worker_libsql -- --nocapture )
}

case "$STAGE" in
  lint) lint ;;
  test) run_tests ;;
  e2e) e2e ;;
  flake) flake ;;
  databases) databases ;;
  all) lint; run_tests; e2e ;;
  *) echo "usage: scripts/ci.sh [lint|test|e2e|flake|databases|all]" >&2; exit 2 ;;
esac

echo "ci.sh: stage '$STAGE' OK"
