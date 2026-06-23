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
run_tests() {
  ( cd "$ROOT/rust" && cargo test --release --all-targets --locked )
}

e2e() {
  ( cd "$ROOT/rust" && cargo build --release --bins )
  bash "$ROOT/scripts/e2e_local.sh"
}

# Tests + flake guard in one pass: compile once (release), then run the suite a
# few times to catch nondeterministic races/ordering bugs a single run can miss.
# Reusing the release profile means no separate debug compile.
flake() {
  ( cd "$ROOT/rust"
    for i in 1 2 3; do
      echo "== test run $i/3 =="
      cargo test --release --all-targets --locked
    done )
}

case "$STAGE" in
  lint) lint ;;
  test) run_tests ;;
  e2e) e2e ;;
  flake) flake ;;
  all) lint; run_tests; e2e ;;
  *) echo "usage: scripts/ci.sh [lint|test|e2e|flake|all]" >&2; exit 2 ;;
esac

echo "ci.sh: stage '$STAGE' OK"
