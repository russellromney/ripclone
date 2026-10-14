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
export RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-ci-e2e-token}"

lint() {
  bash "$ROOT/scripts/audit_control_support.sh"
  bash "$ROOT/scripts/audit_release_surface.sh"
  ( cd "$ROOT/rust"
    cargo fmt --all --check
    cargo clippy --all-targets --locked -- -D warnings )
}

# Unit tests run one at a time: some still mutate process env, and io_uring
# queue allocation must stay under the runner's locked-memory limit. The
# contract suite runs in parallel: each test owns its server, port, and temp
# dir, and races only show up under concurrent load.
#
# This job self-compiles (ci profile + sccache/rust-cache). Fan-out jobs
# (gitea/e2e/s3/…) use prebuilt binaries from ci-build instead.
run_tests() {
  ( cd "$ROOT/rust" && cargo test --profile ci --locked --lib --bins -- --test-threads=1 )
  ( cd "$ROOT/rust" && cargo test --profile ci --locked --test contract )
}

e2e() {
  # Prefer prebuilt bins from ci-build (CI_ARTIFACTS / RIPCLONE_BIN_DIR).
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    export RIPCLONE_BIN_DIR="${RIPCLONE_BIN_DIR:-$CI_ARTIFACTS}"
  elif [ -z "${RIPCLONE_BIN_DIR:-}" ]; then
    local profile="${CARGO_PROFILE:-ci}"
    local target_root="${CARGO_TARGET_DIR:-$ROOT/rust/target}"
    ( cd "$ROOT/rust" && cargo build --profile "$profile" --locked --bins )
    export RIPCLONE_BIN_DIR="$target_root/$profile"
  fi
  bash "$ROOT/scripts/e2e_local.sh"
}

# Historical flake-guard (ran the suite twice). Kept as an alias of `test` so
# local muscle memory (`scripts/ci.sh flake`) still works; CI no longer doubles
# the gate — one run is enough and the second run was ~half of overall wall.
flake() {
  run_tests
}

# Benchmark-harness smoke test. The benchmark scripts talk to the server over
# raw HTTP, so a change to the server's contract (like the B5 added-repos gate)
# does not recompile them — it silently breaks the harness against the next
# deploy. This runs the real benchmark/shaped_benchmark.sh end-to-end against
# a local server and fails if the harness cannot add/warm/benchmark a fixture
# repo. Fast tier: file:// origin, unshaped, one run.
benchmark() {
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    export SERVER_BIN="${SERVER_BIN:-$CI_ARTIFACTS/ripclone-server}"
    export CLI_BIN="${CLI_BIN:-$CI_ARTIFACTS/ripclone}"
  elif [ -z "${SERVER_BIN:-}" ] || [ -z "${CLI_BIN:-}" ]; then
    local profile="${CARGO_PROFILE:-ci}"
    local target_root="${CARGO_TARGET_DIR:-$ROOT/rust/target}"
    ( cd "$ROOT/rust" && cargo build --profile "$profile" --locked --bin ripclone --bin ripclone-server )
    export SERVER_BIN="${SERVER_BIN:-$target_root/$profile/ripclone-server}"
    export CLI_BIN="${CLI_BIN:-$target_root/$profile/ripclone}"
  fi
  bash "$ROOT/scripts/benchmark_smoke.sh"
}

# Compile-once fan-out: product bins + integration tests for
# gitea/docker/e2e/benchmark. See scripts/ci-build-artifacts.sh.
ci_build() {
  bash "$ROOT/scripts/ci-build-artifacts.sh"
}

case "$STAGE" in
  lint) lint ;;
  test) run_tests ;;
  e2e) e2e ;;
  flake) flake ;;
  ci-build) ci_build ;;
  benchmark) benchmark ;;
  all) lint; run_tests; e2e ;;
  *) echo "usage: scripts/ci.sh [lint|test|e2e|flake|ci-build|benchmark|all]" >&2; exit 2 ;;
esac

echo "ci.sh: stage '$STAGE' OK"
