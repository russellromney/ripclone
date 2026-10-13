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
    cargo clippy --all-targets --features dev-tools --locked -- -D warnings )
}

# Unit + integration tests. cargo test runs the test binaries sequentially,
# which keeps concurrent io_uring queue allocation bounded — nextest's
# all-binaries-at-once parallelism exhausts the runner's locked-memory limit
# while io_uring is the default writer.
#
# This job self-compiles (ci profile + sccache/rust-cache). Fan-out jobs
# (gitea/e2e/…) use prebuilt binaries from ci-build instead;
# staging the full suite there was ~30m cold before integration-test
# consolidation and is not worth it.
run_tests() {
  # The local-server harness has both LSM-on and LSM-off fixtures. LSM mode is
  # process-global, so execute its two groups in separate processes while
  # retaining one compiled test binary. The broad pass still compiles every
  # target and runs every other test.
  ( cd "$ROOT/rust" && cargo test --profile ci --all-targets --locked -- \
      --test-threads=1 --skip lsm_on_ --skip lsm_off_ )

  local filter listed
  for filter in lsm_on_ lsm_off_; do
    listed="$(cd "$ROOT/rust" && timeout 60 cargo test --profile ci --locked \
      --test local_server -- "$filter" --list)"
    if ! grep -Fq "$filter" <<<"$listed"; then
      echo "required local-server group matched zero tests: $filter" >&2
      exit 1
    fi
    ( cd "$ROOT/rust" && cargo test --profile ci --locked --test local_server -- \
        "$filter" --test-threads=1 )
  done
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
  bash "$ROOT/scripts/e2e_smart_http.sh"
}

# Historical flake-guard (ran the suite twice). Kept as an alias of `test` so
# local muscle memory (`scripts/ci.sh flake`) still works; CI no longer doubles
# the gate — one run is enough and the second run was ~half of overall wall.
flake() {
  run_tests
}

# Real multi-provider + server-side-token path against a live Gitea (the seam a
# production dogfood found broken but every file:// e2e missed — the #114
# provider-token clobber). Needs a running Gitea; the CI job brings one up and
# exports RIPCLONE_GITEA_URL / _TOKEN / _USER. The test auto-skips if they're
# unset, so a bare `scripts/ci.sh gitea` on a laptop without Gitea is a no-op.
gitea() {
  export RIPCLONE_GITEA_URL="${RIPCLONE_GITEA_URL:-http://127.0.0.1:3000}"
  export RIPCLONE_GITEA_USER="${RIPCLONE_GITEA_USER:-ci}"
  : "${RIPCLONE_GITEA_TOKEN:?set RIPCLONE_GITEA_TOKEN to a Gitea admin access token}"
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    local bin="$CI_ARTIFACTS/e2e_gitea_provider"
    [ -x "$bin" ] || { echo "error: missing $bin" >&2; exit 1; }
    echo "gitea: running prebuilt $bin"
    ( cd "$ROOT/rust" && "$bin" --ignored --nocapture )
  else
    local profile="${CARGO_PROFILE:-ci}"
    ( cd "$ROOT/rust" && cargo test --profile "$profile" --locked --test e2e_gitea_provider -- --ignored --nocapture )
  fi
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
  gitea) gitea ;;
  benchmark) benchmark ;;
  all) lint; run_tests; e2e ;;
  *) echo "usage: scripts/ci.sh [lint|test|e2e|flake|ci-build|gitea|benchmark|all]" >&2; exit 2 ;;
esac

echo "ci.sh: stage '$STAGE' OK"
