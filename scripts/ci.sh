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
  ( cd "$ROOT/rust"
    cargo fmt --all --check
    cargo clippy --all-targets --locked -- -D warnings )
}

# Unit + integration tests. cargo test runs the test binaries sequentially,
# which keeps concurrent io_uring queue allocation bounded — nextest's
# all-binaries-at-once parallelism exhausts the runner's locked-memory limit
# while io_uring is the default writer.
#
# When CI_ARTIFACTS is set (compile-once fan-out), run the prebuilt test
# executables from that dir — no cargo on this runner. Bin paths for tests that
# spawn CLI/worker processes must be exported as CARGO_BIN_EXE_*.
run_tests() {
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    local manifest="$CI_ARTIFACTS/test-executables.txt"
    [ -f "$manifest" ] || {
      echo "error: missing $manifest (ci-build did not stage test binaries)" >&2
      exit 1
    }
    export CARGO_BIN_EXE_ripclone="${CARGO_BIN_EXE_ripclone:-$CI_ARTIFACTS/ripclone}"
    export CARGO_BIN_EXE_ripclone-server="${CARGO_BIN_EXE_ripclone-server:-$CI_ARTIFACTS/ripclone-server}"
    export CARGO_BIN_EXE_ripclone-worker="${CARGO_BIN_EXE_ripclone-worker:-$CI_ARTIFACTS/ripclone-worker}"
    export CARGO_BIN_EXE_git-remote-ripclone="${CARGO_BIN_EXE_git-remote-ripclone:-$CI_ARTIFACTS/git-remote-ripclone}"
    for b in \
      "$CARGO_BIN_EXE_ripclone" \
      "$CARGO_BIN_EXE_ripclone-server" \
      "$CARGO_BIN_EXE_ripclone-worker" \
      "$CARGO_BIN_EXE_git-remote-ripclone"; do
      [ -x "$b" ] || {
        echo "error: missing product bin $b" >&2
        exit 1
      }
    done
    local status=0
    local name
    while IFS= read -r name; do
      [ -n "$name" ] || continue
      local bin="$CI_ARTIFACTS/$name"
      [ -x "$bin" ] || {
        echo "error: missing test binary $bin" >&2
        exit 1
      }
      echo "==> prebuilt test: $name"
      # Match cargo test: run from the crate root so CARGO_MANIFEST_DIR-relative
      # fixtures and relative paths behave the same.
      if ! ( cd "$ROOT/rust" && "$bin" ); then
        status=1
      fi
    done <"$manifest"
    return "$status"
  fi
  ( cd "$ROOT/rust" && cargo test --profile ci --all-targets --locked )
}

e2e() {
  # Prefer prebuilt bins from ci-build (CI_ARTIFACTS / RIPCLONE_BIN_DIR).
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    export RIPCLONE_BIN_DIR="${RIPCLONE_BIN_DIR:-$CI_ARTIFACTS}"
  elif [ -z "${RIPCLONE_BIN_DIR:-}" ]; then
    local profile="${CARGO_PROFILE:-ci}"
    ( cd "$ROOT/rust" && cargo build --profile "$profile" --locked --bins )
    export RIPCLONE_BIN_DIR="$ROOT/rust/target/$profile"
  fi
  bash "$ROOT/scripts/e2e_local.sh"
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

# Real network databases for the queue + metadata adapters the default suite can
# only compile-check: Postgres + MySQL via throwaway docker containers, and
# libsql against a local `sqld`. Needs docker; the libsql leg also needs `sqld`
# on PATH (the test auto-skips without it).
databases() {
  export CARGO_PROFILE="${CARGO_PROFILE:-ci}"
  bash "$ROOT/scripts/test-queue-sql.sh"
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    local bin="$CI_ARTIFACTS/e2e_worker_libsql"
    [ -x "$bin" ] || { echo "error: missing $bin" >&2; exit 1; }
    echo "databases: running prebuilt $bin"
    ( cd "$ROOT/rust" && "$bin" --nocapture )
  else
    ( cd "$ROOT/rust" && cargo test --profile "$CARGO_PROFILE" --locked --test e2e_worker_libsql -- --nocapture )
  fi
}

# Benchmark-harness smoke test. The benchmark scripts talk to the server over
# raw HTTP, so a change to the server's contract (like the B5 added-repos gate)
# does not recompile them — it silently breaks the harness against the next
# deploy. This runs the real benchmark/fly_shaped_benchmark.sh end-to-end against
# a local server and fails if the harness cannot add/warm/benchmark a fixture
# repo. Fast tier: file:// origin, unshaped, one run.
benchmark() {
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    export SERVER_BIN="${SERVER_BIN:-$CI_ARTIFACTS/ripclone-server}"
    export CLI_BIN="${CLI_BIN:-$CI_ARTIFACTS/ripclone}"
  elif [ -z "${SERVER_BIN:-}" ] || [ -z "${CLI_BIN:-}" ]; then
    local profile="${CARGO_PROFILE:-ci}"
    ( cd "$ROOT/rust" && cargo build --profile "$profile" --locked --bin ripclone --bin ripclone-server )
    export SERVER_BIN="${SERVER_BIN:-$ROOT/rust/target/$profile/ripclone-server}"
    export CLI_BIN="${CLI_BIN:-$ROOT/rust/target/$profile/ripclone}"
  fi
  bash "$ROOT/scripts/benchmark_smoke.sh"
}

# Compile-once fan-out: product bins + every test binary (`--all-targets`) for
# test/gitea/databases/docker/e2e/benchmark/s3gc. See scripts/ci-build-artifacts.sh.
ci_build() {
  bash "$ROOT/scripts/ci-build-artifacts.sh"
}

# Back-compat alias used by older workflow snippets / local muscle memory.
s3gc_build() {
  ci_build
}

# Run the S3-backed remote GC end-to-end suite against a local MinIO container
# (or any S3-compatible store pointed at by RIPCLONE_S3_ENDPOINT). This is the
# only place these #[ignored] tests are executed in CI.
#
# Optional $1: a single test name to run (CI shards one test per runner).
# Omit it to run the whole suite locally, same as before.
#
# When S3GC_TEST_BIN is set, runs that prebuilt binary directly (compile-once
# fan-out). Otherwise compiles + runs via cargo test.
s3gc() {
  local test_name="${1:-}"
  export RIPCLONE_S3_ENDPOINT="${RIPCLONE_S3_ENDPOINT:-http://127.0.0.1:9000}"
  export RIPCLONE_S3_BUCKET="${RIPCLONE_S3_BUCKET:-ripclone-test}"
  export RIPCLONE_S3_REGION="${RIPCLONE_S3_REGION:-us-east-1}"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

  local -a filter=(--ignored)
  if [ -n "$test_name" ]; then
    filter+=(--exact "$test_name")
  fi

  if [ -n "${S3GC_TEST_BIN:-}" ]; then
    if [ ! -x "$S3GC_TEST_BIN" ]; then
      echo "error: S3GC_TEST_BIN=$S3GC_TEST_BIN is not executable" >&2
      exit 1
    fi
    echo "s3gc: running prebuilt $S3GC_TEST_BIN ${filter[*]}"
    # Liberate from cargo so the binary's cwd/tmp behavior matches a direct run.
    ( cd "$ROOT/rust" && "$S3GC_TEST_BIN" "${filter[@]}" )
  else
    ( cd "$ROOT/rust" && cargo test --profile ci --locked --test e2e_remote_gc_s3 -- "${filter[@]}" )
  fi
}

case "$STAGE" in
  lint) lint ;;
  test) run_tests ;;
  e2e) e2e ;;
  flake) flake ;;
  databases) databases ;;
  ci-build|s3gc-build) ci_build ;;
  # Pass through any remaining args (e.g. a single test name for sharding).
  # Without this, `scripts/ci.sh s3gc some_test` ignored the name and every
  # "shard" re-ran the full suite (PR #126).
  s3gc) s3gc "${2:-}" ;;
  gitea) gitea ;;
  benchmark) benchmark ;;
  all) lint; run_tests; e2e ;;
  *) echo "usage: scripts/ci.sh [lint|test|e2e|flake|databases|ci-build|s3gc|gitea|benchmark|all]" >&2; exit 2 ;;
esac

echo "ci.sh: stage '$STAGE' OK"
