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

# Unit + integration tests, parallel (the default) so cross-test races surface.
# (cargo test runs the test binaries sequentially, which keeps concurrent
# io_uring queue allocation bounded — nextest's all-binaries-at-once parallelism
# exhausts the runner's locked-memory limit while io_uring is the default writer.)
run_tests() {
  ( cd "$ROOT/rust" && cargo test --profile ci --all-targets --locked )
}

e2e() {
  # ci profile: release-like speed, shares the unit-test graph. Full --release
  # recompiled a third graph for this one job alone.
  local profile="${CARGO_PROFILE:-ci}"
  ( cd "$ROOT/rust" && cargo build --profile "$profile" --locked --bins )
  # e2e_local.sh looks for target/release/* by default; point it at the
  # profile dir when not building release.
  if [ "$profile" != "release" ]; then
    export RIPCLONE_BIN_DIR="${RIPCLONE_BIN_DIR:-$ROOT/rust/target/$profile}"
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
  # ci profile (same as the unit-test gate). Default/dev recompiles the whole
  # graph for this one job and dominated wall time on cold runners (~6 min).
  local profile="${CARGO_PROFILE:-ci}"
  ( cd "$ROOT/rust" && cargo test --profile "$profile" --locked --test e2e_gitea_provider -- --ignored --nocapture )
}

# Real network databases for the queue + metadata adapters the default suite can
# only compile-check: Postgres + MySQL via throwaway docker containers, and
# libsql against a local `sqld`. Needs docker; the libsql leg also needs `sqld`
# on PATH (the test auto-skips without it).
databases() {
  # One profile for the whole job: previously test-queue-sql compiled the
  # default (dev) graph and libsql recompiled --release, paying two full
  # builds. `ci` matches the unit-test gate.
  export CARGO_PROFILE="${CARGO_PROFILE:-ci}"
  bash "$ROOT/scripts/test-queue-sql.sh"
  ( cd "$ROOT/rust" && cargo test --profile "$CARGO_PROFILE" --locked --test e2e_worker_libsql -- --nocapture )
}

# Benchmark-harness smoke test. The benchmark scripts talk to the server over
# raw HTTP, so a change to the server's contract (like the B5 added-repos gate)
# does not recompile them — it silently breaks the harness against the next
# deploy. This runs the real benchmark/fly_shaped_benchmark.sh end-to-end against
# a local debug server and fails if the harness cannot add/warm/benchmark a
# fixture repo (e.g. is rejected with repo_not_added). Fast tier: debug binaries,
# file:// origin, unshaped, one run. Needs the debug ripclone + ripclone-server.
benchmark() {
  # ci profile (not default debug): shares the unit-test graph. Debug was a
  # third full compile for a harness smoke that only needs "binaries run".
  local profile="${CARGO_PROFILE:-ci}"
  ( cd "$ROOT/rust" && cargo build --profile "$profile" --locked --bin ripclone --bin ripclone-server )
  export SERVER_BIN="${SERVER_BIN:-$ROOT/rust/target/$profile/ripclone-server}"
  export CLI_BIN="${CLI_BIN:-$ROOT/rust/target/$profile/ripclone}"
  bash "$ROOT/scripts/benchmark_smoke.sh"
}

# Compile the S3 GC e2e test binary (and the ripclone CLI it shells out to)
# once, no run. Uses the `ci` profile (release opts, many codegen units, no LTO)
# so compile finishes faster than full --release while still running optimized.
# Stages both under rust/target/ci/ with stable names for artifact upload.
s3gc_build() {
  ( cd "$ROOT/rust"
    # --message-format=json so we can pick the exact test binary path without
    # grepping the human log (which is unstable across cargo versions).
    local bin
    bin="$(
      cargo test --profile ci --locked --test e2e_remote_gc_s3 --no-run --message-format=json \
        | jq -r 'select(.reason == "compiler-artifact"
                        and .target.kind == ["test"]
                        and .target.name == "e2e_remote_gc_s3"
                        and .executable != null)
                 | .executable' \
        | tail -n1
    )"
    if [ -z "$bin" ] || [ ! -x "$bin" ]; then
      echo "error: could not locate e2e_remote_gc_s3 test binary after build" >&2
      exit 1
    fi
    # One test spawns the CLI (expired_bearer_…); build it in the same job so
    # shards can download both and set CARGO_BIN_EXE_ripclone at runtime.
    cargo build --profile ci --locked --bin ripclone

    # Prefer CARGO_TARGET_DIR when set (worktrees share the main checkout's
    # target/); otherwise fall back to the package-local target/.
    # --profile ci writes under target/ci/ (not target/release/).
    local target_root="${CARGO_TARGET_DIR:-$ROOT/rust/target}"
    mkdir -p "$target_root/ci" "$ROOT/rust/target/ci"
    cp -f "$bin" "$target_root/ci/e2e_remote_gc_s3"
    chmod +x "$target_root/ci/e2e_remote_gc_s3"
    # Stage package-local copies for CI artifact upload paths.
    if [ "$target_root" != "$ROOT/rust/target" ]; then
      cp -f "$target_root/ci/e2e_remote_gc_s3" "$ROOT/rust/target/ci/e2e_remote_gc_s3"
      cp -f "$target_root/ci/ripclone" "$ROOT/rust/target/ci/ripclone"
    fi
    echo "s3gc-build: wrote $target_root/ci/e2e_remote_gc_s3 + ripclone" >&2
  )
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
  s3gc-build) s3gc_build ;;
  # Pass through any remaining args (e.g. a single test name for sharding).
  # Without this, `scripts/ci.sh s3gc some_test` ignored the name and every
  # "shard" re-ran the full suite (PR #126).
  s3gc) s3gc "${2:-}" ;;
  gitea) gitea ;;
  benchmark) benchmark ;;
  all) lint; run_tests; e2e ;;
  *) echo "usage: scripts/ci.sh [lint|test|e2e|flake|databases|s3gc-build|s3gc|gitea|benchmark|all]" >&2; exit 2 ;;
esac

echo "ci.sh: stage '$STAGE' OK"
