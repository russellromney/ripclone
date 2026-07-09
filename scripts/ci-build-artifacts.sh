#!/usr/bin/env bash
# Compile once (ci profile) everything the PR gate fans out to separate jobs:
# product bins (docker/e2e/benchmark) plus the integration test binaries those
# jobs run. Stages stable names under rust/target/ci-artifacts/ for upload.
#
# Intentionally NOT building the full unit/integration suite here — that is
# ~50 separate linked test binaries and made ci-build ~30m cold. The `test` job
# compiles that suite itself (with sccache / rust-cache).
#
# Usage: scripts/ci-build-artifacts.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${CARGO_PROFILE:-ci}"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/rust/target}"
STAGE_DIR="$ROOT/rust/target/ci-artifacts"
mkdir -p "$STAGE_DIR" "$TARGET_ROOT/$PROFILE"

cd "$ROOT/rust"

pick_test_exe() {
  local name="$1"
  jq -r --arg name "$name" \
    'select(.reason == "compiler-artifact"
            and .target.kind == ["test"]
            and .target.name == $name
            and .executable != null
            and (.executable | length) > 0) | .executable' \
    | tail -n1
}

# Product bins fan-out needs (not writer_bench — internal microbench only).
BINS=(ripclone ripclone-server ripclone-worker git-remote-ripclone)

# Integration tests that other CI jobs run as prebuilt binaries.
TESTS=(
  e2e_gitea_provider
  e2e_worker_postgres
  e2e_worker_mysql
  e2e_metadata_postgres
  e2e_metadata_mysql
  e2e_worker_libsql
  e2e_remote_gc_s3
)

echo "==> building bins + fan-out tests (profile=$PROFILE, one cargo inv)"
test_args=()
for t in "${TESTS[@]}"; do
  test_args+=(--test "$t")
done

# One invocation: builds the named integration tests and, as a dependency for
# CARGO_BIN_EXE_*, the product bins. message-format=json to locate executables.
# shellcheck disable=SC2068
json="$(cargo test --profile "$PROFILE" --locked --no-run --message-format=json \
  ${test_args[@]})"

for b in "${BINS[@]}"; do
  src="$TARGET_ROOT/$PROFILE/$b"
  if [ ! -x "$src" ]; then
    src="$(printf '%s\n' "$json" | jq -r --arg name "$b" '
      select(.reason == "compiler-artifact"
             and .executable != null
             and (.executable | length) > 0
             and .profile.test != true
             and .target.name == $name
             and (.target.kind | index("bin")))
      | .executable' | tail -n1)"
  fi
  if [ -z "$src" ] || [ ! -x "$src" ]; then
    echo "error: missing bin $b" >&2
    exit 1
  fi
  cp -f "$src" "$STAGE_DIR/$b"
  chmod +x "$STAGE_DIR/$b"
  echo "    staged bin $b"
done

for t in "${TESTS[@]}"; do
  exe="$(printf '%s\n' "$json" | pick_test_exe "$t")"
  if [ -z "$exe" ] || [ ! -x "$exe" ]; then
    echo "error: missing test binary $t (exe=${exe:-empty})" >&2
    exit 1
  fi
  cp -f "$exe" "$STAGE_DIR/$t"
  chmod +x "$STAGE_DIR/$t"
  echo "    staged test $t"
done

echo "==> ci-build-artifacts ready in $STAGE_DIR"
ls -la "$STAGE_DIR"
