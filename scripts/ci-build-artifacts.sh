#!/usr/bin/env bash
# Compile once (ci profile) everything the PR gate fans out to separate jobs:
# bins for docker/e2e/benchmark, plus the integration/lib test binaries those
# jobs run. Stages stable names under rust/target/ci-artifacts/ for upload.
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
            and .executable != null) | .executable' \
    | tail -n1
}

pick_lib_test_exe() {
  jq -r \
    'select(.reason == "compiler-artifact"
            and .profile.test == true
            and .executable != null
            and (.target.kind | index("lib"))) | .executable' \
    | tail -n1
}

echo "==> building bins (profile=$PROFILE)"
cargo build --profile "$PROFILE" --locked --bins
for b in ripclone ripclone-server ripclone-worker git-remote-ripclone writer_bench; do
  src="$TARGET_ROOT/$PROFILE/$b"
  if [ ! -x "$src" ]; then
    echo "error: missing bin $src" >&2
    exit 1
  fi
  cp -f "$src" "$STAGE_DIR/$b"
  chmod +x "$STAGE_DIR/$b"
  echo "    staged $b"
done

TESTS=(
  e2e_gitea_provider
  e2e_worker_postgres
  e2e_worker_mysql
  e2e_metadata_postgres
  e2e_metadata_mysql
  e2e_worker_libsql
  e2e_remote_gc_s3
)

echo "==> building integration + lib test binaries (no-run)"
test_args=()
for t in "${TESTS[@]}"; do
  test_args+=(--test "$t")
done
# shellcheck disable=SC2068
json="$(cargo test --profile "$PROFILE" --locked --no-run --message-format=json \
  ${test_args[@]} --lib)"

for t in "${TESTS[@]}"; do
  exe="$(printf '%s\n' "$json" | pick_test_exe "$t")"
  if [ -z "$exe" ] || [ ! -x "$exe" ]; then
    echo "error: missing test binary $t" >&2
    exit 1
  fi
  cp -f "$exe" "$STAGE_DIR/$t"
  chmod +x "$STAGE_DIR/$t"
  echo "    staged $t"
done

lib_exe="$(printf '%s\n' "$json" | pick_lib_test_exe)"
if [ -z "$lib_exe" ] || [ ! -x "$lib_exe" ]; then
  echo "error: missing lib test binary" >&2
  exit 1
fi
cp -f "$lib_exe" "$STAGE_DIR/ripclone-lib-tests"
chmod +x "$STAGE_DIR/ripclone-lib-tests"
echo "    staged ripclone-lib-tests"

# Mirror into target/$PROFILE for local path conventions.
cp -f "$STAGE_DIR"/* "$TARGET_ROOT/$PROFILE/" 2>/dev/null || true

echo "==> ci-build-artifacts ready in $STAGE_DIR"
ls -la "$STAGE_DIR"
