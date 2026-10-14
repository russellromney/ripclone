#!/usr/bin/env bash
# Compile once (ci profile) everything the PR gate fans out to separate jobs:
# product bins (docker/e2e/benchmark) plus the contract test binary the s3 and
# gitea jobs run. Stages stable names under rust/target/ci-artifacts/ for upload.
#
# Not the full unit/integration suite — that is the `test` job's job. Keep this
# list to what fan-out actually executes.
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

BINS=(ripclone ripclone-server)

TESTS=(
  contract
)

echo "==> building bins (profile=$PROFILE)"
cargo build --profile "$PROFILE" --locked --bins
for b in "${BINS[@]}"; do
  src="$TARGET_ROOT/$PROFILE/$b"
  if [ ! -x "$src" ]; then
    echo "error: missing bin $src" >&2
    exit 1
  fi
  cp -f "$src" "$STAGE_DIR/$b"
  chmod +x "$STAGE_DIR/$b"
  echo "    staged bin $b"
done

echo "==> building fan-out test binaries (no-run)"
test_args=()
for t in "${TESTS[@]}"; do
  test_args+=(--test "$t")
done
# shellcheck disable=SC2068
json="$(cargo test --profile "$PROFILE" --locked --no-run --message-format=json \
  ${test_args[@]})"

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
