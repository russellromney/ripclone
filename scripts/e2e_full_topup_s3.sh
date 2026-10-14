#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUCKET="ripclone-topup-$$"
TEST_NAME="s3_interrupted_history_pack_expires_refreshes_exact_b_and_resumes"

: "${RIPCLONE_BIN_DIR:?RIPCLONE_BIN_DIR must name the release binary directory}"
test -x "$RIPCLONE_BIN_DIR/ripclone" || {
  echo "error: missing release CLI $RIPCLONE_BIN_DIR/ripclone" >&2
  exit 1
}
S3_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-topup-s3.XXXXXX")"
S3_EMULATOR_PID=""
cleanup() {
  if [ -n "$S3_EMULATOR_PID" ]; then kill "$S3_EMULATOR_PID" 2>/dev/null || true; fi
  rm -rf "$S3_DIR"
}
trap cleanup EXIT

s3_env="$(bash "$ROOT/scripts/s3_emulator.sh" "$S3_DIR" "$BUCKET")"
while IFS= read -r line; do export "$line"; done <<<"$s3_env"
export RIPCLONE_REQUIRE_S3=1

if [ -n "${CI_ARTIFACTS:-}" ]; then
  test_bin="$CI_ARTIFACTS/e2e_full_topup"
  test -x "$test_bin" || {
    echo "error: missing $test_bin" >&2
    exit 1
  }
  listed="$(timeout 60 "$test_bin" --ignored --list)"
  run=("$test_bin" --ignored --exact "$TEST_NAME" --nocapture)
else
  listed="$(cd "$ROOT/rust" && timeout 300 cargo test --profile ci --locked --test e2e_full_topup -- --ignored --list)"
  run=(cargo test --profile ci --locked --test e2e_full_topup -- --ignored --exact "$TEST_NAME" --nocapture)
fi
grep -Fqx "$TEST_NAME: test" <<<"$listed" || {
  echo "error: exact S3 test '$TEST_NAME' is missing" >&2
  exit 1
}

log="$(mktemp "${TMPDIR:-/tmp}/ripclone-topup-s3-log.XXXXXX")"
set +e
(cd "$ROOT/rust" && timeout 300 "${run[@]}") 2>&1 | tee "$log"
rc=${PIPESTATUS[0]}
set -e
test "$rc" -eq 0 || exit "$rc"
grep -Fq "running 1 test" "$log" || {
  echo "error: exact S3 filter ran zero or multiple tests" >&2
  exit 1
}
grep -Eq "test result: ok\. 1 passed; 0 failed;" "$log" || {
  echo "error: exact S3 proof did not report one passing test" >&2
  exit 1
}
if grep -Fq "SKIP" "$log"; then
  echo "error: S3 proof emitted SKIP" >&2
  exit 1
fi
if grep -Fq "background build failed" "$log" || grep -Fq "fatal:" "$log"; then
  echo "error: S3 proof logged an exact-result build failure" >&2
  exit 1
fi
rm -f "$log"
