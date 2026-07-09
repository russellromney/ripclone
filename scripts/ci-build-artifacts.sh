#!/usr/bin/env bash
# Compile once (ci profile) everything the PR gate fans out:
#   - product bins (e2e/docker/benchmark/worker spawns)
#   - every test binary cargo would build (`--all-targets --no-run`)
#
# One cargo invocation: bins + lib tests + integration tests + bin unit tests
# share a single compile graph. Stages stable names under
# rust/target/ci-artifacts/ for upload; fan-out jobs never invoke cargo.
#
# Usage: scripts/ci-build-artifacts.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${CARGO_PROFILE:-ci}"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/rust/target}"
STAGE_DIR="$ROOT/rust/target/ci-artifacts"
mkdir -p "$STAGE_DIR" "$TARGET_ROOT/$PROFILE"

cd "$ROOT/rust"

# Product bins fan-out needs (not writer_bench — internal microbench only).
BINS=(ripclone ripclone-server ripclone-worker git-remote-ripclone)

echo "==> building all bins + test binaries (profile=$PROFILE, one cargo inv)"
# --bins is covered by --all-targets; listing both keeps intent clear.
# message-format=json: discover every produced executable without parsing paths.
json="$(cargo test --profile "$PROFILE" --locked --no-run --message-format=json --all-targets)"

# Non-test product bins live at target/$PROFILE/<name> after the test build
# (cargo builds them for CARGO_BIN_EXE_* / normal bin artifacts).
for b in "${BINS[@]}"; do
  src="$TARGET_ROOT/$PROFILE/$b"
  if [ ! -x "$src" ]; then
    # Fallback: pick the non-test compiler-artifact for this bin name.
    src="$(printf '%s\n' "$json" | jq -r --arg name "$b" '
      select(.reason == "compiler-artifact"
             and .executable != null
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

# Stable stage names for test executables:
#   integration (kind=test) → <name>          (e2e_auth, …)
#   lib unit tests          → lib-tests
#   bin unit tests          → bin-<name>-tests (avoids clashing with product bins)
: >"$STAGE_DIR/test-executables.txt"
while IFS=$'\t' read -r stage_name exe; do
  [ -n "$stage_name" ] || continue
  [ -x "$exe" ] || {
    echo "error: test exe not executable: $exe" >&2
    exit 1
  }
  cp -f "$exe" "$STAGE_DIR/$stage_name"
  chmod +x "$STAGE_DIR/$stage_name"
  echo "$stage_name" >>"$STAGE_DIR/test-executables.txt"
  echo "    staged test $stage_name"
done < <(printf '%s\n' "$json" | jq -r '
  select(.reason == "compiler-artifact"
         and .executable != null
         and .profile.test == true) |
  [
    (if (.target.kind | index("lib")) then "lib-tests"
     elif (.target.kind | index("test")) then .target.name
     elif (.target.kind | index("bin")) then ("bin-" + .target.name + "-tests")
     else empty end),
    .executable
  ] | @tsv
')

# Deduplicate the manifest (cargo can emit the same artifact twice).
sort -u "$STAGE_DIR/test-executables.txt" -o "$STAGE_DIR/test-executables.txt"

n_tests="$(wc -l <"$STAGE_DIR/test-executables.txt" | tr -d ' ')"
if [ "$n_tests" -lt 1 ]; then
  echo "error: no test executables staged" >&2
  exit 1
fi

# Back-compat aliases used by older workflow env / muscle memory.
if [ -x "$STAGE_DIR/lib-tests" ]; then
  cp -f "$STAGE_DIR/lib-tests" "$STAGE_DIR/ripclone-lib-tests"
fi

echo "==> ci-build-artifacts ready in $STAGE_DIR ($n_tests test binaries + ${#BINS[@]} bins)"
ls -la "$STAGE_DIR"
