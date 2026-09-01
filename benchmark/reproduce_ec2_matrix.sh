#!/usr/bin/env bash
set -euo pipefail

# Reproduce the realistic two-host clone matrix recorded for the cold-history
# pack work. Despite the name, this is cloud-neutral: run it on a dedicated
# Linux client with CAP_NET_ADMIN, pointed at a separate ripclone server.
# Admission and readiness are performed by shaped_benchmark.sh before every
# sample set and are excluded from clone timing.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$SCRIPT_DIR/shaped_benchmark.sh"
TARGET="${TARGET:-/data}"
LOG="${MATRIX_LOG:-$TARGET/reproducible_ec2_matrix.log}"

SMALL_RUNS="${SMALL_RUNS:-10}"
LINUX_RUNS="${LINUX_RUNS:-10}"
LINUX_NATIVE_RUNS="${LINUX_NATIVE_RUNS:-1}"

BUN_REF="${BUN_REF:-43afad2dd4b20fdef6bed1e2c2b43c586faeba83}"
PANDAS_REF="${PANDAS_REF:-518f2a3cb9504555b40c1d5aaab4690245a7d265}"
REACT_REF="${REACT_REF:-eb8feb71096eec5c885b2a4c7d8d030d3622f265}"
LINUX_REF="${LINUX_REF:-dac3e89a2c90c2feeb471e1f22a2512ad424b792}"

: "${RIPCLONE_URL:?set RIPCLONE_URL to the separate server}"
: "${RIPCLONE_SERVER_TOKEN:?set RIPCLONE_SERVER_TOKEN to the server token}"
: "${RIPCLONE:?set RIPCLONE to the release client binary}"
test -x "$RIPCLONE" || { echo "error: RIPCLONE is not executable: $RIPCLONE" >&2; exit 2; }

for command in curl git nft perl python3 tar; do
  command -v "$command" >/dev/null || {
    echo "error: required command is missing: $command" >&2
    exit 2
  }
done
if ! nft list tables >/dev/null 2>&1; then
  echo "error: matrix requires root or CAP_NET_ADMIN for nftables shaping" >&2
  exit 2
fi

mkdir -p "$TARGET" "$(dirname "$LOG")"

run_case() {
  local repo="$1" ref="$2" rate="$3" shaped="$4" modes="$5"
  local ripclone_runs="$6" native_runs="$7"
  echo "=== reproducible case repo=$repo ref=$ref rate=$rate shaped=$shaped ===" | tee -a "$LOG"
  BENCH_REF="$ref" \
  BENCH_MODES="$modes" \
  RIPCLONE_RUNS="$ripclone_runs" \
  NATIVE_RUNS="$native_runs" \
  RIPCLONE_BENCH_READY_RESULTS="head full files" \
  VERIFY_EVERY_RUN="${VERIFY_EVERY_RUN:-0}" \
  SHAPED="$shaped" \
    "$BENCH" "$repo" "$rate" "$ripclone_runs" "$TARGET" 2>&1 | tee -a "$LOG"
}

all_modes="full depth1 files git-full git-depth1 github-files"
ripclone_modes="full depth1 files"

run_case oven-sh/bun "$BUN_REF" 0 0 "$all_modes" "$SMALL_RUNS" "$SMALL_RUNS"
run_case pandas-dev/pandas "$PANDAS_REF" 0 0 "$all_modes" "$SMALL_RUNS" "$SMALL_RUNS"
run_case facebook/react "$REACT_REF" 0 0 "$all_modes" "$SMALL_RUNS" "$SMALL_RUNS"

# Linux gets ten Ripclone samples and one native/archive control by default;
# native Full takes roughly five minutes even on the tested c6in.8xlarge.
run_case torvalds/linux "$LINUX_REF" 0 0 "$all_modes" "$LINUX_RUNS" "$LINUX_NATIVE_RUNS"

# The accepted Linux native control is not repeated at every shaped rate.
SKIP_GIT=1 run_case torvalds/linux "$LINUX_REF" 1000 1 "$ripclone_modes" "$LINUX_RUNS" 1
SKIP_GIT=1 run_case torvalds/linux "$LINUX_REF" 5000 1 "$ripclone_modes" "$LINUX_RUNS" 1

echo "matrix complete: $LOG"
