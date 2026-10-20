#!/usr/bin/env bash
set -euo pipefail

# Unshaped clone race over the real network: ripclone against `git clone`
# from the upstream host, per repo and mode.
#
# Run it on a Linux client near the server, not on the server itself:
#   RIPCLONE_URL=https://your-ripclone-server.example.com \
#   RIPCLONE_SERVER_TOKEN=... \
#   ./benchmark/real_network.sh "sharkdp/fd pandas-dev/pandas" 3
#
# For each repo, shaped_benchmark.sh adds it, pins one commit and waits until
# Head, Files and Full are ready and the repo's refs are published. Then it
# times full, depth1 and files clones and git's full and depth-1 clones,
# round-robin, each in a fresh directory and HOME with the client cache off.
#
# Results go to $TARGET/real_network/<UTC time>/ (TARGET defaults to /data):
#   results.tsv  one row per sample
#   summary.txt  git vs ripclone medians and every run, per repo and mode
#   run.log      the harness output
#
# Environment:
#   RIPCLONE_SERVER_TOKEN - the server token; adding a repo needs it. Without
#                   it, the harness and the clones read the server anonymously.
#   RIPCLONE      - client binary; defaults to `ripclone` on PATH.
#   SKIP_ADD=1    - read-only on the server: pin upstream's tip and wait for the
#                   server to have it, without adding the repo or admitting
#                   builds. The repos must already be added.
#   MODES         - the modes to time; defaults to all five. To compare two
#                   server versions, time git once and only ripclone after:
#                   MODES="full depth1 files".

REPOS="${1:?usage: real_network.sh \"owner/repo ...\" [runs]}"
RUNS="${2:-3}"
TARGET="${TARGET:-/data}"
: "${RIPCLONE_URL:?set RIPCLONE_URL to the server under test}"
RIPCLONE="${RIPCLONE:-ripclone}"
MODES="${MODES:-full depth1 files git-full git-depth1}"
export RIPCLONE_URL RIPCLONE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$SCRIPT_DIR/shaped_benchmark.sh"

for command in curl git perl python3 "$RIPCLONE"; do
  command -v "$command" >/dev/null || {
    echo "error: required command is missing: $command" >&2
    exit 2
  }
done

OUT="$TARGET/real_network/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
RESULTS="$OUT/results.tsv"
printf 'repo\tcommit\tlabel\trun\tms\n' > "$RESULTS"
{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "server: $RIPCLONE_URL"
  echo "client: $(hostname) $(uname -sm) cpus=$(nproc 2>/dev/null || echo ?)"
  echo "ripclone: $("$RIPCLONE" --version 2>/dev/null || echo unknown)"
  echo "git: $(git --version)"
  echo "runs: $RUNS"
  echo "modes: $MODES"
} > "$OUT/header.txt"
cat "$OUT/header.txt"

failed=0
for repo in $REPOS; do
  # One pin per repo: the harness resolves it, so a BENCH_REF or sweep state
  # from the caller must not leak in.
  if ! env -u BENCH_REF -u GIT_REF -u SKIP_SYNC -u SKIP_GIT -u SKIP_RIPCLONE \
      -u RIPCLONE_RUNS -u NATIVE_RUNS \
      BENCH_RESULTS="$RESULTS" \
      BENCH_MODES="$MODES" \
      RIPCLONE_BENCH_READY_RESULTS="head full files" \
      SHAPED=0 \
      "$BENCH" "$repo" 0 "$RUNS" "$TARGET" 2>&1 | tee -a "$OUT/run.log"; then
    echo "error: benchmark failed for $repo" | tee -a "$OUT/run.log"
    failed=1
  fi
done

python3 - "$RESULTS" "$OUT/header.txt" > "$OUT/summary.txt" <<'PY'
import csv, statistics, sys

# Each ripclone mode against its closest git clone.
PAIRS = [
    ("full", "ripclone full (depth=0)", "git clone full"),
    ("depth1", "ripclone depth=1", "git clone --depth 1"),
    ("files", "ripclone files", "git clone --depth 1"),
]
runs = {}
repos = []
with open(sys.argv[1]) as handle:
    for row in csv.DictReader(handle, delimiter="\t"):
        if row["repo"] not in repos:
            repos.append(row["repo"])
        runs.setdefault((row["repo"], row["label"]), []).append(int(row["ms"]) / 1000)

print(open(sys.argv[2]).read().rstrip())
print()
print(f"{'repo':22} {'mode':7} {'git (s)':>8} {'ripclone (s)':>13} {'faster':>7}   runs (s)")
for repo in repos:
    for mode, rip_label, git_label in PAIRS:
        rip, git = runs.get((repo, rip_label), []), runs.get((repo, git_label), [])
        if not rip and not git:
            continue
        rip_median = statistics.median(rip) if rip else None
        git_median = statistics.median(git) if git else None
        ratio = f"{git_median / rip_median:6.1f}x" if rip_median and git_median else "      -"
        fmt = lambda value: f"{value:.2f}" if value is not None else "-"
        listed = lambda values: "[" + ", ".join(f"{v:.2f}" for v in values) + "]"
        print(f"{repo:22} {mode:7} {fmt(git_median):>8} {fmt(rip_median):>13} {ratio}"
              f"   git {listed(git)} ripclone {listed(rip)}")
PY
echo
cat "$OUT/summary.txt"
echo
echo "results: $OUT"
exit "$failed"
