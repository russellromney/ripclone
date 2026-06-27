#!/usr/bin/env bash
set -euo pipefail

# ripclone vs native git clone sweep.
#
# By default the network link is shaped to the requested bandwidth using
# nftables (run on a Linux host with CAP_NET_ADMIN such as the Fly client).
# Set SHAPED=0 to run without traffic shaping for warm-cache comparisons.
#
# Usage (typically run on the Fly ripclone-client-dev machine):
#   RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
#   RIPCLONE_SERVER_TOKEN=... \
#   ./benchmark/run_shaped_sweep.sh [repos] [rates] [runs]
#
#   SHAPED=0 RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
#   RIPCLONE_SERVER_TOKEN=... \
#   ./benchmark/run_shaped_sweep.sh "oven-sh/bun pandas-dev/pandas" "1000" 3
#
#   BENCH_REF=v2.2.2 RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
#   RIPCLONE_SERVER_TOKEN=... \
#   ./benchmark/run_shaped_sweep.sh "pandas-dev/pandas" "1000" 1
#
# Environment:
#   SHAPED     - 1 (default) to shape bandwidth, 0 for unshaped warm-cache runs.
#   BENCH_REF  - tag/commit/branch to sync and benchmark. Use a tag for very
#                active repos where HEAD moves during the sweep.
#
# Defaults:
#   repos = "oven-sh/bun torvalds/linux"
#   rates = "1000 500 250 100 50"   (Mbps)
#   runs  = 3
#
# Results are appended to /data/shaped_sweep.log and per-run stderr is kept in
# /data/shaped_logs/<repo>/<rate>Mbps/.

REPOS="${1:-"oven-sh/bun torvalds/linux"}"
RATES="${2:-"1000 500 250 100 50"}"
RUNS="${3:-3}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$SCRIPT_DIR/fly_shaped_benchmark"

LOG="/data/shaped_sweep.log"
mkdir -p "$(dirname "$LOG")"

echo "===== sweep started at $(date -Iseconds) (SHAPED=${SHAPED:-1}) =====" | tee -a "$LOG"

for repo in $REPOS; do
  first_rate=1
  for rate in $RATES; do
    echo "" | tee -a "$LOG"
    echo "--- repo=$repo rate=${rate}Mbps ---" | tee -a "$LOG"
    if [ "$first_rate" = "1" ]; then
      first_rate=0
      sync_env=""
    else
      sync_env="SKIP_SYNC=1"
    fi
    if env $sync_env SHAPED="${SHAPED:-1}" BENCH_REF="${BENCH_REF:-}" "$BENCH" "$repo" "$rate" "$RUNS" 2>&1 | tee -a "$LOG"; then
      :
    else
      echo "ERROR: benchmark failed for $repo @ ${rate}Mbps" | tee -a "$LOG"
    fi
  done
done

echo "" | tee -a "$LOG"
echo "===== sweep finished at $(date -Iseconds) =====" | tee -a "$LOG"
echo "Per-run logs: /data/shaped_logs/"
