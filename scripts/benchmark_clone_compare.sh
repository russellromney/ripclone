#!/usr/bin/env bash
set -uo pipefail

# Repeatable clone benchmark: ripclone vs native git, depth=1 and full.
#
# Run this ON A CLIENT MACHINE (e.g. the Fly ripclone-client-dev box) with
# RIPCLONE_URL pointing at a ripclone server and a writable TARGET dir.
#
#   RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
#   REPO=oven-sh/bun TARGET=/data RIPCLONE_TOKEN=... \
#   bash scripts/benchmark_clone_compare.sh
#
# Methodology notes (see BENCHMARKS.md):
#   * Cache is force-disabled (RIPCLONE_NO_CACHE=1) so nothing is served from a
#     client-local artifact cache — every clone fetches from object storage.
#   * The server mirror is warmed first so `resolve` reflects steady state
#     (in production the server syncs on push, so the mirror is always fresh).
#   * Each clone goes to a fresh dir on $TARGET (a real disk volume by default).
#   * Native git clones from GitHub for an honest end-to-end comparison.

RIPCLONE="${RIPCLONE:-ripclone}"
RIPCLONE_URL="${RIPCLONE_URL:?set RIPCLONE_URL}"
REPO="${REPO:-oven-sh/bun}"
TARGET="${TARGET:-/data}"
RUNS="${RUNS:-3}"
export RIPCLONE_NO_CACHE=1

ms() { date +%s%3N; }
median() { sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}'; }

bench() { # label  <command that takes a fresh dir as last arg via $D>
  local label="$1"; shift
  local times=()
  for _ in $(seq 1 "$RUNS"); do
    D="$TARGET/bench.$$"; rm -rf "$D"
    local s e
    s=$(ms)
    if "$@" "$D" >/dev/null 2>&1; then e=$(ms); times+=( $((e-s)) ); else echo "$label: FAILED"; rm -rf "$D"; return; fi
    rm -rf "$D"
  done
  local med; med=$(printf '%s\n' "${times[@]}" | median)
  printf '%-26s median=%5dms   runs=[%s]\n' "$label" "$med" "$(IFS=,; echo "${times[*]}")"
}

rc_d1() { "$RIPCLONE" --server "$RIPCLONE_URL" clone "$REPO" --dir "$1" --depth 1; }
rc_d0() { "$RIPCLONE" --server "$RIPCLONE_URL" clone "$REPO" --dir "$1" --depth 0; }
rc_d1_tmp() { "$RIPCLONE" --server "$RIPCLONE_URL" clone "$REPO" --dir "$1" --depth 1 --temp; }
rc_d0_tmp() { "$RIPCLONE" --server "$RIPCLONE_URL" clone "$REPO" --dir "$1" --depth 0 --temp; }
git_d1() { git clone --depth 1 "https://github.com/$REPO" "$1"; }
git_d0() { git clone "https://github.com/$REPO" "$1"; }

echo "repo=$REPO  target=$TARGET  runs=$RUNS  host=$(hostname)  cpus=$(nproc 2>/dev/null || echo ?)"
echo "warming server mirror..."
"$RIPCLONE" --server "$RIPCLONE_URL" clone "$REPO" --dir "$TARGET/warm.$$" --depth 1 >/dev/null 2>&1
rm -rf "$TARGET/warm.$$"

echo "== ripclone (warm, cache-disabled) =="
bench "ripclone depth=1 (vol)"  rc_d1
bench "ripclone depth=1 (tmpfs)" rc_d1_tmp
bench "ripclone full    (vol)"  rc_d0
bench "ripclone full    (tmpfs)" rc_d0_tmp
echo "== native git (from GitHub) =="
bench "git clone --depth 1"     git_d1
bench "git clone full"          git_d0
