#!/usr/bin/env bash
# Fly client/server benchmark for the gix migration.
#
# Runs from inside a Fly client machine.  Measures:
#   - cold sync   (server mirror must be cleared externally before run)
#   - delta sync  (immediate re-sync after cold sync)
#   - full clone  (editable, depth=0)
#   - depth-1 clone (editable, depth=1)
#   - files clone (worktree only, --mode files --depth=1)
#
# Env:
#   SERVER_URL    ripclone server URL (default https://ripclone-server-dev.fly.dev)
#   REPOS         space-separated "owner/repo" list
#   CLONE_ITER    iterations for each clone mode (default 3)
#   BASE_DIR      where clones are written (default /data/bench)
#   RUST_LOG      default error
#   SKIP_COLD     if set, skip cold/delta sync and only benchmark clones
#   RIPCLONE_NO_CACHE  default 1
set -uo pipefail

SERVER_URL="${SERVER_URL:-https://ripclone-server-dev.fly.dev}"
REPOS="${REPOS:-facebook/react oven-sh/bun}"
CLONE_ITER="${CLONE_ITER:-3}"
BASE_DIR="${BASE_DIR:-/data/bench}"
RUST_LOG="${RUST_LOG:-error}"
RIPCLONE_NO_CACHE="${RIPCLONE_NO_CACHE:-1}"

export RUST_LOG RIPCLONE_NO_CACHE

mkdir -p "$BASE_DIR"

now_ns() { date +%s%N; }

# Run a command quietly and print elapsed milliseconds, or "FAIL".
measure() {
  local label=$1
  shift
  local start end elapsed
  start=$(now_ns)
  if "$@" >/dev/null 2>&1; then
    end=$(now_ns)
    elapsed=$(( (end - start) / 1000000 ))
    echo "$elapsed"
  else
    echo "FAIL"
  fi
}

# Compute min and average from a list of integer milliseconds.
# Usage: stats $iter "${times[@]}"
stats() {
  local n=$1 min=9999999 max=0 sum=0 t
  shift
  for t in "$@"; do
    if [ "$t" = "FAIL" ]; then continue; fi
    if [ "$t" -lt "$min" ]; then min=$t; fi
    if [ "$t" -gt "$max" ]; then max=$t; fi
    sum=$((sum + t))
  done
  if [ "$min" -eq 9999999 ]; then
    echo "FAIL FAIL FAIL"
  else
    local avg=$((sum / n))
    echo "$avg $min $max"
  fi
}

printf "server=%s\n" "$SERVER_URL"
printf "clone_iterations=%s\n" "$CLONE_ITER"
printf "repo operation avg_ms min_ms max_ms\n"

for repo in $REPOS; do
  name=${repo//\//_}
  printf "\n# repo: %s\n" "$repo"

  # Sync phases --------------------------------------------------------------
  if [ -z "${SKIP_COLD:-}" ]; then
    cold_ms=$(measure "cold sync" ripclone --server "$SERVER_URL" sync "$repo")
    printf "%s cold-sync %s - -\n" "$repo" "$cold_ms"

    delta_ms=$(measure "delta sync" ripclone --server "$SERVER_URL" sync "$repo")
    printf "%s delta-sync %s - -\n" "$repo" "$delta_ms"
  else
    printf "# SKIP_COLD set; not measuring sync\n"
  fi

  # Clone phases -------------------------------------------------------------
  mkdir -p "$BASE_DIR/$name"
  for mode in full d1 files; do
    dir="$BASE_DIR/$name/$mode"
    times=()
    for i in $(seq 1 "$CLONE_ITER"); do
      rm -rf "$dir"
      case "$mode" in
        full)
          t=$(measure "full clone $i" \
            ripclone --server "$SERVER_URL" clone "$repo" --dir "$dir" --depth 0 --mode editable)
          ;;
        d1)
          t=$(measure "depth-1 clone $i" \
            ripclone --server "$SERVER_URL" clone "$repo" --dir "$dir" --depth 1 --mode editable)
          ;;
        files)
          t=$(measure "files clone $i" \
            ripclone --server "$SERVER_URL" clone "$repo" --dir "$dir" --depth 1 --mode files)
          ;;
      esac
      times+=("$t")

      # Light verification.
      if [ "$mode" != "files" ] && [ -d "$dir/.git" ]; then
        git -C "$dir" status --short >/dev/null 2>&1 || true
      fi
    done
    read -r avg min max <<< "$(stats "$CLONE_ITER" "${times[@]}")"
    printf "%s %s %s %s %s\n" "$repo" "$mode" "$avg" "$min" "$max"
  done
done

printf "\n# benchmark complete\n"
