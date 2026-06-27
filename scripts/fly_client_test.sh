#!/bin/bash
set -uo pipefail

SERVER="${RIPCLONE_URL:-https://ripclone.fly.dev}"
REPO="${REPO:-oven-sh/bun}"

# Self-heal: install any runtime libs that are not baked into the image.
apt-get update -qq >/dev/null 2>&1 || true
apt-get install -y -qq libfuse2 libssl3t64 libgit2-1.7 time git ca-certificates coreutils >/dev/null 2>&1 || true

export PATH="/usr/local/bin:$PATH"
export RIPCLONE_URL="$SERVER"
export RUST_LOG="${RUST_LOG:-info}"

TOKEN_HASH=$(printf '%s' "${RIPCLONE_SERVER_TOKEN:-${RIPCLONE_TOKEN:-}}" | sha256sum | awk '{print $1}')

# Clean up any leftover overlay mounts from previous runs so rm -rf does not
# fail with EBUSY, and free tmpfs staging directories so each run starts with
# the full /dev/shm budget available.
cleanup_staging() {
  for d in /tmp/bun-install /tmp/bun-install-rootfs /tmp/bun-archive /tmp/bun-ripclone /tmp/bun-http /tmp/bun-github; do
    umount -l "$d" 2>/dev/null || true
  done
  rm -rf /dev/shm/ripclone-overlay-* 2>/dev/null || true
}
cleanup_staging

now_ms() { date +%s%N; }

run_clone() {
  local label=$1
  local dir=$2
  local env_vars=$3
  local cmd=$4
  local out_var=$5
  local cpu_var=$6

  rm -rf "$dir"
  local start end ms
  local time_file="/tmp/time.$$.${label// /_}"
  local log_file="/tmp/log.$$.${label// /_}"
  start=$(now_ms)
  # Capture the command's stderr in log_file and /usr/bin/time output in
  # time_file so we can inspect ripclone logs on success as well as failure.
  # Capture both stdout and stderr in log_file so tracing/ripclone logs are
  # preserved regardless of whether they go to stdout or stderr.
  if env $env_vars /usr/bin/time -p -o "$time_file" bash -c "$cmd" >"$log_file" 2>&1; then
    end=$(now_ms)
    ms=$(( (end - start) / 1000000 ))
    local cpu
    cpu=$(awk '/^user /{u=$2} /^sys /{s=$2} END{printf "%.3f", u+s}' "$time_file")
    local status_start status_end status_ms
    status_start=$(now_ms)
    (cd "$dir" && git status >/dev/null && git log --oneline -1 >/dev/null)
    status_end=$(now_ms)
    status_ms=$(( (status_end - status_start) / 1000000 ))
    echo "$label: ${ms}ms (CPU ${cpu}s, status ${status_ms}ms)"
    # Emit the command log for the extraction paths so we can see where time
    # is spent without having to re-run interactively.
    case "$label" in
      *archive*|*direct-install*)
        echo "----- $label log -----"
        cat "$log_file"
        echo "----- end $label log -----"
        ;;
    esac
    printf -v "$out_var" '%s' "$ms"
    printf -v "$cpu_var" '%s' "$cpu"
  else
    echo "$label: FAILED"
    cat "$time_file" "$time_file.cmd" "$log_file" || true
    printf -v "$out_var" '%s' '0'
    printf -v "$cpu_var" '%s' '0'
  fi
  # Free the overlay staging directories between runs. If overlay staging is
  # left in /dev/shm, later runs (especially archive extraction) may see too
  # little free tmpfs and fall back to the slow rootfs.
  cleanup_staging
  rm -f "$time_file" "$time_file.cmd" "$log_file"
}

# Show the filesystem speed gap explicitly.
echo "==> [0/6] filesystem sanity check"
echo -n "rootfs (/tmp):  "
dd if=/dev/zero of=/tmp/fsspeed bs=1M count=50 oflag=direct 2>&1 | tail -1
rm -f /tmp/fsspeed
echo -n "tmpfs (/dev/shm):  "
dd if=/dev/zero of=/dev/shm/fsspeed bs=1M count=50 oflag=direct 2>&1 | tail -1
rm -f /dev/shm/fsspeed

echo "==> [1/6] ripclone direct-install (overlay staging, default)"
run_clone \
  "ripclone direct-install (overlay)" \
  "/tmp/bun-install" \
  "" \
  "ripclone --server $SERVER clone $REPO --dir /tmp/bun-install" \
  install_overlay_ms \
  install_overlay_cpu

echo "==> [2/6] ripclone direct-install (overlay staging, archive extraction)"
run_clone \
  "ripclone archive-extraction (overlay)" \
  "/tmp/bun-archive" \
  "RIPCLONE_MODE=fast" \
  "ripclone --server $SERVER clone $REPO --dir /tmp/bun-archive" \
  install_archive_ms \
  install_archive_cpu

echo "==> [3/6] ripclone direct-install (no overlay, rootfs)"
run_clone \
  "ripclone direct-install (rootfs)" \
  "/tmp/bun-install-rootfs" \
  "RIPCLONE_NO_OVERLAY=1" \
  "ripclone --server $SERVER clone $REPO --dir /tmp/bun-install-rootfs" \
  install_rootfs_ms \
  install_rootfs_cpu

echo "==> [4/6] git-remote-ripclone clone"
run_clone \
  "git-remote-ripclone" \
  "/tmp/bun-ripclone" \
  "" \
  "git clone ripclone://${REPO}.git /tmp/bun-ripclone" \
  helper_ms \
  helper_cpu

echo "==> [5/6] smart-HTTP fallback clone"
run_clone \
  "smart-HTTP fallback" \
  "/tmp/bun-http" \
  "" \
  "git clone http://ripclone:${TOKEN_HASH}@${SERVER#*://}/v1/git/${REPO} /tmp/bun-http" \
  http_ms \
  http_cpu

echo "==> [6/6] baseline: git clone --depth 1 from GitHub"
rm -rf /tmp/bun-github
start=$(now_ms)
if git clone --depth 1 "https://github.com/${REPO}.git" /tmp/bun-github >/dev/null 2>&1; then
  end=$(now_ms)
  gh_ms=$(( (end - start) / 1000000 ))
  gh_size=$(du -sh /tmp/bun-github | cut -f1)
  (cd /tmp/bun-github && git status >/dev/null && git log --oneline -1 >/dev/null)
  echo "GitHub depth-1 clone: ${gh_ms}ms (size ${gh_size})"
else
  echo "GitHub depth-1 clone: FAILED"
  gh_ms=0
fi

echo ""
echo "=========================================================="
echo "Fly client clone timings ($REPO -> $SERVER)"
echo "  ripclone direct-install (overlay):   ${install_overlay_ms:-0}ms (CPU ${install_overlay_cpu:-0}s)"
echo "  ripclone archive-extraction (overlay): ${install_archive_ms:-0}ms (CPU ${install_archive_cpu:-0}s)"
echo "  ripclone direct-install (rootfs):    ${install_rootfs_ms:-0}ms (CPU ${install_rootfs_cpu:-0}s)"
echo "  git-remote-ripclone:                 ${helper_ms:-0}ms (CPU ${helper_cpu:-0}s)"
echo "  smart-HTTP fallback:                 ${http_ms:-0}ms (CPU ${http_cpu:-0}s)"
echo "  GitHub depth-1 baseline:             ${gh_ms:-0}ms"
echo "=========================================================="
