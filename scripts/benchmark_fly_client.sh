#!/usr/bin/env bash
set -euo pipefail

SERVER_URL="${SERVER_URL:-http://157.230.7.238:8080}"
TOKEN="${RIPCLONE_TOKEN:-}"

if [ -z "$TOKEN" ]; then
  echo "error: RIPCLONE_TOKEN not set"
  exit 1
fi
export RIPCLONE_TOKEN="$TOKEN"

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

printf "%-25s %-20s %10s %10s\n" "repo" "mode" "ms" ".git size"

for repo in oven-sh/bun pandas-dev/pandas openclaw/openclaw; do
  name=$(basename "$repo")

  # ripclone editable clone
  rm -rf "/tmp/fly-${name}-ripclone"
  start=$(now_ms)
  ripclone --server "$SERVER_URL" clone "$repo" --dir "/tmp/fly-${name}-ripclone" >/dev/null 2>&1
  end=$(now_ms)
  size=$(du -s "/tmp/fly-${name}-ripclone/.git" 2>/dev/null | awk '{print $1}')
  printf "%-25s %-20s %10s %10s\n" "$repo" "ripclone" "$((end - start))" "${size}K"

  # verify
  cd "/tmp/fly-${name}-ripclone"
  git show HEAD:README.md >/dev/null 2>&1 || true
  git status --short >/dev/null 2>&1 || true
  cd - >/dev/null

  # git clone --depth=1
  rm -rf "/tmp/fly-${name}-git-d1"
  start=$(now_ms)
  git clone --depth=1 "https://github.com/${repo}.git" "/tmp/fly-${name}-git-d1" >/dev/null 2>&1
  end=$(now_ms)
  size=$(du -s "/tmp/fly-${name}-git-d1/.git" 2>/dev/null | awk '{print $1}')
  printf "%-25s %-20s %10s %10s\n" "$repo" "git clone --depth=1" "$((end - start))" "${size}K"

  rm -rf "/tmp/fly-${name}-ripclone" "/tmp/fly-${name}-git-d1"
done
