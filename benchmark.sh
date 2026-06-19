#!/usr/bin/env bash
# Reproduce the clone-time benchmarks for oven-sh/bun.
# Requires: git, curl, tar, python3, time (/usr/bin/time on Linux)
set -euo pipefail

REPO_URL="https://github.com/oven-sh/bun.git"
BRANCH="main"
WORKDIR=$(mktemp -d)
echo "Benchmarking in $WORKDIR"

cd "$WORKDIR"

echo ""
echo "=== 1. GitHub tarball only (download + extract) ==="
rm -rf bun-tarball
mkdir bun-tarball && cd bun-tarball
time curl -fsSL -o bun.tar.gz "https://github.com/oven-sh/bun/archive/refs/heads/${BRANCH}.tar.gz"
time tar -xzf bun.tar.gz --strip-components=1
du -sh . ../bun-tarball 2>/dev/null | tail -1
cd ..

echo ""
echo "=== 2. Shallow git clone --depth=1 ==="
rm -rf bun-shallow
time git clone --depth=1 --single-branch --branch "$BRANCH" "$REPO_URL" bun-shallow
du -sh bun-shallow bun-shallow/.git

echo ""
echo "=== 3. lazygit.py clone ==="
rm -rf bun-lazygit
cd "$(dirname "$0")"
time python3 lazygit.py clone "$REPO_URL" --branch "$BRANCH" --dir "$WORKDIR/bun-lazygit"
cd "$WORKDIR/bun-lazygit"
du -sh . .git

echo ""
echo "=== Cleanup ==="
cd /
rm -rf "$WORKDIR"
echo "Done."
