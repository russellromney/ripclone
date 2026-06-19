#!/bin/bash
set -e
REPO="${1:-/tmp/bun.git}"
FILE_LIST="${2:-/tmp/agent-files.txt}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

COMMIT=$(git -C "$REPO" rev-parse HEAD)
echo "Materializing files from file list for $COMMIT"

# Build skeleton pack and index it
git -C "$REPO" rev-list --objects --no-object-names "$COMMIT" > "$TMPDIR/all"
git -C "$REPO" cat-file --batch-check='%(objectname) %(objecttype)' < "$TMPDIR/all" | awk '$2=="commit"||$2=="tree"{print $1}' > "$TMPDIR/skel"
git -C "$REPO" pack-objects --stdout < "$TMPDIR/skel" > "$TMPDIR/skel.pack" 2>/dev/null
git init -q "$TMPDIR/client"
mkdir -p "$TMPDIR/client/.git/objects/pack"
cp "$TMPDIR/skel.pack" "$TMPDIR/client/.git/objects/pack/skeleton.pack"
git -C "$TMPDIR/client" index-pack "$TMPDIR/client/.git/objects/pack/skeleton.pack" > /dev/null 2>&1
echo "$COMMIT" > "$TMPDIR/client/.git/HEAD"

# Materialize files
START=$(date +%s.%N)
FETCHED=0
FETCHED_BYTES=0
MISSING=0
while read -r FPATH; do
    [ -z "$FPATH" ] && continue
    ENTRY=$(git -C "$REPO" ls-tree "$COMMIT" "$FPATH" 2>/dev/null || true)
    [ -z "$ENTRY" ] && continue
    SHA=$(echo "$ENTRY" | awk '{print $3}')
    mkdir -p "$TMPDIR/client/$(dirname "$FPATH")"
    git -C "$REPO" cat-file -p "$SHA" > "$TMPDIR/client/$FPATH"
    FETCHED=$((FETCHED+1))
    SZ=$(stat -f%z "$TMPDIR/client/$FPATH" 2>/dev/null || stat -c%s "$TMPDIR/client/$FPATH")
    FETCHED_BYTES=$((FETCHED_BYTES+SZ))
done < "$FILE_LIST"
MAT_TIME=$(echo "$(date +%s.%N) - $START" | bc)

echo "Materialized $FETCHED files ($(echo "scale=2; $FETCHED_BYTES/1048576" | bc) MB) in ${MAT_TIME}s"

# Compare to git checkout of same files from shallow clone
rm -rf /tmp/bun-shallow-checkout
START=$(date +%s.%N)
git clone --depth=1 --single-branch --branch "$(git -C "$REPO" rev-parse --abbrev-ref HEAD)" "file://$REPO" /tmp/bun-shallow-checkout 2>&1 | tail -1
CHECKOUT_TIME=$(echo "$(date +%s.%N) - $START" | bc)
echo "Full git shallow clone (all files) took ${CHECKOUT_TIME}s"
rm -rf /tmp/bun-shallow-checkout
