#!/bin/bash
set -e
REPO="${1:-/tmp/bun.git}"
FILE_LIST="${2:-/tmp/agent-files.txt}"
CAS_DIR="${3:-/tmp/spike-cas}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

mkdir -p "$CAS_DIR"
COMMIT=$(git -C "$REPO" rev-parse HEAD)
echo "Simulating lazy clone for $COMMIT"

# Server: build skeleton pack and store in CAS
echo "Building skeleton pack..."
git -C "$REPO" rev-list --objects --no-object-names "$COMMIT" > "$TMPDIR/all"
git -C "$REPO" cat-file --batch-check='%(objectname) %(objecttype)' < "$TMPDIR/all" | awk '$2=="commit"||$2=="tree"{print $1}' > "$TMPDIR/skel"
git -C "$REPO" pack-objects --stdout < "$TMPDIR/skel" > "$TMPDIR/skel.pack" 2>/dev/null
SKEL_SIZE=$(stat -f%z "$TMPDIR/skel.pack" 2>/dev/null || stat -c%s "$TMPDIR/skel.pack")
SKEL_HASH=$(sha1sum "$TMPDIR/skel.pack" | awk '{print $1}')
mkdir -p "$CAS_DIR/${SKEL_HASH:0:2}"
cp "$TMPDIR/skel.pack" "$CAS_DIR/${SKEL_HASH:0:2}/$SKEL_HASH"
echo "Skeleton pack: $SKEL_SIZE bytes, hash $SKEL_HASH"

# Server: store only the blobs we know the agent will need
echo "Storing needed blobs in CAS..."
BLOB_COUNT=0
BLOB_BYTES=0
NEEDED_BLOBS="$TMPDIR/needed_blobs"
> "$NEEDED_BLOBS"
while read -r FPATH; do
    [ -z "$FPATH" ] && continue
    ENTRY=$(git -C "$REPO" ls-tree "$COMMIT" "$FPATH" 2>/dev/null || true)
    [ -z "$ENTRY" ] && continue
    SHA=$(echo "$ENTRY" | awk '{print $3}')
    echo "$SHA" >> "$NEEDED_BLOBS"
done < "$FILE_LIST"
sort -u "$NEEDED_BLOBS" > "$NEEDED_BLOBS.sorted"
mv "$NEEDED_BLOBS.sorted" "$NEEDED_BLOBS"

while read -r SHA; do
    [ -z "$SHA" ] && continue
    mkdir -p "$CAS_DIR/${SHA:0:2}"
    git -C "$REPO" cat-file -p "$SHA" > "$CAS_DIR/${SHA:0:2}/$SHA"
    BLOB_COUNT=$((BLOB_COUNT+1))
    SZ=$(stat -f%z "$CAS_DIR/${SHA:0:2}/$SHA" 2>/dev/null || stat -c%s "$CAS_DIR/${SHA:0:2}/$SHA")
    BLOB_BYTES=$((BLOB_BYTES+SZ))
done < "$NEEDED_BLOBS"
echo "Stored $BLOB_COUNT needed blobs ($(echo "scale=2; $BLOB_BYTES/1048576" | bc) MB)"

# Client: fetch skeleton pack
START=$(date +%s.%N)
cp "$CAS_DIR/${SKEL_HASH:0:2}/$SKEL_HASH" "$TMPDIR/client-skel.pack"
SKEL_FETCH_TIME=$(echo "$(date +%s.%N) - $START" | bc)

# Client: index skeleton pack (much faster than unpack-objects)
START=$(date +%s.%N)
git init -q "$TMPDIR/client"
mkdir -p "$TMPDIR/client/.git/objects/pack"
cp "$TMPDIR/client-skel.pack" "$TMPDIR/client/.git/objects/pack/skeleton.pack"
GIT_DIR="$TMPDIR/client/.git" git index-pack "$TMPDIR/client/.git/objects/pack/skeleton.pack" > /dev/null 2>&1
echo "$COMMIT" > "$TMPDIR/client/.git/HEAD"
SKEL_UNPACK_TIME=$(echo "$(date +%s.%N) - $START" | bc)

# Client: fetch blobs for file list and materialize
FETCHED=0
FETCHED_BYTES=0
MISSING=0
START=$(date +%s.%N)
while read -r FPATH; do
    [ -z "$FPATH" ] && continue
    ENTRY=$(git -C "$REPO" ls-tree "$COMMIT" "$FPATH" 2>/dev/null || true)
    if [ -z "$ENTRY" ]; then
        MISSING=$((MISSING+1))
        continue
    fi
    SHA=$(echo "$ENTRY" | awk '{print $3}')
    mkdir -p "$TMPDIR/client/$(dirname "$FPATH")"
    cp "$CAS_DIR/${SHA:0:2}/$SHA" "$TMPDIR/client/$FPATH"
    FETCHED=$((FETCHED+1))
    SZ=$(stat -f%z "$CAS_DIR/${SHA:0:2}/$SHA" 2>/dev/null || stat -c%s "$CAS_DIR/${SHA:0:2}/$SHA")
    FETCHED_BYTES=$((FETCHED_BYTES+SZ))
done < "$FILE_LIST"
BLOB_FETCH_TIME=$(echo "$(date +%s.%N) - $START" | bc)

TOTAL_TIME=$(echo "$SKEL_FETCH_TIME + $SKEL_UNPACK_TIME + $BLOB_FETCH_TIME" | bc)
TOTAL_BYTES=$((SKEL_SIZE + FETCHED_BYTES))

echo ""
echo "Client simulation:"
echo "  skeleton fetch: ${SKEL_SIZE} bytes in ${SKEL_FETCH_TIME}s"
echo "  skeleton unpack: ${SKEL_UNPACK_TIME}s"
echo "  blobs fetched: $FETCHED ($MISSING missing, ${FETCHED_BYTES} bytes) in ${BLOB_FETCH_TIME}s"
echo "  total bytes: $TOTAL_BYTES ($(echo "scale=2; $TOTAL_BYTES/1048576" | bc) MB)"
echo "  total time: ${TOTAL_TIME}s"

# Baseline: shallow git clone with blob:none
echo ""
echo "Baseline: git clone --depth=1 --filter=blob:none"
rm -rf /tmp/bun-shallow-baseline
START=$(date +%s.%N)
git clone --depth=1 --filter=blob:none --single-branch --branch "$(git -C "$REPO" rev-parse --abbrev-ref HEAD)" "file://$REPO" /tmp/bun-shallow-baseline 2>&1 | tail -1
BASELINE_TIME=$(echo "$(date +%s.%N) - $START" | bc)
echo "Baseline shallow clone (blob:none) took ${BASELINE_TIME}s"
rm -rf /tmp/bun-shallow-baseline
