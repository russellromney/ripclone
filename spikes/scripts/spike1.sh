#!/bin/bash
set -e
REPO="${1:-/tmp/bun.git}"
COUNT="${2:-50}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "Analyzing skeletons for last $COUNT commits..."
COMMITS=$(git -C "$REPO" log --format=%H --first-parent -n "$COUNT" HEAD)

TOTAL_SIZE=0
TOTAL_OBJS=0
TOTAL_UNIQUE=0
ALL_OBJS_FILE="$TMPDIR/all_objs"
PREV_OBJS=""
I=0

for COMMIT in $COMMITS; do
    I=$((I+1))
    # List commit+tree shas
    git -C "$REPO" rev-list --objects --no-object-names "$COMMIT" > "$TMPDIR/all"
    git -C "$REPO" cat-file --batch-check='%(objectname) %(objecttype)' < "$TMPDIR/all" | awk '$2=="commit"||$2=="tree"{print $1}' > "$TMPDIR/skel"
    
    # Build pack
    git -C "$REPO" pack-objects --stdout < "$TMPDIR/skel" > "$TMPDIR/skel.pack" 2>/dev/null
    SIZE=$(stat -f%z "$TMPDIR/skel.pack" 2>/dev/null || stat -c%s "$TMPDIR/skel.pack")
    OBJS=$(wc -l < "$TMPDIR/skel")
    
    TOTAL_SIZE=$((TOTAL_SIZE + SIZE))
    TOTAL_OBJS=$((TOTAL_OBJS + OBJS))
    cat "$TMPDIR/skel" >> "$ALL_OBJS_FILE"
    
    NEW_VS_PARENT=0
    if [ -n "$PREV_OBJS" ]; then
        NEW_VS_PARENT=$(comm -23 <(sort "$TMPDIR/skel") <(sort "$PREV_OBJS") | wc -l)
    fi
    
    printf "commit %3d/%s %.7s: pack=%7d bytes, %6d objects, new-vs-parent=%6d\n" "$I" "$COUNT" "$COMMIT" "$SIZE" "$OBJS" "$NEW_VS_PARENT"
    
    cp "$TMPDIR/skel" "$TMPDIR/prev_skel"
    PREV_OBJS="$TMPDIR/prev_skel"
done

UNIQUE=$(sort -u "$ALL_OBJS_FILE" | wc -l)
UNIQUE_PACK=$(git -C "$REPO" pack-objects --stdout < <(sort -u "$ALL_OBJS_FILE") 2>/dev/null | wc -c)

echo ""
echo "Summary:"
echo "  commits analyzed: $I"
echo "  total unique objects across skeletons: $UNIQUE"
echo "  sum of per-commit skeleton sizes: $TOTAL_SIZE bytes ($(echo "scale=2; $TOTAL_SIZE/1048576" | bc) MB)"
echo "  sum of per-commit object counts: $TOTAL_OBJS"
echo "  average skeleton size: $(echo "scale=2; $TOTAL_SIZE/$I/1024" | bc) KB"
echo "  unique-objects pack size: $UNIQUE_PACK bytes ($(echo "scale=2; $UNIQUE_PACK/1048576" | bc) MB)"
echo "  duplication overhead: $(echo "scale=2; $TOTAL_SIZE/$UNIQUE_PACK" | bc)x"
