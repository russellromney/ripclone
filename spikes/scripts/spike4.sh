#!/bin/bash
set -e
REPO="${1:-/tmp/bun.git}"
COUNT="${2:-50}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "Analyzing storage models for last $COUNT commits..."
COMMITS=$(git -C "$REPO" log --format=%H --first-parent -n "$COUNT" HEAD | tac)

ALL_OBJS="$TMPDIR/all_objs"
> "$ALL_OBJS"

FULL_SKEL_TOTAL=0
DELTA_SKEL_TOTAL=0
FULL_HEAD_TOTAL=0
PREV_SKEL=""
I=0

for COMMIT in $COMMITS; do
    I=$((I+1))
    git -C "$REPO" rev-list --objects --no-object-names "$COMMIT" > "$TMPDIR/all"
    git -C "$REPO" cat-file --batch-check='%(objectname) %(objecttype)' < "$TMPDIR/all" > "$TMPDIR/types"
    
    # Skeleton (commits+trees)
    awk '$2=="commit"||$2=="tree"{print $1}' "$TMPDIR/types" > "$TMPDIR/skel"
    git -C "$REPO" pack-objects --stdout < "$TMPDIR/skel" > "$TMPDIR/skel.pack" 2>/dev/null
    SZ=$(stat -f%z "$TMPDIR/skel.pack" 2>/dev/null || stat -c%s "$TMPDIR/skel.pack")
    FULL_SKEL_TOTAL=$((FULL_SKEL_TOTAL + SZ))
    
    # Full HEAD (all objects)
    awk '{print $1}' "$TMPDIR/types" > "$TMPDIR/head"
    git -C "$REPO" pack-objects --stdout < "$TMPDIR/head" > "$TMPDIR/head.pack" 2>/dev/null
    HSZ=$(stat -f%z "$TMPDIR/head.pack" 2>/dev/null || stat -c%s "$TMPDIR/head.pack")
    FULL_HEAD_TOTAL=$((FULL_HEAD_TOTAL + HSZ))
    
    # Delta skeleton vs previous
    if [ -n "$PREV_SKEL" ]; then
        comm -23 <(sort "$TMPDIR/skel") <(sort "$PREV_SKEL") > "$TMPDIR/delta"
        if [ -s "$TMPDIR/delta" ]; then
            git -C "$REPO" pack-objects --stdout < "$TMPDIR/delta" > "$TMPDIR/delta.pack" 2>/dev/null
            DSZ=$(stat -f%z "$TMPDIR/delta.pack" 2>/dev/null || stat -c%s "$TMPDIR/delta.pack")
            DELTA_SKEL_TOTAL=$((DELTA_SKEL_TOTAL + DSZ))
        fi
    else
        DELTA_SKEL_TOTAL=$((DELTA_SKEL_TOTAL + SZ))
    fi
    
    cat "$TMPDIR/skel" >> "$ALL_OBJS"
    cp "$TMPDIR/skel" "$TMPDIR/prev_skel"
    PREV_SKEL="$TMPDIR/prev_skel"
done

# Perfect sharing: pack all unique skeleton objects
UNIQUE="$TMPDIR/unique"
sort -u "$ALL_OBJS" > "$UNIQUE"
git -C "$REPO" pack-objects --stdout < "$UNIQUE" > "$TMPDIR/unique.pack" 2>/dev/null
PERFECT_SIZE=$(stat -f%z "$TMPDIR/unique.pack" 2>/dev/null || stat -c%s "$TMPDIR/unique.pack")
UNIQUE_COUNT=$(wc -l < "$UNIQUE")

echo ""
echo "Storage model comparison ($I commits):"
echo "  unique skeleton objects: $UNIQUE_COUNT"
printf "  (a) Perfect sharing:           %10d bytes (%6.2f MB)\n" "$PERFECT_SIZE" "$(echo "scale=2; $PERFECT_SIZE/1048576" | bc)"
printf "  (b) Full skeleton per commit:  %10d bytes (%6.2f MB, %.2fx)\n" "$FULL_SKEL_TOTAL" "$(echo "scale=2; $FULL_SKEL_TOTAL/1048576" | bc)" "$(echo "scale=2; $FULL_SKEL_TOTAL/$PERFECT_SIZE" | bc)"
printf "  (c) Delta skeleton per commit: %10d bytes (%6.2f MB, %.2fx)\n" "$DELTA_SKEL_TOTAL" "$(echo "scale=2; $DELTA_SKEL_TOTAL/1048576" | bc)" "$(echo "scale=2; $DELTA_SKEL_TOTAL/$PERFECT_SIZE" | bc)"
printf "  (d) Full working tree pack:    %10d bytes (%6.2f MB, %.2fx)\n" "$FULL_HEAD_TOTAL" "$(echo "scale=2; $FULL_HEAD_TOTAL/1048576" | bc)" "$(echo "scale=2; $FULL_HEAD_TOTAL/$PERFECT_SIZE" | bc)"
