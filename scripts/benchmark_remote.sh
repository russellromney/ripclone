#!/usr/bin/env bash
set -euo pipefail

# Benchmark the direct-install clone path against a remote ripclone-server.
#
# Usage:
#   SERVER_URL=http://ripclone.fly.dev:8080 REPO=oven-sh/bun ITER=3 ./scripts/benchmark_remote.sh
#
# Environment:
#   SERVER_URL - URL of the ripclone-server (required)
#   REPO       - "owner/repo" to clone (default oven-sh/bun)
#   ITER       - number of clone runs to average (default 3)

REPO="${REPO:-oven-sh/bun}"
ITER="${ITER:-3}"
SERVER_URL="${SERVER_URL:-}"

if [ -z "$SERVER_URL" ]; then
  echo "error: SERVER_URL is required"
  echo "usage: SERVER_URL=http://... REPO=owner/repo ITER=3 ./scripts/benchmark_remote.sh"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="$ROOT_DIR/rust/target/release/ripclone"

if [ ! -x "$RIPCLONE" ]; then
  echo "error: missing binary $RIPCLONE (run cargo build --release in rust/)"
  exit 1
fi

if [ -z "${RIPCLONE_TOKEN:-}" ]; then
  echo "error: RIPCLONE_TOKEN must be set (server is fail-closed)"
  exit 1
fi
AUTH_HASH=$(printf '%s' "$RIPCLONE_TOKEN" | shasum -a 256 | awk '{print $1}')
CURL_AUTH=(-H "Authorization: Ripclone $AUTH_HASH")

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

BASE_DIR="$(mktemp -d /tmp/ripclone-remote-bench.XXXXXX)"

cleanup_overlay() {
  # Unmount any overlay targets under the temp dir and free tmpfs staging so
  # repeated runs do not fall back to the slow rootfs.
  if [ -d "$BASE_DIR" ]; then
    for d in "$BASE_DIR"/*/; do
      umount -l "$d" 2>/dev/null || true
    done
  fi
  rm -rf /dev/shm/ripclone-overlay-* 2>/dev/null || true
}

cleanup() {
  cleanup_overlay
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

echo "==> server:   $SERVER_URL"
echo "==> repo:     $REPO"
echo "==> data dir: $BASE_DIR"

for i in $(seq 1 30); do
  if curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then
  echo "error: server is not reachable at $SERVER_URL"
  exit 1
fi

echo ""
echo "==> Syncing mirror and building artifacts on server (one-time)..."
sync_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO"
sync_end=$(now_ms)
printf "sync=%d ms\n" $((sync_end - sync_start))

OWNER="$(echo "$REPO" | cut -d/ -f1)"
NAME="$(echo "$REPO" | cut -d/ -f2)"

# Report artifact sizes.
file_size() {
  if [ -f "$1" ]; then
    stat -f%z "$1" 2>/dev/null || stat -c%s "$1"
  else
    echo 0
  fi
}

ref_json=$(curl -fsS "${CURL_AUTH[@]}" "$SERVER_URL/v1/repos/$OWNER/$NAME/refs/HEAD")
clonepack_manifest_hash=$(echo "$ref_json" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("clonepack_manifest",""))')
if [ -z "$clonepack_manifest_hash" ]; then
  echo "error: server did not return a clonepack manifest"
  exit 1
fi

# Decode the clonepack manifest protobuf enough to report metadata/archive sizes.
clonepack_data=$(curl -fsSL "${CURL_AUTH[@]}" "$SERVER_URL/v1/artifacts/$clonepack_manifest_hash")
metadata_hash=$(echo "$clonepack_data" | python3 -c '
import sys
data = sys.stdin.buffer.read()
i = 0
def varint(d, i):
    x = s = 0
    while True:
        b = d[i]; i += 1
        x |= (b & 0x7f) << s
        if not b & 0x80: break
        s += 7
    return x, i
while i < len(data):
    tag, i = varint(data, i)
    field, wire = tag >> 3, tag & 7
    if wire == 2:
        length, i = varint(data, i)
        value = data[i:i+length]; i += length
    else:
        value = b""; i += 1
    if field == 4 and wire == 2:  # metadata_chunk ChunkRef
        # embedded message: hash (field 1, bytes), len (field 2, varint)
        j = 0
        while j < len(value):
            t, j = varint(value, j)
            f, w = t >> 3, t & 7
            if w == 2:
                l, j = varint(value, j)
                v = value[j:j+l]; j += l
            else:
                v = b""; j += 1
            if f == 1:
                print(v.hex())
')
archive_chunk_count=$(echo "$clonepack_data" | python3 -c '
import sys
data = sys.stdin.buffer.read()
count = 0
i = 0
def varint(d, i):
    x = s = 0
    while True:
        b = d[i]; i += 1
        x |= (b & 0x7f) << s
        if not b & 0x80: break
        s += 7
    return x, i
while i < len(data):
    tag, i = varint(data, i)
    field, wire = tag >> 3, tag & 7
    if wire == 2:
        length, i = varint(data, i)
        value = data[i:i+length]; i += length
    else:
        value = b""; i += 1
    if field == 5:
        count += 1
print(count)
')
metadata_size=$(curl -fsSL "${CURL_AUTH[@]}" "$SERVER_URL/v1/artifacts/$metadata_hash" | wc -c)

echo ""
echo "==> Ref metadata"
printf "  clonepack manifest: %s\n" "$clonepack_manifest_hash"
printf "  metadata chunk:     %s (%s bytes)\n" "$metadata_hash" "$metadata_size"
printf "  archive chunks:     %s\n" "$archive_chunk_count"

echo ""
echo "==> Direct-install clone ($ITER runs)..."
total=0
for n in $(seq 1 "$ITER"); do
  install_dir="$BASE_DIR/install-$n"
  install_start=$(now_ms)
  "$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$install_dir"
  install_end=$(now_ms)
  elapsed=$((install_end - install_start))
  total=$((total + elapsed))
  printf "  run %d: %d ms\n" "$n" "$elapsed"
done
avg=$((total / ITER))
printf "average install: %d ms\n" "$avg"

echo ""
echo "==> Verifying last clone..."
cd "$install_dir"
if [ -n "$(git status --short)" ]; then
  echo "error: git status not clean after install"
  git status --short
  exit 1
fi
if ! git diff --quiet HEAD; then
  echo "error: git diff reports changes"
  exit 1
fi
echo "verification OK"

echo ""
echo "=========================================================="
echo "Remote benchmark complete for $REPO."
echo "  server:          $SERVER_URL"
echo "  metadata chunk:  $metadata_size bytes"
echo "  archive chunks:  $archive_chunk_count"
echo "  avg install:     ${avg} ms"
echo "=========================================================="
