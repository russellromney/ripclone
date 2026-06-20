#!/usr/bin/env bash
set -euo pipefail

# End-to-end clonepack round-trip test.
# Builds a clonepack for a small public fixture repo on a local server, then
# clones it with all supported modes (full, fast, hybrid, skeleton).

REPO="${REPO:-octocat/Hello-World}"
OWNER="${REPO%%/*}"
NAME="${REPO#*/}"
SYNC_DEPTH="${SYNC_DEPTH:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RIPCLONE="$ROOT_DIR/rust/target/release/ripclone"
SERVER="$ROOT_DIR/rust/target/release/ripclone-server"

for bin in "$RIPCLONE" "$SERVER"; do
  if [ ! -x "$bin" ]; then
    echo "error: missing binary $bin (run cargo build --release in rust/)"
    exit 1
  fi
done

if [ -z "${RIPCLONE_TOKEN:-}" ]; then
  echo "error: RIPCLONE_TOKEN must be set (server is fail-closed)"
  exit 1
fi
AUTH_HASH=$(printf '%s' "$RIPCLONE_TOKEN" | shasum -a 256 | awk '{print $1}')
CURL_AUTH=(-H "Authorization: Ripclone $AUTH_HASH")

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

PORT=$(( 10000 + RANDOM % 50000 ))
SERVER_URL="http://127.0.0.1:$PORT"
BASE_DIR="$(mktemp -d /tmp/ripclone-clonepack-e2e.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"

cleanup_overlay() {
  for d in "$BASE_DIR"/*/; do
    umount -l "$d" 2>/dev/null || true
  done
  rm -rf /dev/shm/ripclone-overlay-* 2>/dev/null || true
}

cleanup() {
  cleanup_overlay
  if [ -n "${SERVER_PID:-}" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

start_server() {
  "$SERVER" \
    --cas-dir "$CAS_DIR" \
    --repo-root "$REPO_ROOT" \
    --host 127.0.0.1 \
    --port "$PORT" \
    > "$BASE_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  for i in $(seq 1 30); do
    if curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then break; fi
    sleep 1
  done
  if ! curl -fsS "$SERVER_URL/healthz" >/dev/null 2>&1; then
    echo "error: server failed to start"
    cat "$BASE_DIR/server.log"
    exit 1
  fi
}

echo "==> Starting server..."
start_server

echo ""
echo "==> Syncing $REPO (one-time)..."
sync_start=$(now_ms)
if [ -n "$SYNC_DEPTH" ]; then
  "$RIPCLONE" --server "$SERVER_URL" sync "$REPO" --depth "$SYNC_DEPTH"
else
  "$RIPCLONE" --server "$SERVER_URL" sync "$REPO"
fi
sync_end=$(now_ms)
printf "sync took %d ms\n" $((sync_end - sync_start))

echo ""
echo "==> Fetching ref response..."
ref_json=$(curl -fsS "${CURL_AUTH[@]}" "$SERVER_URL/v1/repos/$OWNER/$NAME/refs/HEAD")
clonepack_manifest_hash=$(echo "$ref_json" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("clonepack_manifest",""))')
if [ -z "$clonepack_manifest_hash" ]; then
  echo "error: ref response missing clonepack_manifest"
  exit 1
fi
echo "clonepack manifest: $clonepack_manifest_hash"

# Parse the protobuf clonepack manifest to discover metadata chunk and archive chunks.
echo ""
echo "==> Decoding clonepack manifest..."
cat > "$BASE_DIR/parse_clonepack.py" <<'PY'
import sys

cpm = sys.stdin.buffer.read()

def varint(d, i):
    x = s = 0
    while True:
        b = d[i]; i += 1
        x |= (b & 0x7f) << s
        if not b & 0x80: break
        s += 7
    return x, i

def parse(data):
    i = 0
    out = {}
    while i < len(data):
        tag, i = varint(data, i)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            val, i = varint(data, i)
        elif wire == 2:
            length, i = varint(data, i)
            val = data[i:i+length]; i += length
        else:
            val = None; i += 1
        out.setdefault(field, []).append(val)
    return out

cpm_parsed = parse(cpm)
assert set(cpm_parsed.keys()) <= {1,2,3,4,5,7,8}, f'unexpected fields: {cpm_parsed.keys()}'
commit = cpm_parsed.get(1, [b''])[0].decode()
branch = cpm_parsed.get(3, [b''])[0].decode()
print(f'commit: {commit}')
print(f'default_branch: {branch}')

meta_ref = parse(cpm_parsed[4][0])
meta_hash = meta_ref[1][0].hex()
meta_len = meta_ref[2][0]
print(f'metadata_chunk: {meta_hash} ({meta_len} bytes)')

for i, ref in enumerate(cpm_parsed.get(5, [])):
    arc = parse(ref)
    arc_hash = arc[1][0].hex()
    arc_len = arc[2][0]
    print(f'archive_chunk[{i}]: {arc_hash} ({arc_len} bytes)')

for i, ref in enumerate(cpm_parsed.get(8, [])):
    hb = parse(ref)
    hb_hash = hb[1][0].hex()
    hb_len = hb[2][0]
    print(f'head_blobs_chunk[{i}]: {hb_hash} ({hb_len} bytes)')
PY
curl -fsS "${CURL_AUTH[@]}" "$SERVER_URL/v1/artifacts/$clonepack_manifest_hash" | python3 "$BASE_DIR/parse_clonepack.py" | tee "$BASE_DIR/clonepack.txt"

metadata_hash=$(awk '/^metadata_chunk:/{print $2}' "$BASE_DIR/clonepack.txt")
archive_chunk_count=$(grep -c '^archive_chunk\[' "$BASE_DIR/clonepack.txt" || true)
if [ -z "$metadata_hash" ]; then
  echo "error: could not parse metadata chunk from clonepack manifest"
  exit 1
fi
echo "archive chunks: $archive_chunk_count"

echo ""
echo "==> Verifying metadata chunk round-trip..."
curl -fsS -o "$BASE_DIR/metadata.chunk" "${CURL_AUTH[@]}" "$SERVER_URL/v1/artifacts/$metadata_hash"
metadata_size=$(stat -f%z "$BASE_DIR/metadata.chunk" 2>/dev/null || stat -c%s "$BASE_DIR/metadata.chunk")
echo "metadata chunk size: $metadata_size bytes"
if [ "$metadata_size" -eq 0 ]; then
  echo "error: metadata chunk is empty"
  exit 1
fi
# Decode/encode round-trip via protoc to ensure it is valid protobuf.
if ! protoc --decode_raw < "$BASE_DIR/metadata.chunk" > "$BASE_DIR/metadata.txt" 2>&1; then
  echo "error: metadata chunk is not valid protobuf"
  cat "$BASE_DIR/metadata.txt"
  exit 1
fi
echo "metadata chunk protobuf decode OK"

verify_clone() {
  local dir="$1"
  local mode="$2"
  local expect_blobs="$3"
  cd "$dir"
  if [ -n "$(git status --short)" ]; then
    echo "error: git status not clean after $mode clone"
    git status --short
    exit 1
  fi
  if ! git diff --quiet HEAD; then
    echo "error: git diff reports changes after $mode clone"
    exit 1
  fi
  if ! git log --oneline -1 >/dev/null; then
    echo "error: git log failed after $mode clone"
    exit 1
  fi

  local sample_file
  sample_file=$(git ls-files | sed -n '1p')
  if [ -z "$sample_file" ]; then
    echo "error: no tracked files to verify blob availability"
    exit 1
  fi
  if [ "$expect_blobs" = "yes" ]; then
    if ! git show "HEAD:$sample_file" >/dev/null 2>&1; then
      echo "error: expected blob objects in $mode mode, but git show failed for $sample_file"
      exit 1
    fi
  else
    if git show "HEAD:$sample_file" >/dev/null 2>&1; then
      echo "error: expected blob objects to be missing in $mode mode, but git show succeeded for $sample_file"
      exit 1
    fi
  fi
  echo "$mode clone OK"
}

echo ""
echo "==> Full mode clone (git checkout-index)..."
full_dir="$BASE_DIR/full-clone"
full_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$full_dir" --mode=full --bench
full_end=$(now_ms)
printf "full clone took %d ms\n" $((full_end - full_start))
verify_clone "$full_dir" full yes

echo ""
echo "==> Fast mode clone (archive extraction only)..."
fast_dir="$BASE_DIR/fast-clone"
fast_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$fast_dir" --mode=fast --bench
fast_end=$(now_ms)
printf "fast clone took %d ms\n" $((fast_end - fast_start))
verify_clone "$fast_dir" fast no

echo ""
echo "==> Hybrid mode clone (archive + head-blobs)..."
hybrid_dir="$BASE_DIR/hybrid-clone"
hybrid_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$hybrid_dir" --mode=hybrid --bench
hybrid_end=$(now_ms)
printf "hybrid clone took %d ms\n" $((hybrid_end - hybrid_start))
verify_clone "$hybrid_dir" hybrid yes

echo ""
echo "==> Skeleton mode clone (.git only)..."
skeleton_dir="$BASE_DIR/skeleton-clone"
skeleton_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$skeleton_dir" --mode=skeleton
skeleton_end=$(now_ms)
printf "skeleton clone took %d ms\n" $((skeleton_end - skeleton_start))
if [ ! -d "$skeleton_dir/.git" ]; then
  echo "error: skeleton clone missing .git directory"
  exit 1
fi
if [ -n "$(find "$skeleton_dir" -mindepth 1 -maxdepth 1 -not -name '.git' -print -quit 2>/dev/null)" ]; then
  echo "error: skeleton clone has unexpected working tree files"
  exit 1
fi
if ! (cd "$skeleton_dir" && git rev-parse HEAD >/dev/null); then
  echo "error: git rev-parse failed after skeleton clone"
  exit 1
fi
echo "skeleton clone OK"

echo ""
echo "==> Comparing file lists..."
(cd "$full_dir" && git ls-files -z | sort -z) > "$BASE_DIR/files-full.txt"
(cd "$fast_dir" && git ls-files -z | sort -z) > "$BASE_DIR/files-fast.txt"
(cd "$hybrid_dir" && git ls-files -z | sort -z) > "$BASE_DIR/files-hybrid.txt"
if ! diff -q "$BASE_DIR/files-full.txt" "$BASE_DIR/files-fast.txt" >/dev/null; then
  echo "error: file lists differ between full and fast clones"
  diff "$BASE_DIR/files-full.txt" "$BASE_DIR/files-fast.txt" | head -20
  exit 1
fi
if ! diff -q "$BASE_DIR/files-full.txt" "$BASE_DIR/files-hybrid.txt" >/dev/null; then
  echo "error: file lists differ between full and hybrid clones"
  diff "$BASE_DIR/files-full.txt" "$BASE_DIR/files-hybrid.txt" | head -20
  exit 1
fi
echo "file lists match"

echo ""
echo "=========================================================="
echo "Clonepack round-trip test passed for $REPO."
echo "  sync:    $((sync_end - sync_start)) ms"
echo "  full:    $((full_end - full_start)) ms"
echo "  fast:    $((fast_end - fast_start)) ms"
echo "  hybrid:  $((hybrid_end - hybrid_start)) ms"
echo "  skeleton:$((skeleton_end - skeleton_start)) ms"
echo "=========================================================="
