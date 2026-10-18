#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-bench-token}"
export RIPCLONE_SERVER_TOKEN
AUTH="Authorization: Ripclone $(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')"

WORKDIR=$(mktemp -d /tmp/ripclone-size.XXXXXX)
PORT=${PORT:-18769}
SERVER_PID=""
trap 'kill $SERVER_PID 2>/dev/null || true; rm -rf "$WORKDIR"' EXIT

./rust/target/release/ripclone-server \
  --data-dir "$WORKDIR" \
  --port "$PORT" \
  > "$WORKDIR/server.log" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null; then
    break
  fi
  sleep 0.1
done

curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null || { echo "server did not start"; cat "$WORKDIR/server.log"; exit 1; }

REPO="${REPO:-oven-sh/bun}"
API="http://127.0.0.1:$PORT/v1/repos/github/$REPO"
echo "==> Adding $REPO..."
commit=$(curl -sf -X POST -H "$AUTH" "$API/add" | python3 -c 'import sys,json; print(json.load(sys.stdin)["commit"])')
echo "==> Waiting for the Files output of $commit..."
while true; do
  curl -sf -H "$AUTH" "$API/commits/$commit?kind=files" > "$WORKDIR/sync.json"
  state=$(python3 -c 'import sys,json; print(json.load(open(sys.argv[1]))["state"])' "$WORKDIR/sync.json")
  [ "$state" = "pending" ] || break
  sleep 2
done
[ "$state" = "ready" ] || { echo "Files output $state"; cat "$WORKDIR/sync.json"; exit 1; }

echo "==> CAS total"
du -sh "$WORKDIR/cache"

echo "==> Clonepack artifact sizes"
python3 - "$WORKDIR/sync.json" "$WORKDIR/cache" <<'PY'
import json, sys, os
sync = json.load(open(sys.argv[1]))
cas = sys.argv[2]

def size(h):
    p = os.path.join(cas, h[:2], h)
    return os.path.getsize(p) if os.path.exists(p) else 0

def varint(d, i):
    x = s = 0
    while True:
        b = d[i]; i += 1
        x |= (b & 0x7f) << s
        if not b & 0x80: break
        s += 7
    return x, i

def parse_message(data):
    i = 0
    fields = {}
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
        fields.setdefault(field, []).append(val)
    return fields

cpm_hash = sync.get('clonepack_manifest', '')
if not cpm_hash:
    print('no clonepack_manifest in sync response')
    sys.exit(0)

cpm_data = open(os.path.join(cas, cpm_hash[:2], cpm_hash), 'rb').read()
cpm = parse_message(cpm_data)
meta_ref = cpm.get(4, [b''])[0]
meta = parse_message(meta_ref)
meta_hash = meta.get(1, [b''])[0].hex()
print(f"clonepack_manifest: {cpm_hash}  {size(cpm_hash):,} bytes")
print(f"metadata_chunk:     {meta_hash}  {size(meta_hash):,} bytes")
for i, ref_bytes in enumerate(cpm.get(5, [])):
    arc = parse_message(ref_bytes)
    arc_hash = arc.get(1, [b''])[0].hex()
    print(f"archive_chunk[{i}]:  {arc_hash}  {size(arc_hash):,} bytes")
PY

echo "==> Top 10 largest CAS objects"
find "$WORKDIR/cache" -type f -exec ls -l {} \; | sort -k5 -n | tail -10
