#!/usr/bin/env bash
# Start a local S3-compatible server for tests and create one bucket.
#
#   scripts/s3_emulator.sh <data-dir> <bucket>
#
# The server is versitygw (Apache-2.0) with its posix backend: real SigV4
# auth, presigned URL expiry, Range GETs and multipart uploads. The binary is a
# pinned release, checked by SHA-256 and cached under
# ${RIPCLONE_TOOL_CACHE:-~/.cache/ripclone-tools}.
#
# On success it prints KEY=VALUE lines (S3 settings, test credentials, and
# S3_EMULATOR_PID) for `>> "$GITHUB_ENV"` or `export`. The server keeps running
# in the background; stop it with `kill "$S3_EMULATOR_PID"`. Its log is
# <data-dir>/versitygw.log.
set -euo pipefail

VERSION="v1.9.0"
ACCESS_KEY="ripclone-test"
SECRET_KEY="ripclone-test-secret"
REGION="us-east-1"

data_dir="${1:?usage: s3_emulator.sh <data-dir> <bucket>}"
bucket="${2:?usage: s3_emulator.sh <data-dir> <bucket>}"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) asset="Linux_x86_64" sha="0c28ccda3452451d7c71a0427f6aa7d0db5cac5e2b98a8e4dd2970967e1aadc1" ;;
  Linux-aarch64 | Linux-arm64) asset="Linux_arm64" sha="1ab234dc11d540d861898d391bfda5914eaff78f20d162c3ce7c84337b3d9204" ;;
  Darwin-arm64) asset="Darwin_arm64" sha="96e926d4c0f45f57385a2cfa566809f53387682e8c5c622b55915f82b4261c5f" ;;
  Darwin-x86_64) asset="Darwin_x86_64" sha="67b65bf563880799703079af194926ac796ec37e442cdfa9a69c0e1d7a4d3ccd" ;;
  *) echo "error: no pinned versitygw build for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | awk '{print $1}'
}

cache="${RIPCLONE_TOOL_CACHE:-$HOME/.cache/ripclone-tools}/versitygw-$VERSION-$asset"
bin="$cache/versitygw"
if [ ! -x "$bin" ]; then
  # Parallel tests install at the same time. Each one unpacks into its own
  # directory and renames the binary into place, so no process ever runs a
  # half-written file.
  mkdir -p "$cache"
  tmp="$(mktemp -d "$cache/install.XXXXXX")"
  trap 'rm -rf "$tmp"' EXIT
  curl -fsSL --retry 3 --max-time 120 -o "$tmp/versitygw.tar.gz" \
    "https://github.com/versity/versitygw/releases/download/$VERSION/versitygw_${VERSION}_${asset}.tar.gz"
  got="$(sha256 "$tmp/versitygw.tar.gz")"
  if [ "$got" != "$sha" ]; then
    echo "error: versitygw checksum mismatch: got $got, want $sha" >&2
    exit 1
  fi
  tar -xzf "$tmp/versitygw.tar.gz" -C "$tmp" --strip-components=1
  mv -f "$tmp/versitygw" "$bin"
  rm -rf "$tmp"
  trap - EXIT
fi

# Bind port 0 and ask the OS which port the process got. Choosing a free
# port first and passing it in races with every other process on the host.
# versitygw's banner echoes the requested port, so read the socket instead.
listen_port() { # pid
  if command -v lsof >/dev/null; then
    lsof -nP -a -p "$1" -iTCP -sTCP:LISTEN -Fn 2>/dev/null | sed -n 's/^n.*:\([0-9]*\)$/\1/p' | head -1
  else
    ss -Hltnp 2>/dev/null | grep "pid=$1," | awk '{print $4}' | sed 's/.*://' | head -1
  fi
}

mkdir -p "$data_dir/store"
nohup "$bin" --access "$ACCESS_KEY" --secret "$SECRET_KEY" --region "$REGION" \
  --port "127.0.0.1:0" posix "$data_dir/store" \
  </dev/null >"$data_dir/versitygw.log" 2>&1 &
pid=$!

# Any HTTP status means the listener is up; 000 means not yet.
port=""
code="000"
for _ in $(seq 1 100); do
  kill -0 "$pid" 2>/dev/null || break
  [ -n "$port" ] || port="$(listen_port "$pid" || true)"
  if [ -n "$port" ]; then
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$port/" || true)"
    [ "$code" != "000" ] && break
  fi
  sleep 0.1
done
if [ "$code" = "000" ]; then
  kill "$pid" 2>/dev/null || true
  echo "error: versitygw did not start; log:" >&2
  cat "$data_dir/versitygw.log" >&2
  exit 1
fi
endpoint="http://127.0.0.1:$port"

curl -fsS --max-time 10 -X PUT --aws-sigv4 "aws:amz:$REGION:s3" \
  --user "$ACCESS_KEY:$SECRET_KEY" "$endpoint/$bucket" >/dev/null

cat <<EOF
RIPCLONE_S3_ENDPOINT=$endpoint
RIPCLONE_S3_REGION=$REGION
RIPCLONE_S3_BUCKET=$bucket
AWS_ACCESS_KEY_ID=$ACCESS_KEY
AWS_SECRET_ACCESS_KEY=$SECRET_KEY
S3_EMULATOR_PID=$pid
EOF
