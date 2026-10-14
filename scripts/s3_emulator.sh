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
  mkdir -p "$cache"
  tarball="$cache/versitygw.tar.gz"
  curl -fsSL --retry 3 --max-time 120 -o "$tarball" \
    "https://github.com/versity/versitygw/releases/download/$VERSION/versitygw_${VERSION}_${asset}.tar.gz"
  got="$(sha256 "$tarball")"
  if [ "$got" != "$sha" ]; then
    rm -f "$tarball"
    echo "error: versitygw checksum mismatch: got $got, want $sha" >&2
    exit 1
  fi
  tar -xzf "$tarball" -C "$cache" --strip-components=1
  rm -f "$tarball"
fi

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
endpoint="http://127.0.0.1:$port"
mkdir -p "$data_dir/store"
nohup "$bin" --access "$ACCESS_KEY" --secret "$SECRET_KEY" --region "$REGION" \
  --port "127.0.0.1:$port" posix "$data_dir/store" \
  </dev/null >"$data_dir/versitygw.log" 2>&1 &
pid=$!

# Any HTTP status means the listener is up; 000 means not yet.
for _ in $(seq 1 100); do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "$endpoint/" || true)"
  [ "$code" != "000" ] && break
  sleep 0.1
done
if [ "$code" = "000" ]; then
  kill "$pid" 2>/dev/null || true
  echo "error: versitygw did not start; log:" >&2
  cat "$data_dir/versitygw.log" >&2
  exit 1
fi

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
