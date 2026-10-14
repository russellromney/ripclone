#!/usr/bin/env bash
# Start a local MinIO server for tests and create one bucket.
#
#   scripts/s3_emulator.sh <data-dir> <bucket>
#
# MinIO no longer ships community binaries or images, so this builds the last
# community release (RELEASE.2025-10-15T17-29-55Z, pinned by commit) from its
# archived source with `go install`. Go's checksum database verifies the
# source. The binary is cached under ${RIPCLONE_TOOL_CACHE:-~/.cache/ripclone-tools};
# the first build takes a few minutes and needs `go` on PATH.
#
# On success it prints KEY=VALUE lines (S3 settings, test credentials, and
# S3_EMULATOR_PID) for `>> "$GITHUB_ENV"` or `export`. The server keeps running
# in the background; stop it with `kill "$S3_EMULATOR_PID"`. Its log is
# <data-dir>/minio.log.
set -euo pipefail

MINIO_COMMIT="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" # RELEASE.2025-10-15T17-29-55Z
ACCESS_KEY="ripclone-test"
SECRET_KEY="ripclone-test-secret"
REGION="us-east-1"

data_dir="${1:?usage: s3_emulator.sh <data-dir> <bucket>}"
bucket="${2:?usage: s3_emulator.sh <data-dir> <bucket>}"

cache="${RIPCLONE_TOOL_CACHE:-$HOME/.cache/ripclone-tools}/minio-$MINIO_COMMIT-$(uname -s)-$(uname -m)"
bin="$cache/minio"
if [ ! -x "$bin" ]; then
  command -v go >/dev/null || {
    echo "error: building MinIO needs Go on PATH" >&2
    exit 1
  }
  mkdir -p "$cache"
  echo "building MinIO $MINIO_COMMIT from source (first run only)" >&2
  GOBIN="$cache" go install "github.com/minio/minio@$MINIO_COMMIT" >&2
fi

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}
port="$(free_port)"
console_port="$(free_port)"
endpoint="http://127.0.0.1:$port"
mkdir -p "$data_dir/store"
MINIO_ROOT_USER="$ACCESS_KEY" MINIO_ROOT_PASSWORD="$SECRET_KEY" MINIO_REGION="$REGION" \
  nohup "$bin" server "$data_dir/store" \
  --address "127.0.0.1:$port" --console-address "127.0.0.1:$console_port" \
  </dev/null >"$data_dir/minio.log" 2>&1 &
pid=$!

ready=0
for _ in $(seq 1 150); do
  if curl -fs -o /dev/null --max-time 1 "$endpoint/minio/health/live"; then
    ready=1
    break
  fi
  sleep 0.1
done
if [ "$ready" -ne 1 ]; then
  kill "$pid" 2>/dev/null || true
  echo "error: MinIO did not start; log:" >&2
  cat "$data_dir/minio.log" >&2
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
