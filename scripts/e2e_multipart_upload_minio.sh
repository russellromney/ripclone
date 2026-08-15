#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_IMAGE="minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
CONTAINER="ripclone-multipart-minio-$$"
BUCKET="ripclone-multipart-$$"
TEST_NAME="multipart_large_file_completes_and_failed_upload_aborts_on_s3"

command -v docker >/dev/null || {
  echo "error: Docker is required" >&2
  exit 1
}
command -v curl >/dev/null || {
  echo "error: curl is required" >&2
  exit 1
}

cleanup() {
  local status=$?
  if timeout 10 docker inspect "$CONTAINER" >/dev/null 2>&1; then
    if ! timeout 30 docker rm -f "$CONTAINER" >/dev/null; then
      echo "error: failed to remove MinIO container $CONTAINER" >&2
      status=1
    fi
  fi
  if timeout 10 docker inspect "$CONTAINER" >/dev/null 2>&1; then
    echo "error: MinIO container $CONTAINER survived cleanup" >&2
    status=1
  fi
  exit "$status"
}
trap cleanup EXIT

timeout 15 docker image inspect "$MINIO_IMAGE" >/dev/null 2>&1 || \
  timeout 120 docker pull "$MINIO_IMAGE"
timeout 30 docker run -d --name "$CONTAINER" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  "$MINIO_IMAGE" server /data >/dev/null

HOST_PORT="$(timeout 10 docker port "$CONTAINER" 9000/tcp | awk -F: 'NR==1 {print $NF}')"
test -n "$HOST_PORT" || {
  echo "error: Docker did not publish MinIO port" >&2
  exit 1
}
ENDPOINT="http://127.0.0.1:$HOST_PORT"

if ! timeout 45 bash -c \
  "for _ in \$(seq 1 30); do curl --max-time 2 -fsS '$ENDPOINT/minio/health/live' >/dev/null && exit 0; sleep 1; done; exit 1"; then
  echo "error: digest-pinned MinIO did not become ready within 45 seconds" >&2
  exit 1
fi

timeout 30 docker exec "$CONTAINER" sh -c \
  "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && mc mb local/$BUCKET >/dev/null"

export RIPCLONE_REQUIRE_MINIO=1
export RIPCLONE_S3_ENDPOINT="$ENDPOINT"
export RIPCLONE_S3_BUCKET="$BUCKET"
export RIPCLONE_S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3GC_TIMEOUT_SECS="${S3GC_TIMEOUT_SECS:-300}"

echo "multipart MinIO proof: $TEST_NAME"
set +e
bash "$ROOT/scripts/ci.sh" s3gc "$TEST_NAME"
test_status=$?
set -e

incomplete_uploads=""
if ! incomplete_uploads="$(timeout 30 docker exec "$CONTAINER" \
  mc ls --incomplete --recursive "local/$BUCKET" 2>&1)"; then
  echo "error: failed to inspect MinIO incomplete multipart uploads" >&2
  test_status=1
elif [ -n "$incomplete_uploads" ]; then
  echo "error: MinIO retained incomplete multipart state:" >&2
  printf '%s\n' "$incomplete_uploads" >&2
  test_status=1
fi

echo "MinIO image: $MINIO_IMAGE"
exit "$test_status"
