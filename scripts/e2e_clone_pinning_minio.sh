#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_IMAGE="minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
CONTAINER="ripclone-pin-minio-$$"
BUCKET="ripclone-pin-$$"

: "${RIPCLONE_BIN_DIR:?RIPCLONE_BIN_DIR must name the release binary directory}"
test -x "$RIPCLONE_BIN_DIR/ripclone" || {
  echo "error: missing release CLI $RIPCLONE_BIN_DIR/ripclone" >&2
  exit 1
}
command -v docker >/dev/null || {
  echo "error: Docker is required" >&2
  exit 1
}

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker image inspect "$MINIO_IMAGE" >/dev/null 2>&1 || docker pull "$MINIO_IMAGE"
docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  "$MINIO_IMAGE" server /data >/dev/null
HOST_PORT="$(docker port "$CONTAINER" 9000/tcp | awk -F: 'NR==1 {print $NF}')"
test -n "$HOST_PORT" || {
  echo "error: Docker did not publish MinIO port" >&2
  exit 1
}
ENDPOINT="http://127.0.0.1:$HOST_PORT"

ready=0
for _ in $(seq 1 30); do
  if curl --max-time 2 -fsS "$ENDPOINT/minio/health/live" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
test "$ready" -eq 1 || {
  echo "error: digest-pinned MinIO did not become ready within 30 seconds" >&2
  exit 1
}

docker exec "$CONTAINER" sh -c \
  "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && mc mb local/$BUCKET >/dev/null"

export RIPCLONE_REQUIRE_MINIO=1
export RIPCLONE_S3_ENDPOINT="$ENDPOINT"
export RIPCLONE_S3_BUCKET="$BUCKET"
export RIPCLONE_S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

tests=(
  expired_signed_url_retry_stays_on_pinned_commit
  expired_bearer_blocks_pinned_refresh
)

for test_name in "${tests[@]}"; do
  echo "minio pinning proof: $test_name"
  timeout 300 bash "$ROOT/scripts/ci.sh" s3gc "$test_name"
done

echo "MinIO image: $MINIO_IMAGE"
