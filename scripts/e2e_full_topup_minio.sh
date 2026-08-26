#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_IMAGE="minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
CONTAINER="ripclone-topup-minio-$$"
BUCKET="ripclone-topup-$$"
TEST_NAME="minio_signed_base_stale_url_refresh_remains_pinned_to_b"

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
  timeout 20 docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

timeout 15 docker image inspect "$MINIO_IMAGE" >/dev/null 2>&1 || timeout 120 docker pull "$MINIO_IMAGE"
timeout 30 docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  "$MINIO_IMAGE" server /data >/dev/null
HOST_PORT="$(timeout 10 docker port "$CONTAINER" 9000/tcp | awk -F: 'NR==1 {print $NF}')"
test -n "$HOST_PORT" || {
  echo "error: Docker did not publish the MinIO port" >&2
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

timeout 30 docker exec "$CONTAINER" sh -c \
  "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && mc mb local/$BUCKET >/dev/null"

export RIPCLONE_REQUIRE_MINIO=1
export RIPCLONE_S3_ENDPOINT="$ENDPOINT"
export RIPCLONE_S3_BUCKET="$BUCKET"
export RIPCLONE_S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

if [ -n "${CI_ARTIFACTS:-}" ]; then
  test_bin="$CI_ARTIFACTS/e2e_full_topup"
  test -x "$test_bin" || {
    echo "error: missing $test_bin" >&2
    exit 1
  }
  listed="$(timeout 60 "$test_bin" --ignored --list)"
  run=("$test_bin" --ignored --exact "$TEST_NAME" --nocapture)
else
  listed="$(cd "$ROOT/rust" && timeout 300 cargo test --profile ci --locked --test e2e_full_topup -- --ignored --list)"
  run=(cargo test --profile ci --locked --test e2e_full_topup -- --ignored --exact "$TEST_NAME" --nocapture)
fi
grep -Fqx "$TEST_NAME: test" <<<"$listed" || {
  echo "error: exact MinIO test '$TEST_NAME' is missing" >&2
  exit 1
}

log="$(mktemp "${TMPDIR:-/tmp}/ripclone-topup-minio.XXXXXX")"
set +e
(cd "$ROOT/rust" && timeout 300 "${run[@]}") 2>&1 | tee "$log"
rc=${PIPESTATUS[0]}
set -e
test "$rc" -eq 0 || exit "$rc"
grep -Fq "running 1 test" "$log" || {
  echo "error: exact MinIO filter ran zero or multiple tests" >&2
  exit 1
}
grep -Eq "test result: ok\. 1 passed; 0 failed;" "$log" || {
  echo "error: exact MinIO proof did not report one passing test" >&2
  exit 1
}
if grep -Fq "SKIP" "$log"; then
  echo "error: MinIO proof emitted SKIP" >&2
  exit 1
fi
if grep -Fq "background build failed" "$log" || grep -Fq "fatal:" "$log"; then
  echo "error: MinIO proof logged an exact-result build failure" >&2
  exit 1
fi
rm -f "$log"
echo "MinIO image: $MINIO_IMAGE"
