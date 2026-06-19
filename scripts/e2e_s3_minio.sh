#!/usr/bin/env bash
set -euo pipefail

# End-to-end test of the S3-compatible storage backend using a local MinIO
# container. The server uploads artifacts to MinIO and serves them via signed
# URL redirects; the client follows the redirects and installs the repo.

REPO="oven-sh/bun"

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

MINIO_IMAGE="minio/minio:latest"
MINIO_CONTAINER="ripclone-minio-e2e"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin"
BUCKET="ripclone-test"

PORT=$(( 10000 + RANDOM % 50000 ))
SERVER_URL="http://127.0.0.1:$PORT"
BASE_DIR="$(mktemp -d /tmp/ripclone-s3-e2e.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

cleanup() {
  if [ -n "${SERVER_PID:-}" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  docker rm -f "$MINIO_CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

echo "==> Starting MinIO..."
docker run --rm -d \
  --name "$MINIO_CONTAINER" \
  -p "$MINIO_API_PORT:9000" \
  -p "$MINIO_CONSOLE_PORT:9001" \
  -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
  -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
  "$MINIO_IMAGE" server /data --console-address ":9001" \
  >/dev/null

for i in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:$MINIO_API_PORT/minio/health/live" >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! curl -fsS "http://127.0.0.1:$MINIO_API_PORT/minio/health/live" >/dev/null 2>&1; then
  echo "error: MinIO failed to start"
  exit 1
fi

AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
  aws s3 mb "s3://$BUCKET" \
    --endpoint-url "http://127.0.0.1:$MINIO_API_PORT" \
    --region us-east-1 \
    >/dev/null

echo "==> Starting ripclone server with S3 backend..."
export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
export AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
export RIPCLONE_S3_ENDPOINT="http://127.0.0.1:$MINIO_API_PORT"
export RIPCLONE_S3_REGION="us-east-1"
export RIPCLONE_S3_BUCKET="$BUCKET"
export RIPCLONE_S3_PREFIX="test/"
export RIPCLONE_S3_CACHE_DIR="$CAS_DIR"

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

echo "==> Syncing mirror and uploading artifacts to S3..."
sync_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO"
sync_end=$(now_ms)
printf "sync + upload took %d ms\n" $((sync_end - sync_start))

echo "==> Direct-install clone via signed-URL redirects..."
install_dir="$BASE_DIR/bun-install"
install_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$install_dir"
install_end=$(now_ms)
printf "install took %d ms\n" $((install_end - install_start))

echo "==> Verifying git status..."
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
if ! git log --oneline -1 >/dev/null; then
  echo "error: git log failed"
  exit 1
fi

echo "==> S3 objects created:"
AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
  aws s3 ls "s3://$BUCKET/test/" \
    --endpoint-url "http://127.0.0.1:$MINIO_API_PORT" \
    --recursive \
    --human-readable \
    --summarize

echo ""
echo "=========================================================="
echo "S3 e2e passed for $REPO."
echo "  sync + upload: $((sync_end - sync_start)) ms"
echo "  install:       $((install_end - install_start)) ms"
echo "=========================================================="
