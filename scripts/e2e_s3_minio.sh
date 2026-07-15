#!/usr/bin/env bash
set -euo pipefail

: "${RIPCLONE_REQUIRE_MINIO:?RIPCLONE_REQUIRE_MINIO=1 is required}"
[ "$RIPCLONE_REQUIRE_MINIO" = 1 ] || { echo "error: RIPCLONE_REQUIRE_MINIO must be 1" >&2; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${RIPCLONE_BIN_DIR:-$ROOT/rust/target/release}"
SERVER="$BIN_DIR/ripclone-server"
CLI="$BIN_DIR/ripclone"
MINIO_IMAGE="minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
CONTAINER="ripclone-minio-$PPID-$$"
BASE="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-minio.XXXXXX")"
BUCKET="ripclone-$PPID-$$"
TOKEN="minio-e2e-token"
SERVER_PID=""
WORKER_PID=""

for command in docker curl aws git; do
  command -v "$command" >/dev/null || { echo "error: required command unavailable: $command" >&2; exit 1; }
done
for bin in "$SERVER" "$CLI"; do
  [ -x "$bin" ] || { echo "error: missing release binary $bin" >&2; exit 1; }
done

cleanup() {
  if [ -n "$WORKER_PID" ]; then kill "$WORKER_PID" 2>/dev/null || true; wait "$WORKER_PID" 2>/dev/null || true; fi
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$BASE"
}
trap cleanup EXIT

docker pull "$MINIO_IMAGE" >/dev/null
docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  "$MINIO_IMAGE" server /data >/dev/null
PORT="$(docker port "$CONTAINER" 9000/tcp | sed -E 's/.*:([0-9]+)$/\1/' | head -1)"
[ -n "$PORT" ] || { echo "error: MinIO port was not assigned" >&2; exit 1; }
ENDPOINT="http://127.0.0.1:$PORT"

ready=0
for _ in $(seq 1 120); do
  if curl --max-time 2 -fsS "$ENDPOINT/minio/health/live" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.25
done
[ "$ready" = 1 ] || { docker logs "$CONTAINER" >&2; echo "error: MinIO unavailable after 30s" >&2; exit 1; }

export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
aws --endpoint-url "$ENDPOINT" s3api create-bucket --bucket "$BUCKET" >/dev/null

ORIGINS="$BASE/origins"
mkdir -p "$ORIGINS/acme"
make_origin() {
  local repo="$1" value="$2" work="$BASE/work-$repo" bare="$ORIGINS/acme/$repo.git"
  git init -q -b main "$work"
  git -C "$work" config user.email test@example.com
  git -C "$work" config user.name Test
  printf '%s\n' "$value" > "$work/value.txt"
  git -C "$work" add value.txt
  git -C "$work" commit -q -m initial
  git init --bare -q -b main "$bare"
  git -C "$work" push -q "$bare" main
  git -C "$bare" symbolic-ref HEAD refs/heads/main
}

export RIPCLONE_SERVER_TOKEN="$TOKEN" RIPCLONE_TRUST_GATEWAY=1
export RIPCLONE_ORIGIN_BASE="file://$ORIGINS"
export RIPCLONE_S3_ENDPOINT="$ENDPOINT" RIPCLONE_S3_REGION=us-east-1 RIPCLONE_S3_BUCKET="$BUCKET"
export RIPCLONE_S3_PREFIX="artifacts/"

start_server() {
  local label="$1" metadata="$2" port
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=""; fi
  port=$((20000 + RANDOM % 40000))
  export RIPCLONE_METADATA="$metadata"
  if [ "$metadata" = sqlite ]; then export RIPCLONE_METADATA_DB_URL="$BASE/$label-meta.db"; else unset RIPCLONE_METADATA_DB_URL; fi
  "$SERVER" --host 127.0.0.1 --port "$port" --cas-dir "$BASE/$label-cas" --repo-root "$BASE/$label-repos" >"$BASE/$label.log" 2>&1 &
  SERVER_PID=$!
  SERVER_URL="http://127.0.0.1:$port"
  local ready=0
  for _ in $(seq 1 120); do
    if curl --max-time 2 -fsS "$SERVER_URL/readyz" >/dev/null 2>&1; then ready=1; break; fi
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.25
  done
  [ "$ready" = 1 ] || { cat "$BASE/$label.log" >&2; echo "error: $label server unavailable after 30s" >&2; exit 1; }
}

clone_retry() {
  local repo="$1" dir="$2"; shift 2
  for _ in $(seq 1 120); do
    rm -rf "$dir"
    if "$CLI" --server "$SERVER_URL" clone "acme/$repo" --dir "$dir" "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.25
  done
  echo "error: clone never became ready: $repo $*" >&2
  return 1
}

echo "row: SQLite metadata/queue + authenticated API worker + S3-compatible artifact bytes"
make_origin sqlite-s3 artifact
export RIPCLONE_QUEUE=sqlite RIPCLONE_QUEUE_DB_URL="$BASE/s3-queue.db"
start_server sqlite-artifacts sqlite
job_token="$("$CLI" mint-worker-token --ttl-days 1)"
env -u RIPCLONE_QUEUE_DB_URL -u RIPCLONE_METADATA_DB_URL \
  RIPCLONE_QUEUE=api RIPCLONE_QUEUE_API_URL="$SERVER_URL" \
  RIPCLONE_METADATA=api RIPCLONE_METADATA_REPORT_URL="$SERVER_URL/v1/refs" \
  RIPCLONE_METADATA_JOB_TOKEN="$job_token" \
  "$BIN_DIR/ripclone-worker" --cas-dir "$BASE/sqlite-artifacts-cas" \
    --repo-root "$BASE/api-worker-repos" --idle-poll-ms 50 >"$BASE/api-worker.log" 2>&1 &
WORKER_PID=$!
"$CLI" --server "$SERVER_URL" add acme/sqlite-s3 >/dev/null
"$CLI" --server "$SERVER_URL" sync acme/sqlite-s3 >/dev/null
clone_retry sqlite-s3 "$BASE/head" --depth 1
clone_retry sqlite-s3 "$BASE/full" --depth 0
clone_retry sqlite-s3 "$BASE/files" --mode files
[ "$(cat "$BASE/files/value.txt")" = artifact ]

echo "row: temporary S3 ref store + local queue + S3-compatible artifact bytes"
kill "$WORKER_PID" 2>/dev/null || true
wait "$WORKER_PID" 2>/dev/null || true
WORKER_PID=""
export RIPCLONE_QUEUE=local
unset RIPCLONE_QUEUE_DB_URL
make_origin rollback-s3 rollback
start_server s3-refs s3
"$CLI" --server "$SERVER_URL" add acme/rollback-s3 >/dev/null
"$CLI" --server "$SERVER_URL" sync acme/rollback-s3 >/dev/null
clone_retry rollback-s3 "$BASE/rollback"
[ "$(cat "$BASE/rollback/value.txt")" = rollback ]

echo "row: direct S3RefStore read/write"
(cd "$ROOT/rust" && cargo test --profile ci --locked --test ref_ordering \
  s3_ref_store_newest_wins -- --exact --nocapture)

objects="$(aws --endpoint-url "$ENDPOINT" s3api list-objects-v2 --bucket "$BUCKET" --query 'length(Contents)' --output text)"
[ "$objects" != None ] && [ "$objects" -gt 0 ] || { echo "error: MinIO contains no artifacts/refs" >&2; exit 1; }
echo "e2e_s3_minio: PASS image=$MINIO_IMAGE objects=$objects"
