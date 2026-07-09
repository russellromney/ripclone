#!/usr/bin/env bash
# Run the env-gated Postgres + MySQL queue tests against throwaway docker
# databases (real-DB correctness baseline). Brings the containers up on random
# host ports, waits for readiness, runs the unit + e2e tests with the
# RIPCLONE_TEST_*_URL env vars set, then tears the containers down.
#
# Skip container management when URLs are already set (CI service containers):
#   RIPCLONE_TEST_PG_URL=postgres://… RIPCLONE_TEST_MYSQL_URL=mysql://… \
#     scripts/test-queue-sql.sh
#
# Profile: CARGO_PROFILE (default: ci) — same compile as the unit-test gate so
# we do not pay a second full graph for debug or release.
set -euo pipefail

cd "$(dirname "$0")/../rust"

PROFILE="${CARGO_PROFILE:-ci}"
PG_NAME="ripclone-test-pg-$$"
MY_NAME="ripclone-test-mysql-$$"
OWNED_CONTAINERS=0

cleanup() {
  if [ "$OWNED_CONTAINERS" = "1" ]; then
    docker rm -f "$PG_NAME" "$MY_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ -n "${RIPCLONE_TEST_PG_URL:-}" ] && [ -n "${RIPCLONE_TEST_MYSQL_URL:-}" ]; then
  echo "== using pre-provisioned DB URLs (no local containers) =="
  echo "== RIPCLONE_TEST_PG_URL=$RIPCLONE_TEST_PG_URL =="
  echo "== RIPCLONE_TEST_MYSQL_URL=$RIPCLONE_TEST_MYSQL_URL =="
else
  OWNED_CONTAINERS=1
  echo "== starting postgres + mysql containers =="
  # Start both at once so their pull/init overlap.
  docker run -d --rm --name "$PG_NAME" \
    -e POSTGRES_PASSWORD=ripclone -e POSTGRES_DB=ripclone \
    -p 127.0.0.1::5432 postgres:16 >/dev/null
  # Fast-ish MySQL init: skip binary log + doublewrite (test-only, durability
  # not required). Still real mysql:8 wire protocol for the dialect tests.
  docker run -d --rm --name "$MY_NAME" \
    -e MYSQL_ROOT_PASSWORD=ripclone -e MYSQL_DATABASE=ripclone \
    -p 127.0.0.1::3306 mysql:8 \
    --skip-log-bin \
    --innodb-flush-log-at-trx-commit=0 \
    --sync-binlog=0 \
    >/dev/null

  PG_PORT=$(docker port "$PG_NAME" 5432/tcp | head -1 | sed 's/.*://')
  MY_PORT=$(docker port "$MY_NAME" 3306/tcp | head -1 | sed 's/.*://')
  export RIPCLONE_TEST_PG_URL="postgres://postgres:ripclone@127.0.0.1:${PG_PORT}/ripclone"
  export RIPCLONE_TEST_MYSQL_URL="mysql://root:ripclone@127.0.0.1:${MY_PORT}/ripclone"

  echo "== waiting for postgres ($PG_PORT) =="
  for _ in $(seq 1 60); do
    docker exec "$PG_NAME" pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 0.5
  done

  echo "== waiting for mysql ($MY_PORT) =="
  # Use a real authenticated query (not `ping`): mysql:8's entrypoint runs a
  # temporary init server that answers ping before the real server restarts, so
  # ping passes too early and the first real connection hits EOF.
  for _ in $(seq 1 120); do
    docker exec "$MY_NAME" mysql -uroot -pripclone ripclone -e 'SELECT 1' >/dev/null 2>&1 && break
    sleep 0.5
  done

  echo "== RIPCLONE_TEST_PG_URL=$RIPCLONE_TEST_PG_URL =="
  echo "== RIPCLONE_TEST_MYSQL_URL=$RIPCLONE_TEST_MYSQL_URL =="
fi

echo "== unit: queue + metadata lifecycle on pg + mysql (profile=$PROFILE) =="
# "lifecycle" matches {postgres,mysql}_queue_lifecycle (jobs table) and
# {postgres,mysql}_refstore_lifecycle (refs table) — independent tables.
cargo test --profile "$PROFILE" --locked --lib lifecycle -- --nocapture

echo "== e2e: real worker process on pg + mysql =="
cargo test --profile "$PROFILE" --locked --test e2e_worker_postgres --test e2e_worker_mysql -- --nocapture

echo "== e2e: metadata store on pg + mysql (full server) =="
cargo test --profile "$PROFILE" --locked --test e2e_metadata_postgres --test e2e_metadata_mysql -- --nocapture

echo "== OK =="
