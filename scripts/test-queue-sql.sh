#!/usr/bin/env bash
# Run the env-gated Postgres + MySQL queue tests against throwaway docker
# databases (real-DB correctness baseline), or against pre-set URLs (CI service
# containers).
#
# When CI_ARTIFACTS is set to a directory of prebuilt test binaries (from
# scripts/ci-build-artifacts.sh), no cargo compile runs — just execute them.
#
# Profile (compile path only): CARGO_PROFILE (default: ci).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/../rust"

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
  docker run -d --rm --name "$PG_NAME" \
    -e POSTGRES_PASSWORD=ripclone -e POSTGRES_DB=ripclone \
    -p 127.0.0.1::5432 postgres:16 >/dev/null
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
  for _ in $(seq 1 120); do
    docker exec "$MY_NAME" mysql -uroot -pripclone ripclone -e 'SELECT 1' >/dev/null 2>&1 && break
    sleep 0.5
  done

  echo "== RIPCLONE_TEST_PG_URL=$RIPCLONE_TEST_PG_URL =="
  echo "== RIPCLONE_TEST_MYSQL_URL=$RIPCLONE_TEST_MYSQL_URL =="
fi

run_lib_filter() {
  local filter="$1"
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    local bin="$CI_ARTIFACTS/ripclone-lib-tests"
    [ -x "$bin" ] || { echo "error: missing $bin" >&2; exit 1; }
    echo "== unit: $filter via prebuilt $bin =="
    "$bin" "$filter" --nocapture
  else
    echo "== unit: $filter via cargo (profile=$PROFILE) =="
    cargo test --profile "$PROFILE" --locked --lib "$filter" -- --nocapture
  fi
}

run_test_bin() {
  local name="$1"
  if [ -n "${CI_ARTIFACTS:-}" ]; then
    local bin="$CI_ARTIFACTS/$name"
    [ -x "$bin" ] || { echo "error: missing $bin" >&2; exit 1; }
    echo "== e2e: $name (prebuilt) =="
    "$bin" --nocapture
  else
    echo "== e2e: $name (cargo profile=$PROFILE) =="
    cargo test --profile "$PROFILE" --locked --test "$name" -- --nocapture
  fi
}

run_lib_filter lifecycle
run_lib_filter artifact_scheduler_postgres::tests::live_postgres_adversarial_conformance
run_test_bin e2e_worker_postgres
run_test_bin e2e_worker_mysql
run_test_bin e2e_metadata_postgres
run_test_bin e2e_metadata_mysql

echo "== OK =="
