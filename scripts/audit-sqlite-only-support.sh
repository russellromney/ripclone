#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PATTERN='mysql|postgres|postgresql|libsql|sqld'
failed=0

fail_matches() {
  local label="$1"
  shift
  local matches
  matches="$(git -C "$ROOT" grep -n -i -E "$PATTERN" -- "$@" 2>/dev/null || true)"
  if [ -n "$matches" ]; then
    echo "error: prohibited removed-database match in $label" >&2
    echo "$matches" >&2
    failed=1
  fi
}

echo "classified: rust/src/backends.rs -> required fail-closed diagnostics"
git -C "$ROOT" grep -n -i -E "$PATTERN" -- rust/src/backends.rs
if git -C "$ROOT" grep -n -i -E "$PATTERN" -- rust/src \
  ':(exclude)rust/src/backends.rs' ':(exclude)rust/src/dispatch/ENV_BAG.md'; then
  echo "error: reachable production source still names a removed database" >&2
  failed=1
fi

echo "classified: rust/tests/e2e_removed_database_config.rs -> rejection matrix proof"
git -C "$ROOT" grep -n -i -E "$PATTERN" -- rust/tests/e2e_removed_database_config.rs
fail_matches "other compiled tests" rust/tests \
  ':(exclude)rust/tests/e2e_removed_database_config.rs'

echo "classified: docs/BACKENDS.md, CHANGELOG.md, docs/CHANGELOG.md -> compatibility notice/history"
git -C "$ROOT" grep -n -i -E "$PATTERN" -- docs/BACKENDS.md CHANGELOG.md docs/CHANGELOG.md
public_matches="$(git -C "$ROOT" grep -n -i -E "$PATTERN" -- README.md docs \
  ':(exclude)docs/BACKENDS.md' ':(exclude)docs/CHANGELOG.md' \
  ':(exclude)docs/internal/**' ':(exclude)docs/superpowers/**' 2>/dev/null || true)"
if [ -n "$public_matches" ]; then
  echo "error: prohibited removed-database match in public support documentation" >&2
  echo "$public_matches" >&2
  failed=1
fi
fail_matches "CI and examples" .github .env.example tests
fail_matches "scripts" scripts ':(exclude)scripts/audit-sqlite-only-support.sh'
fail_matches "Cargo policy and configuration" rust/deny.toml rust/.cargo

for file in \
  rust/src/artifact_scheduler_mysql.rs rust/src/artifact_scheduler_postgres.rs rust/src/artifact_scheduler_libsql.rs \
  rust/src/git_source_registry_mysql.rs rust/src/git_source_registry_postgres.rs rust/src/git_source_registry_libsql.rs \
  rust/src/meta/mysql.rs rust/src/meta/postgres.rs rust/src/meta/libsql.rs \
  rust/src/queue/mysql_db.rs rust/src/queue/postgres_db.rs rust/src/queue/libsql_db.rs; do
  if [ -e "$ROOT/$file" ]; then
    echo "error: removed implementation still exists: $file" >&2
    failed=1
  fi
done

if git -C "$ROOT" grep -n -E 'name = "(libsql|sqlx-mysql|sqlx-postgres)"' -- rust/Cargo.lock; then
  echo "error: removed database crate remains in Cargo.lock" >&2
  failed=1
fi
if git -C "$ROOT" grep -n -E 'features = \[[^]]*"(mysql|postgres)"|^libsql[[:space:]]*=' -- rust/Cargo.toml; then
  echo "error: removed database feature/dependency remains in Cargo.toml" >&2
  failed=1
fi

if [ "$failed" -ne 0 ]; then
  exit 1
fi
echo "audit-sqlite-only-support: all remaining matches classified; no reachable support found"
