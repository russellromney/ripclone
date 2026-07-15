#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PATTERN='mysql|postgres|postgresql|libsql|sqld'
failed=0

fail_matches() {
  local label="$1"
  shift
  local matches
  matches="$(rg -n -i "$PATTERN" "$@" 2>/dev/null || true)"
  if [ -n "$matches" ]; then
    echo "error: prohibited removed-database match in $label" >&2
    echo "$matches" >&2
    failed=1
  fi
}

echo "classified: rust/src/backends.rs -> required fail-closed diagnostics"
rg -n -i "$PATTERN" "$ROOT/rust/src/backends.rs"
if rg -n -i "$PATTERN" "$ROOT/rust/src" \
  --glob '!backends.rs' --glob '!dispatch/ENV_BAG.md'; then
  echo "error: reachable production source still names a removed database" >&2
  failed=1
fi

echo "classified: rust/tests/e2e_removed_database_config.rs -> rejection matrix proof"
rg -n -i "$PATTERN" "$ROOT/rust/tests/e2e_removed_database_config.rs"
fail_matches "other compiled tests" "$ROOT/rust/tests" \
  --glob '!e2e_removed_database_config.rs'

echo "classified: docs/BACKENDS.md, CHANGELOG.md, docs/CHANGELOG.md -> compatibility notice/history"
rg -n -i "$PATTERN" "$ROOT/docs/BACKENDS.md" "$ROOT/CHANGELOG.md" "$ROOT/docs/CHANGELOG.md"
public_matches="$(rg -n -i "$PATTERN" "$ROOT/README.md" "$ROOT/docs" 2>/dev/null \
  | rg -v '/docs/(BACKENDS|CHANGELOG)\.md:|/docs/internal/|/docs/superpowers/' || true)"
if [ -n "$public_matches" ]; then
  echo "error: prohibited removed-database match in public support documentation" >&2
  echo "$public_matches" >&2
  failed=1
fi
fail_matches "CI and examples" "$ROOT/.github" "$ROOT/.env.example" "$ROOT/tests"
fail_matches "scripts" "$ROOT/scripts" --glob '!audit-sqlite-only-support.sh'

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

if rg -n 'name = "(libsql|sqlx-mysql|sqlx-postgres)"' "$ROOT/rust/Cargo.lock"; then
  echo "error: removed database crate remains in Cargo.lock" >&2
  failed=1
fi
if rg -n 'features = \[[^]]*"(mysql|postgres)"|^libsql[[:space:]]*=' "$ROOT/rust/Cargo.toml"; then
  echo "error: removed database feature/dependency remains in Cargo.toml" >&2
  failed=1
fi

if [ "$failed" -ne 0 ]; then
  exit 1
fi
echo "audit-sqlite-only-support: all remaining matches classified; no reachable support found"
