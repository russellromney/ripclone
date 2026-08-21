#!/usr/bin/env bash
# Fail when removed control/queue/dispatcher support escapes its explicit
# rejection tests and compatibility documentation.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PATTERN='RIPCLONE_(METADATA($|[^A-Z_])|METADATA_DB_|QUEUE($|[^A-Z_])|QUEUE_DB_|DISPATCH($|[^A-Z_])|DISPATCH_|HEARTBEAT_URL|RECHECK_MAX|REF_CACHE_TTL_SECS)|postgres://|mysql://|tokio-postgres|mysql_async|sqlx.*/(postgres|mysql)|branch-level repository configuration|repo-config/.*/branches'

matches="$(cd "$ROOT" && git grep -nE "$PATTERN" -- . ':!scripts/audit_control_support.sh' || true)"
allowed=0
unexpected=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  path="${line%%:*}"
  case "$path" in
    rust/src/control.rs|rust/src/server.rs|rust/tests/control_startup.rs|rust/tests/e2e_token_only_worker.rs|rust/tests/suites/local_default/e2e_repo_config.rs|docs/BACKENDS.md|docs/CONFIG.md|docs/CHANGELOG.md|docs/internal/*)
      allowed=$((allowed + 1))
      ;;
    *)
      echo "unsupported control support outside allowlist: $line" >&2
      unexpected=$((unexpected + 1))
      ;;
  esac
done <<<"$matches"

if [ "$allowed" -eq 0 ]; then
  echo "unsupported-control audit matched zero rejection/compatibility rows" >&2
  exit 1
fi
if [ "$unexpected" -ne 0 ]; then
  exit 1
fi

dependency_matches="$(cd "$ROOT" && git grep -nE \
  'name = "(sqlx|sqlx-postgres|sqlx-mysql|tokio-postgres|postgres|mysql_async)"|features = .*"(postgres|mysql)"' \
  -- rust/Cargo.toml rust/Cargo.lock || true)"
if [ -n "$dependency_matches" ]; then
  echo "removed network-database dependency closure is present:" >&2
  echo "$dependency_matches" >&2
  exit 1
fi

for path in \
  rust/src/dispatch \
  rust/src/queue/postgres.rs \
  rust/src/queue/mysql.rs \
  rust/src/queue/local.rs \
  scripts/test-queue-sql.sh
do
  if (cd "$ROOT" && git ls-files --error-unmatch "$path" >/dev/null 2>&1) ||
     (cd "$ROOT" && git ls-files "$path/" | grep -q .); then
    echo "removed control implementation still exists: $path" >&2
    exit 1
  fi
done

removed_config_implementation="$(cd "$ROOT" && git grep -nE \
  'RepoConfigStore|fn (branch_key|overlay)\(' -- rust/src || true)"
if [ -n "$removed_config_implementation" ]; then
  echo "removed repository-config implementation is present:" >&2
  echo "$removed_config_implementation" >&2
  exit 1
fi

artifact_config_keys="$(cd "$ROOT" && git grep -n 'repo-config/' -- rust/src || true)"
if [ -n "$artifact_config_keys" ]; then
  echo "artifact-backed repository-config key is present in production:" >&2
  echo "$artifact_config_keys" >&2
  exit 1
fi

echo "unsupported-control audit: $allowed allowlisted rejection/compatibility rows; no implementation or dependency escape"
