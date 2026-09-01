#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPECTED=$'ripclone\nripclone-server\nripclone-worker'

actual="$({
  cd "$ROOT/rust"
  cargo metadata --locked --no-deps --format-version 1
} | jq -r '.packages[] | select(.name == "ripclone") | .targets[]
  | select(.kind == ["bin"] and (.["required-features"] | length) == 0) | .name' | sort)"

if [ "$actual" != "$EXPECTED" ]; then
  echo "error: default Cargo-installed programs do not match the release allowlist" >&2
  diff -u <(printf '%s\n' "$EXPECTED") <(printf '%s\n' "$actual") >&2 || true
  exit 1
fi

tools="$({
  cd "$ROOT/rust"
  cargo metadata --locked --no-deps --format-version 1
} | jq -r '.packages[] | select(.name == "ripclone") | .targets[]
  | select(.kind == ["bin"] and (.["required-features"] | index("dev-tools"))) | .name' | sort)"
if [ "$tools" != $'ripclone-proxy\nwriter_bench' ]; then
  echo "error: developer-only program allowlist changed" >&2
  exit 1
fi

archive="$(
  rg -o '\$bindir/[a-z0-9-]+' "$ROOT/.github/workflows/release.yml" \
    | cut -d/ -f2 | sort -u
)"
installer="$(
  sed -n 's/^for b in \(.*\); do$/\1/p' "$ROOT/install.sh" \
    | tr ' ' '\n' | sed '/^$/d' | sort -u
)"

check_surface() {
  local surface="$1" value="$2"
  if [ "$value" != "$EXPECTED" ]; then
    echo "error: $surface programs do not match the release allowlist" >&2
    diff -u <(printf '%s\n' "$EXPECTED") <(printf '%s\n' "$value") >&2 || true
    exit 1
  fi
}

check_surface archive "$archive"
check_surface installer "$installer"

echo "release surface: ripclone, ripclone-server, ripclone-worker"
