#!/usr/bin/env bash
set -euo pipefail

# End-to-end test of the direct artifact-install clone path on oven-sh/bun.
# The server builds every prebuilt artifact (skeleton pack/idx, head-blobs
# pack/idx, index, archive, manifest) during sync. The client only downloads
# and writes files.

REPO="${REPO:-oven-sh/bun}"
OWNER="${REPO%%/*}"
NAME="${REPO#*/}"

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

file_size() {
  stat -f%z "$1" 2>/dev/null || stat -c%s "$1"
}

if [ -z "${RIPCLONE_TOKEN:-}" ]; then
  export RIPCLONE_TOKEN="test-token"
fi

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
}

PORT=$(( 10000 + RANDOM % 50000 ))
SERVER_URL="http://127.0.0.1:$PORT"
BASE_DIR="$(mktemp -d /tmp/ripclone-install-e2e.XXXXXX)"
CAS_DIR="$BASE_DIR/cache"
REPO_ROOT="$BASE_DIR/repos"

cleanup() {
  if [ -n "${SERVER_PID:-}" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

start_server() {
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
}

require_clean_status() {
  local dir="$1"
  local msg="$2"
  cd "$dir"
  if [ -n "$(git status --short)" ]; then
    echo "error: $msg"
    git status --short | head -20
    exit 1
  fi
}

verify_contents() {
  local dir="$1"
  local label="$2"

  echo ""
  echo "==> Verifying $label file contents against HEAD..."
  local content_errors=0
  while IFS= read -r -d '' record; do
    local meta_path="${record%%$'\t'*}"
    local path="${record#*$'\t'}"
    local meta=( $meta_path )
    local mode="${meta[0]}"
    local obj_type="${meta[1]}"
    if [ -z "$path" ] || [ "$obj_type" != "blob" ]; then
      continue
    fi
    # Symlinks are verified separately by comparing the link target.
    if [[ "$mode" == 120* ]]; then
      continue
    fi
    # Use git's own diff so clean/smudge filters (e.g. core.autocrlf,
    # .gitattributes text=auto) are respected.
    if ! (cd "$dir" && git diff --quiet HEAD -- "$path"); then
      echo "error: content mismatch for $path"
      content_errors=$((content_errors + 1))
    fi
  done < <(git -C "$MIRROR_DIR" ls-tree -r -z HEAD)
  if [ "$content_errors" -ne 0 ]; then
    exit 1
  fi
  echo "$label file contents match HEAD"

  echo "==> Verifying $label git status is clean..."
  require_clean_status "$dir" "git status not clean after $label"

  echo "==> Verifying $label tracked files are present..."
  local missing=0
  while IFS= read -r -d '' path; do
    if [ ! -e "$dir/$path" ] && [ ! -L "$dir/$path" ]; then
      echo "error: tracked file missing: $path"
      missing=$((missing + 1))
    fi
  done < <(cd "$dir" && git ls-files -z)
  if [ "$missing" -ne 0 ]; then
    exit 1
  fi
  echo "$label all tracked files present"

  echo "==> Verifying $label symlinks..."
  local symlink_errors=0
  while IFS= read -r -d '' record; do
    local meta_path="${record%%$'\t'*}"
    local path="${record#*$'\t'}"
    local meta=( $meta_path )
    local mode="${meta[0]}"
    if [[ "$mode" == 120* ]]; then
      local expected
      expected=$(git -C "$MIRROR_DIR" cat-file blob "HEAD:$path" 2>/dev/null || true)
      local actual
      actual=$(readlink "$dir/$path" || true)
      if [ "$expected" != "$actual" ]; then
        echo "error: symlink mismatch for $path: expected '$expected', got '$actual'"
        symlink_errors=$((symlink_errors + 1))
      fi
    fi
  done < <(git -C "$MIRROR_DIR" ls-tree -r -z HEAD)
  if [ "$symlink_errors" -ne 0 ]; then
    exit 1
  fi
  echo "$label symlinks OK"

  echo "==> Verifying $label executable bits..."
  local exe_errors=0
  while IFS= read -r -d '' record; do
    local meta_path="${record%%$'\t'*}"
    local path="${record#*$'\t'}"
    local meta=( $meta_path )
    local mode="${meta[0]}"
    if [ -z "$path" ]; then continue; fi
    if [ "$mode" = "100755" ]; then
      if [ ! -x "$dir/$path" ]; then
        echo "error: expected executable: $path"
        exe_errors=$((exe_errors + 1))
      fi
    elif [ "$mode" = "100644" ]; then
      if [ -x "$dir/$path" ]; then
        echo "error: unexpected executable bit: $path"
        exe_errors=$((exe_errors + 1))
      fi
    fi
  done < <(git -C "$MIRROR_DIR" ls-tree -r -z HEAD)
  if [ "$exe_errors" -ne 0 ]; then
    exit 1
  fi
  echo "$label executable bits OK"

  echo "==> Verifying $label basic git operations..."
  cd "$dir"
  if ! git log --oneline -1 >/dev/null; then
    echo "error: git log failed for $label"
    exit 1
  fi
  if ! git diff --quiet HEAD; then
    echo "error: git diff reports changes for $label"
    exit 1
  fi
  echo "$label git operations OK"
}

echo "==> Starting server..."
start_server

echo ""
echo "==> Syncing mirror and building artifacts (one-time)..."
sync_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" sync "$REPO"
sync_end=$(now_ms)
printf "sync took %d ms\n" $((sync_end - sync_start))

MIRROR_DIR="$REPO_ROOT/${OWNER}_${NAME}.git"
if [ ! -d "$MIRROR_DIR" ]; then
  echo "error: mirror not found at $MIRROR_DIR"
  exit 1
fi

echo ""
echo "==> Full mode clone (git checkout-index)..."
install_dir="$BASE_DIR/bun-install"
install_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$install_dir" --mode=full
install_end=$(now_ms)
printf "full install took %d ms\n" $((install_end - install_start))

verify_contents "$install_dir" "full"

echo "==> Verifying origin remote is configured..."
origin_url=$(cd "$install_dir" && git remote get-url origin 2>/dev/null || true)
if [ "$origin_url" != "https://github.com/$REPO.git" ]; then
  echo "error: unexpected origin url: '$origin_url'"
  exit 1
fi
echo "origin remote OK: $origin_url"

echo "==> Verifying edits are detected..."
cd "$install_dir"
echo "# modified" >> README.md
if git diff --quiet HEAD; then
  echo "error: git diff did not detect an edit"
  exit 1
fi
git checkout -- README.md
require_clean_status "$install_dir" "git status not clean after reverting edit"
echo "edit detection OK"

echo "==> Verifying git status stays clean with core.fileMode=true..."
cd "$install_dir"
git config core.fileMode true
require_clean_status "$install_dir" "git status not clean after enabling core.fileMode"
echo "core.fileMode=true OK"

echo ""
echo "==> Fast mode clone (archive extraction only)..."
fast_dir="$BASE_DIR/bun-fast"
fast_start=$(now_ms)
"$RIPCLONE" --server "$SERVER_URL" clone "$REPO" --dir "$fast_dir" --mode=fast
fast_end=$(now_ms)
printf "fast install took %d ms\n" $((fast_end - fast_start))

verify_contents "$fast_dir" "fast"

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo "=========================================================="
echo "All e2e checks passed for $REPO."
echo "  sync:    $((sync_end - sync_start)) ms"
echo "  full:    $((install_end - install_start)) ms"
echo "  fast:    $((fast_end - fast_start)) ms"
echo "=========================================================="
