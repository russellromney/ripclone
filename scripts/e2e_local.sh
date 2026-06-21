#!/usr/bin/env bash
set -euo pipefail

# Comprehensive offline end-to-end test against the REAL binaries.
#
# Mirrors from a local file:// origin (RIPCLONE_ORIGIN_BASE) so it needs no
# network and is safe in CI. Exercises every clone mode, re-sync (new commits),
# and the LSM incremental build, asserting git correctness at each step.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SERVER_BIN="$ROOT_DIR/rust/target/release/ripclone-server"
CLI_BIN="$ROOT_DIR/rust/target/release/ripclone"

for bin in "$SERVER_BIN" "$CLI_BIN"; do
  [ -x "$bin" ] || { echo "error: missing binary $bin (cargo build --release)"; exit 1; }
done

export RIPCLONE_TOKEN="${RIPCLONE_TOKEN:-e2e-local-token}"
sha256() { if command -v sha256sum >/dev/null; then sha256sum | awk '{print $1}'; else shasum -a 256 | awk '{print $1}'; fi; }
TOKEN_HASH=$(printf '%s' "$RIPCLONE_TOKEN" | sha256)

BASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-e2e-local.XXXXXX")"
ORIGIN_ROOT="$BASE_DIR/origins"
CAS_DIR="$BASE_DIR/cas"
REPO_ROOT="$BASE_DIR/repos"
WORK="$BASE_DIR/work"
mkdir -p "$ORIGIN_ROOT" "$WORK"
export RIPCLONE_ORIGIN_BASE="file://$ORIGIN_ROOT"
export TMPDIR="$REPO_ROOT"
mkdir -p "$REPO_ROOT"

PORT=$(( 20000 + RANDOM % 40000 ))
SERVER_URL="http://127.0.0.1:$PORT"
SERVER_PID=""

cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$BASE_DIR"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  ok: $*"; }

gitc() { git -C "$1" "${@:2}"; }

# --- build a local origin: <ORIGIN_ROOT>/<owner>/<repo>.git -------------------
new_origin() {
  local owner="$1" repo="$2"
  local w="$WORK/$owner-$repo"
  rm -rf "$w"; mkdir -p "$w"
  git -C "$w" init -q -b main
  git -C "$w" config user.email t@t; git -C "$w" config user.name t
  local bare="$ORIGIN_ROOT/$owner/$repo.git"
  mkdir -p "$(dirname "$bare")"
  git init --bare -q -b main "$bare"
  echo "$w"
}
commit() { # work_dir file content msg
  printf '%s' "$3" > "$1/$2"
  git -C "$1" add -A; git -C "$1" commit -q -m "$4"
}
publish() { # work_dir owner repo
  local bare="$ORIGIN_ROOT/$2/$3.git"
  git -C "$1" push -q --force "$bare" main
  git -C "$bare" symbolic-ref HEAD refs/heads/main
}

start_server() {
  RUST_LOG=warn "$SERVER_BIN" --cas-dir "$CAS_DIR" --repo-root "$REPO_ROOT" \
    --host 127.0.0.1 --port "$PORT" >"$BASE_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 200); do
    if curl -fsS -o /dev/null "$SERVER_URL/readyz" 2>/dev/null; then return 0; fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then cat "$BASE_DIR/server.log"; fail "server died"; fi
    sleep 0.1
  done
  cat "$BASE_DIR/server.log"; fail "server not ready"
}

sync_repo() { "$CLI_BIN" --server "$SERVER_URL" sync "$1/$2" >/dev/null; }
clone_repo() { # owner repo dir [extra cli args...]
  rm -rf "$3"
  "$CLI_BIN" --server "$SERVER_URL" clone "$1/$2" --dir "$3" "${@:4}" >/dev/null
}

assert_clean() { [ -z "$(gitc "$1" status --porcelain)" ] || fail "$2: worktree not clean"; }
assert_file() { [ "$(cat "$1/$2")" = "$3" ] || fail "$1/$2 != '$3'"; }
assert_count() { [ "$(gitc "$1" rev-list --count HEAD)" = "$2" ] || fail "$1: rev-list != $2"; }

echo "==> starting server ($SERVER_URL)"
start_server

# === editable depth=1 (shallow) ===============================================
echo "==> editable --depth 1"
w=$(new_origin acme d1)
commit "$w" a.txt "one" c1
commit "$w" a.txt "two" c2
publish "$w" acme d1
sync_repo acme d1
clone_repo acme d1 "$BASE_DIR/c-d1" --depth 1
assert_file "$BASE_DIR/c-d1" a.txt "two"
[ -f "$BASE_DIR/c-d1/.git/shallow" ] || fail "depth=1 missing .git/shallow"
assert_count "$BASE_DIR/c-d1" 1
assert_clean "$BASE_DIR/c-d1" "d1"
gitc "$BASE_DIR/c-d1" fsck --connectivity-only HEAD >/dev/null || fail "d1 fsck"
pass "depth=1 shallow, clean, 1 commit"

# === editable depth=0 (full) ==================================================
echo "==> editable --depth 0"
w=$(new_origin acme d0)
commit "$w" a.txt "1" c1; commit "$w" a.txt "2" c2; commit "$w" a.txt "3" c3
publish "$w" acme d0
sync_repo acme d0
clone_repo acme d0 "$BASE_DIR/c-d0" --depth 0
assert_file "$BASE_DIR/c-d0" a.txt "3"
[ ! -f "$BASE_DIR/c-d0/.git/shallow" ] || fail "full clone must not be shallow"
assert_count "$BASE_DIR/c-d0" 3
gitc "$BASE_DIR/c-d0" rev-list --objects HEAD >/dev/null || fail "d0 incomplete closure"
gitc "$BASE_DIR/c-d0" fsck --connectivity-only HEAD >/dev/null || fail "d0 fsck"
assert_clean "$BASE_DIR/c-d0" "d0"
pass "depth=0 complete, fsck clean, 3 commits"

# === files mode ===============================================================
echo "==> files mode"
w=$(new_origin acme files)
commit "$w" only.txt "hello" c1
publish "$w" acme files
sync_repo acme files
clone_repo acme files "$BASE_DIR/c-files" --mode files
assert_file "$BASE_DIR/c-files" only.txt "hello"
pass "files mode materialized worktree"

# === skeleton mode ============================================================
echo "==> skeleton mode"
w=$(new_origin acme skel)
commit "$w" f "1" c1
publish "$w" acme skel
sync_repo acme skel
clone_repo acme skel "$BASE_DIR/c-skel" --mode skeleton
[ -d "$BASE_DIR/c-skel/.git" ] || fail "skeleton missing .git"
pass "skeleton installed .git"

# === special files: symlink + executable bit ==================================
echo "==> symlinks + exec bits"
w=$(new_origin acme special)
echo "target contents" > "$w/target.txt"
ln -s target.txt "$w/link.txt"
printf '#!/bin/sh\necho hi\n' > "$w/run.sh"; chmod +x "$w/run.sh"
git -C "$w" add -A; git -C "$w" commit -q -m c1
publish "$w" acme special
sync_repo acme special
clone_repo acme special "$BASE_DIR/c-special" --depth 1
[ -L "$BASE_DIR/c-special/link.txt" ] || fail "symlink not preserved"
[ "$(readlink "$BASE_DIR/c-special/link.txt")" = "target.txt" ] || fail "symlink target wrong"
[ -x "$BASE_DIR/c-special/run.sh" ] || fail "exec bit not preserved"
assert_clean "$BASE_DIR/c-special" "special"
pass "symlink + exec bit preserved, clean"

# === re-sync picks up new commits (stale-ref regression) ======================
echo "==> re-sync after new push"
w=$(new_origin acme resync)
commit "$w" a "1" c1; publish "$w" acme resync
sync_repo acme resync
clone_repo acme resync "$BASE_DIR/c-rs1" --depth 0
assert_count "$BASE_DIR/c-rs1" 1
commit "$w" a "2" c2; publish "$w" acme resync
sync_repo acme resync
clone_repo acme resync "$BASE_DIR/c-rs2" --depth 0
assert_file "$BASE_DIR/c-rs2" a "2"
assert_count "$BASE_DIR/c-rs2" 2
pass "re-sync served the new commit"

# === LSM incremental build ====================================================
echo "==> LSM incremental (restarting server with RIPCLONE_LSM=1)"
kill "$SERVER_PID" 2>/dev/null || true; SERVER_PID=""
export RIPCLONE_LSM=1 RIPCLONE_LSM_SEAL_BYTES=1
start_server
w=$(new_origin acme lsm)
commit "$w" a.txt "v1" c1; publish "$w" acme lsm
sync_repo acme lsm                                  # seal level 0
commit "$w" a.txt "v2" c2; commit "$w" b.txt "b" c3; publish "$w" acme lsm
sync_repo acme lsm                                  # tail since seal -> seal level 1
clone_repo acme lsm "$BASE_DIR/c-lsm" --depth 0
assert_file "$BASE_DIR/c-lsm" a.txt "v2"
assert_file "$BASE_DIR/c-lsm" b.txt "b"
assert_count "$BASE_DIR/c-lsm" 3
gitc "$BASE_DIR/c-lsm" rev-list --objects HEAD >/dev/null || fail "lsm incomplete closure"
gitc "$BASE_DIR/c-lsm" fsck --connectivity-only HEAD >/dev/null || fail "lsm fsck"
# the v1 blob (sealed in level 0, changed by c2) must still be present
v1=$(git -C "$w" rev-parse HEAD~2:a.txt)
gitc "$BASE_DIR/c-lsm" cat-file -e "$v1" || fail "sealed-level blob missing"
pass "LSM full clone complete across two seals"

echo "ALL E2E PASSED"
