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

export RIPCLONE_SERVER_TOKEN="${RIPCLONE_SERVER_TOKEN:-e2e-local-token}"
# This script does `sync` then `clone`. Builds are always asynchronous and
# two-phase: depth=1 is ready when `sync` returns, but the full/files variants
# finish in the background, so the clone helpers below retry until ready.
# Per-repo access enforcement (AU1) probes the provider over HTTP and can't
# reach this file:// origin. This is a single-tenant local e2e, so use the
# documented trust-mode escape hatch (the shared token is the only auth here).
export RIPCLONE_TRUST_GATEWAY=1
sha256() { if command -v sha256sum >/dev/null; then sha256sum | awk '{print $1}'; else shasum -a 256 | awk '{print $1}'; fi; }
TOKEN_HASH=$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | sha256)

BASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-e2e-local.XXXXXX")"
ORIGIN_ROOT="$BASE_DIR/origins"
CAS_DIR="$BASE_DIR/cas"
REPO_ROOT="$BASE_DIR/repos"
WORK="$BASE_DIR/work"
mkdir -p "$ORIGIN_ROOT" "$WORK"
export RIPCLONE_ORIGIN_BASE="file://$ORIGIN_ROOT"
export TMPDIR="$REPO_ROOT"
mkdir -p "$REPO_ROOT"

# LFS pointer files are stored/served as-is by ripclone. Make sure every git
# operation in this script (server mirror clones, reference clones, client
# materialization) passes pointers through without invoking git-lfs, which may
# not be installed and is not needed for these fixture tests.
GITCONFIG="$BASE_DIR/gitconfig"
cat > "$GITCONFIG" <<'EOF'
[filter "lfs"]
	clean = cat
	smudge = cat
	process = 
	required = false
EOF
export GIT_CONFIG_GLOBAL="$GITCONFIG"

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
  # Fully reap any previous server and bind a fresh port. Reusing the same port
  # right after killing the old process races the OS releasing it ("Address
  # already in use"), e.g. on the LSM restart below.
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
  PORT=$(( 20000 + RANDOM % 40000 ))
  SERVER_URL="http://127.0.0.1:$PORT"
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
# Builds are two-phase: depth=1 is ready as soon as `sync` returns, but the full
# and files variants finish in the background, and on a re-sync the full variant
# briefly serves the previous commit. So retry the clone until it succeeds.
clone_repo() { # owner repo dir [extra cli args...]
  local i
  for i in $(seq 1 80); do
    rm -rf "$3"
    if "$CLI_BIN" --server "$SERVER_URL" clone "$1/$2" --dir "$3" "${@:4}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  # Final attempt without suppression so the real error surfaces under `set -e`.
  rm -rf "$3"
  "$CLI_BIN" --server "$SERVER_URL" clone "$1/$2" --dir "$3" "${@:4}" >/dev/null
}
# Like clone_repo but also waits out the background full build / brief stale
# serving on a re-sync: retry until the clone reaches exactly `$4` commits.
clone_until_count() { # owner repo dir count [extra cli args...]
  local i
  for i in $(seq 1 80); do
    rm -rf "$3"
    if "$CLI_BIN" --server "$SERVER_URL" clone "$1/$2" --dir "$3" "${@:5}" >/dev/null 2>&1 \
      && [ "$(gitc "$3" rev-list --count HEAD 2>/dev/null)" = "$4" ]; then
      return 0
    fi
    sleep 0.5
  done
  fail "clone of $1/$2 never reached $4 commits"
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
clone_until_count acme d0 "$BASE_DIR/c-d0" 3 --depth 0
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
clone_until_count acme resync "$BASE_DIR/c-rs1" 1 --depth 0
assert_count "$BASE_DIR/c-rs1" 1
commit "$w" a "2" c2; publish "$w" acme resync
sync_repo acme resync
clone_until_count acme resync "$BASE_DIR/c-rs2" 2 --depth 0
assert_file "$BASE_DIR/c-rs2" a "2"
assert_count "$BASE_DIR/c-rs2" 2
pass "re-sync served the new commit"

# === LSM incremental build ====================================================
echo "==> LSM incremental (restarting server with RIPCLONE_LSM=1)"
# start_server reaps the running server and rebinds a fresh port.
export RIPCLONE_LSM=1 RIPCLONE_LSM_SEAL_BYTES=1
start_server
w=$(new_origin acme lsm)
commit "$w" a.txt "v1" c1; publish "$w" acme lsm
sync_repo acme lsm                                  # seal level 0
commit "$w" a.txt "v2" c2; commit "$w" b.txt "b" c3; publish "$w" acme lsm
sync_repo acme lsm                                  # tail since seal -> seal level 1
clone_until_count acme lsm "$BASE_DIR/c-lsm" 3 --depth 0
assert_file "$BASE_DIR/c-lsm" a.txt "v2"
assert_file "$BASE_DIR/c-lsm" b.txt "b"
assert_count "$BASE_DIR/c-lsm" 3
gitc "$BASE_DIR/c-lsm" rev-list --objects HEAD >/dev/null || fail "lsm incomplete closure"
gitc "$BASE_DIR/c-lsm" fsck --connectivity-only HEAD >/dev/null || fail "lsm fsck"
# the v1 blob (sealed in level 0, changed by c2) must still be present
v1=$(git -C "$w" rev-parse HEAD~2:a.txt)
gitc "$BASE_DIR/c-lsm" cat-file -e "$v1" || fail "sealed-level blob missing"
pass "LSM full clone complete across two seals"

# === byte-for-byte equivalence oracle =========================================
run_equivalence_oracle() {
  local writer_label="$1" io_uring="$2"
  echo "==> equivalence oracle ($writer_label)"

  local w sub_w sub_bare sub_sha
  w=$(new_origin equivalence fixture)

  # Build a submodule repo and get its HEAD sha for a manual gitlink entry.
  sub_w="$WORK/equivalence-sub"
  rm -rf "$sub_w"; mkdir -p "$sub_w"
  git -C "$sub_w" init -q -b main
  echo "submodule readme" > "$sub_w/sub.txt"
  git -C "$sub_w" add sub.txt
  git -C "$sub_w" -c user.email=t@t -c user.name=t commit -q -m "init sub"
  sub_sha=$(git -C "$sub_w" rev-parse HEAD)
  sub_bare="$ORIGIN_ROOT/equivalence/submod.git"
  rm -rf "$sub_bare"
  mkdir -p "$(dirname "$sub_bare")"
  git init --bare -q -b main "$sub_bare"
  git -C "$sub_w" push -q "$sub_bare" main

  # Disable LFS filters so pointer files are stored verbatim (matches this
  # machine and the ripclone pass-through policy).
  git -C "$w" config filter.lfs.clean cat
  git -C "$w" config filter.lfs.smudge cat
  git -C "$w" config filter.lfs.process ""
  git -C "$w" config filter.lfs.required false

  # Fixture contents.
  echo "symlink target contents" > "$w/target.txt"
  ln -s target.txt "$w/link.txt"
  # Non-UTF-8 symlink target (raw bytes 0x80-0x83).
  python3 -c "import os; os.symlink(b'\\x80\\x81\\x82\\x83', '$w/bad-link.txt')"
  printf '#!/bin/sh\necho hello\n' > "$w/run.sh"; chmod +x "$w/run.sh"
  : > "$w/empty.txt"
  printf 'unicode file contents\n' > "$w/日本語.txt"
  mkdir -p "$w/deeply/nested/dir/structure"
  echo "deeply nested content" > "$w/deeply/nested/dir/structure/deep.txt"
  mkdir -p "$w/empty-dir"; : > "$w/empty-dir/.gitkeep"
  # 9 MiB binary blob to cross chunk boundaries.
  python3 -c 'import sys; sys.stdout.buffer.write(bytes((i % 251) for i in range(9*1024*1024)))' > "$w/big.bin"
  printf '*.lfs filter=lfs diff=lfs merge=lfs -text\n' > "$w/.gitattributes"
  printf 'version https://git-lfs.github.com/spec/v1\noid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nsize 0\n' > "$w/asset.lfs"
  printf '[submodule "sub"]\n\tpath = vendor/sub\n\turl = %s\n' "$sub_bare" > "$w/.gitmodules"
  git -C "$w" update-index --add --cacheinfo "160000,$sub_sha,vendor/sub"

  git -C "$w" add -A
  git -C "$w" -c user.email=t@t -c user.name=t commit -q -m fixture
  git -C "$w" tag v1.0.0
  publish "$w" equivalence fixture
  git -C "$w" push -q --force "$ORIGIN_ROOT/equivalence/fixture.git" main --tags

  sync_repo equivalence fixture

  local git_shallow git_full
  git_shallow="$BASE_DIR/git-fixture-shallow-$writer_label"
  git_full="$BASE_DIR/git-fixture-full-$writer_label"

  rm -rf "$git_shallow" "$git_full"
  git clone -q --depth 1 \
      --config filter.lfs.process= \
      --config filter.lfs.smudge=cat \
      --config filter.lfs.clean=cat \
      --config filter.lfs.required=false \
      "file://$ORIGIN_ROOT/equivalence/fixture.git" "$git_shallow"
  git clone -q \
      --config filter.lfs.process= \
      --config filter.lfs.smudge=cat \
      --config filter.lfs.clean=cat \
      --config filter.lfs.required=false \
      "file://$ORIGIN_ROOT/equivalence/fixture.git" "$git_full"

  local rip_shallow rip_full rip_files
  rip_shallow="$BASE_DIR/rip-fixture-shallow-$writer_label"
  rip_full="$BASE_DIR/rip-fixture-full-$writer_label"
  rip_files="$BASE_DIR/rip-fixture-files-$writer_label"

  # Run ripclone clones under the chosen writer backend.
  (
    [ "$io_uring" = "1" ] && export RIPCLONE_IO_URING=1 || export RIPCLONE_IO_URING=0
    clone_repo equivalence fixture "$rip_shallow" --depth 1
  )
  (
    [ "$io_uring" = "1" ] && export RIPCLONE_IO_URING=1 || export RIPCLONE_IO_URING=0
    clone_until_count equivalence fixture "$rip_full" 1 --depth 0
  )
  (
    [ "$io_uring" = "1" ] && export RIPCLONE_IO_URING=1 || export RIPCLONE_IO_URING=0
    clone_repo equivalence fixture "$rip_files" --mode files
  )

  diff_worktree() {
    diff -r --no-dereference --exclude=.git "$1" "$2" >/dev/null || fail "$3 worktree differs"
  }

  diff_worktree "$git_shallow" "$rip_shallow" "editable depth=1 ($writer_label)"
  diff_worktree "$git_full" "$rip_full" "editable depth=0 ($writer_label)"
  diff_worktree "$git_full" "$rip_files" "files mode ($writer_label)"

  [ "$(gitc "$rip_shallow" rev-parse HEAD)" = "$(gitc "$git_shallow" rev-parse HEAD)" ] || fail "shallow HEAD mismatch ($writer_label)"
  [ "$(gitc "$rip_shallow" rev-parse refs/heads/main)" = "$(gitc "$git_shallow" rev-parse refs/heads/main)" ] || fail "shallow branch ref mismatch ($writer_label)"
  [ "$(gitc "$rip_full" rev-parse HEAD)" = "$(gitc "$git_full" rev-parse HEAD)" ] || fail "full HEAD mismatch ($writer_label)"
  [ "$(gitc "$rip_full" rev-parse refs/heads/main)" = "$(gitc "$git_full" rev-parse refs/heads/main)" ] || fail "full branch ref mismatch ($writer_label)"

  gitc "$rip_shallow" fsck --connectivity-only HEAD >/dev/null || fail "shallow fsck ($writer_label)"
  gitc "$rip_full" fsck --connectivity-only HEAD >/dev/null || fail "full fsck ($writer_label)"
  assert_clean "$rip_shallow" "shallow ($writer_label)"
  assert_clean "$rip_full" "full ($writer_label)"

  pass "equivalence oracle ($writer_label)"
}

run_equivalence_oracle posix 0
if [ "$(uname -s)" = "Linux" ]; then
  run_equivalence_oracle io_uring 1
fi

echo "ALL E2E PASSED"
