#!/usr/bin/env bash
# Build the static-musl Linux release target and prove the binaries actually run
# on a clean Alpine container.
#
# musl is a different `target_env` from every other job in CI: it is the only one
# that compiles the mimalloc `#[global_allocator]` and the hand-written kernel
# `struct statx` in src/statx_compat.rs. Without this script a musl-only break
# (wrong struct offset, a symbol musl does not have, a dynamic library sneaking
# into the link) would first surface when a release tag is pushed.
#
#   scripts/musl-smoke.sh
#
# Env:
#   MUSL_TARGET    rust target triple    (default x86_64-unknown-linux-musl)
#   MUSL_PLATFORM  docker platform       (default linux/amd64)
#   MUSL_PROFILE   cargo profile         (default ci)
#
# Needs cargo-zigbuild (zig supplies the musl C cross-toolchain) and docker.
set -euo pipefail

TARGET="${MUSL_TARGET:-x86_64-unknown-linux-musl}"
PLATFORM="${MUSL_PLATFORM:-linux/amd64}"
PROFILE="${MUSL_PROFILE:-ci}"

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$repo_root/rust"

BINS=(ripclone ripclone-server ripclone-worker git-remote-ripclone)

echo "==> building ${BINS[*]} for $TARGET (profile: $PROFILE)"
cargo zigbuild --profile "$PROFILE" --locked --target "$TARGET" \
  "${BINS[@]/#/--bin=}"

echo "==> building the library test binary for $TARGET"
test_bin="$(cargo-zigbuild test --no-run --profile "$PROFILE" --locked \
  --target "$TARGET" --lib --message-format=json |
  python3 -c '
import json, sys
for line in sys.stdin:
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    if msg.get("executable") and msg.get("profile", {}).get("test"):
        print(msg["executable"])
' | tail -n1)"
[ -n "$test_bin" ] || { echo "could not locate the musl test binary" >&2; exit 1; }

bindir="$repo_root/rust/target/$TARGET/$PROFILE"

# A release binary that quietly grew a dynamic dependency still runs on the glibc
# build host — it only fails on the user's Alpine box. Reject it here instead.
echo "==> checking every binary is fully static (no PT_INTERP, no dynamic section)"
for b in "${BINS[@]}"; do
  python3 - "$bindir/$b" <<'PY'
import struct, sys

path = sys.argv[1]
with open(path, "rb") as fh:
    data = fh.read()
if data[:4] != b"\x7fELF":
    sys.exit(f"{path}: not an ELF binary")
if data[4] != 2:
    sys.exit(f"{path}: expected a 64-bit ELF")

e_phoff, = struct.unpack_from("<Q", data, 0x20)
e_phentsize, e_phnum = struct.unpack_from("<HH", data, 0x36)
PT_INTERP, PT_DYNAMIC = 3, 2
kinds = {
    struct.unpack_from("<I", data, e_phoff + i * e_phentsize)[0]
    for i in range(e_phnum)
}
if PT_INTERP in kinds:
    sys.exit(f"{path}: has PT_INTERP — dynamically linked, will not run on Alpine")
if PT_DYNAMIC in kinds:
    sys.exit(f"{path}: has PT_DYNAMIC — links a shared library at runtime")
print(f"    {path.rsplit('/', 1)[-1]}: static")
PY
done

echo "==> running the binaries on a clean Alpine container ($PLATFORM)"
docker run --rm --platform "$PLATFORM" \
  -e RIPCLONE_SERVER_TOKEN=musl-smoke \
  -v "$bindir:/b:ro" \
  -v "$(dirname "$test_bin"):/d:ro" \
  alpine:latest sh -euc '
    echo "--- $(grep PRETTY_NAME /etc/os-release | cut -d= -f2-), base packages only"

    # Every binary must at least load and execute: a missing symbol or a missing
    # shared library shows up right here.
    /b/ripclone --version
    /b/ripclone-server --version
    /b/ripclone-worker --help >/dev/null && echo "ripclone-worker: ok"
    /b/git-remote-ripclone 2>&1 | head -1 >/dev/null && echo "git-remote-ripclone: ok"

    # ...and one of them must do real work: boot the server (tokio, rustls,
    # sqlite, the local storage backend) and answer live requests.
    /b/ripclone-server --port 8123 --cas-dir /tmp/cas --repo-root /tmp/repos >/tmp/srv.log 2>&1 &
    srv=$!
    for _ in $(seq 1 80); do
      wget -qO- http://127.0.0.1:8123/healthz >/dev/null 2>&1 && break
      sleep 0.25
    done
    health="$(wget -qO- http://127.0.0.1:8123/healthz || true)"
    version="$(wget -qO- http://127.0.0.1:8123/v1/version || true)"
    kill "$srv" 2>/dev/null || true
    [ -n "$health" ] || { echo "server never became healthy:"; cat /tmp/srv.log; exit 1; }
    echo "GET /healthz    -> $health"
    echo "GET /v1/version -> $version"

    # The musl-only code paths: the hand-written statx layout and the mimalloc
    # global allocator. Only these two are run here — the rest of the library
    # suite is target-independent and already runs on the glibc `test` job, and a
    # good part of it shells out to `git`, which a clean Alpine does not have.
    echo "--- musl-only library tests"
    /d/'"$(basename "$test_bin")"' --exact \
      statx_compat::tests::statx_fields_match_stat_for_a_regular_file \
      statx_compat::tests::statx_fields_match_stat_for_a_directory \
      statx_compat::tests::index_stat_from_statx_matches_index_stat_from_metadata \
      musl_global_allocator::rust_allocations_are_served_by_mimalloc
  '

echo "==> musl smoke passed ($TARGET on alpine:latest)"
