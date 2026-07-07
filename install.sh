#!/bin/sh
# ripclone installer.
#
#   curl -fsSL https://github.com/russellromney/ripclone/releases/latest/download/install.sh | sh
#
# Env:
#   RIPCLONE_VERSION  pin a tag (e.g. v0.1.0); default: latest
#   RIPCLONE_BIN_DIR  install dir; default: $HOME/.local/bin
set -eu

REPO="russellromney/ripclone"
VERSION="${RIPCLONE_VERSION:-latest}"
BIN_DIR="${RIPCLONE_BIN_DIR:-$HOME/.local/bin}"

os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux) os_name="linux" ;;
  Darwin) os_name="macos" ;;
  *) echo "ripclone: unsupported OS '$os'" >&2; exit 1 ;;
esac
case "$arch" in
  x86_64 | amd64) arch_name="x86_64" ;;
  arm64 | aarch64) arch_name="arm64" ;;
  *) echo "ripclone: unsupported architecture '$arch'" >&2; exit 1 ;;
esac
asset="ripclone-${os_name}-${arch_name}.tar.gz"

if [ "$VERSION" = "latest" ]; then
  base="https://github.com/$REPO/releases/latest/download"
else
  base="https://github.com/$REPO/releases/download/$VERSION"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "ripclone: downloading $asset ($VERSION)"
curl -fsSL "$base/$asset" -o "$tmp/$asset"
curl -fsSL "$base/$asset.sha256" -o "$tmp/$asset.sha256"

# Verify the checksum (the published .sha256 may include a path; compare hashes).
echo "ripclone: verifying checksum"
expected="$(awk '{print $1}' "$tmp/$asset.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
else
  actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
fi
if [ "$expected" != "$actual" ]; then
  echo "ripclone: checksum mismatch (expected $expected, got $actual)" >&2
  exit 1
fi

tar -xzf "$tmp/$asset" -C "$tmp"
mkdir -p "$BIN_DIR"
# The tarball contains a top-level dir; install the binaries from it.
src="$(find "$tmp" -type f -name ripclone -perm -u+x | head -n1)"
src_dir="$(dirname "$src")"
for b in ripclone ripclone-server ripclone-worker git-remote-ripclone; do
  if [ -f "$src_dir/$b" ]; then
    install -m 0755 "$src_dir/$b" "$BIN_DIR/$b"
  fi
done

echo "ripclone: installed to $BIN_DIR"
case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) echo "ripclone: add $BIN_DIR to your PATH, e.g.  export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

# Smoke test: the binaries are statically linked against their C deps (zstd,
# zlib-ng) and use pure-Rust git + TLS, so they should run on a minimal image
# with only libc present. If this fails it is a real problem — surface it
# instead of hiding it (no `|| true`). The most likely cause on an old distro is
# a host glibc older than the one the release was built against.
if ! "$BIN_DIR/ripclone" --version; then
  echo "ripclone: the installed binary failed to run." >&2
  echo "ripclone: this usually means the host C library is too old for this build." >&2
  echo "ripclone: 'ldd --version' to check your glibc, or build from source with 'cargo install ripclone --locked'." >&2
  exit 1
fi
