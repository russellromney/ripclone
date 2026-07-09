#!/usr/bin/env bash
# Single definition of the system build dependencies, shared by every CI job and
# usable locally. Uses sudo only when not already root (CI runners are non-root;
# containers are root).
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  SUDO="sudo"
else
  SUDO=""
fi

$SUDO apt-get update
$SUDO apt-get install -y --no-install-recommends \
  protobuf-compiler \
  cmake \
  build-essential \
  pkg-config \
  libssl-dev \
  libfuse-dev \
  libgit2-dev \
  git \
  curl \
  mold

# rust/.cargo/config.toml points x86_64-unknown-linux-gnu builds at mold (see
# that file for why). ~50 integration test binaries link on every Linux job;
# mold cuts that 3-5x and rust-cache can't touch linking. This has no effect on
# macOS or the musl release target (different target triples).
