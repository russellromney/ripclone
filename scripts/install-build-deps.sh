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
  curl
