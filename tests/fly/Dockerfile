# syntax=docker/dockerfile:1
# Build stage
FROM rust:1.96.0-slim AS builder

WORKDIR /app

# Install git and fuse headers for build-time git operations and fuser.
RUN apt-get update && apt-get install -y --no-install-recommends git pkg-config libssl-dev libfuse-dev libgit2-dev protobuf-compiler cmake build-essential && rm -rf /var/lib/apt/lists/*

COPY rust/Cargo.toml rust/Cargo.lock rust/build.rs ./
COPY rust/proto ./proto
COPY rust/src ./src

# Cache the cargo registry/git and the target dir across builds (BuildKit cache
# mounts, persisted by Depot). A code-only change then recompiles just the
# ripclone crates instead of all ~400 dependencies. The target dir is a cache
# mount (not an image layer), so the binaries are copied out to /out in the same
# RUN for the runtime stage to pick up.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    mkdir -p /out && \
    cp target/release/ripclone target/release/ripclone-server \
       target/release/ripclone-worker /out/

# Runtime stage
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates curl libfuse2 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/ripclone /usr/local/bin/ripclone
COPY --from=builder /out/ripclone-server /usr/local/bin/ripclone-server
# The standalone build worker. The same image runs the server (default CMD) or a
# worker (override the CMD in the worker process group). See ripclone-worker.rs.
COPY --from=builder /out/ripclone-worker /usr/local/bin/ripclone-worker

ENV RUST_LOG=info

EXPOSE 8000

CMD ["ripclone-server", "--cas-dir", "/data/cache", "--repo-root", "/data/repos"]
