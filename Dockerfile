# Build stage
FROM rust:1.96.0-slim AS builder

WORKDIR /app

# Install git and fuse headers for build-time git operations and fuser.
RUN apt-get update && apt-get install -y --no-install-recommends git pkg-config libssl-dev libfuse-dev libgit2-dev protobuf-compiler cmake build-essential && rm -rf /var/lib/apt/lists/*

# Copy manifests and build script first for layer caching.
COPY rust/Cargo.toml rust/Cargo.lock rust/build.rs ./
COPY rust/proto ./proto
COPY rust/src ./src

RUN cargo build --release

# Runtime stage
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates curl libfuse2 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/ripclone /usr/local/bin/ripclone
COPY --from=builder /app/target/release/ripclone-server /usr/local/bin/ripclone-server

ENV RUST_LOG=info

EXPOSE 8000

CMD ["ripclone-server"]
