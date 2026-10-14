# syntax=docker/dockerfile:1
# CI assembles this runtime image from already-built binaries. It deliberately
# includes system Git because the server uses it for upstream mirror
# and artifact build operations.
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates curl libfuse2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ci-bins/ripclone /usr/local/bin/ripclone
COPY ci-bins/ripclone-server /usr/local/bin/ripclone-server

ENV RUST_LOG=info

EXPOSE 8000

CMD ["ripclone-server"]
