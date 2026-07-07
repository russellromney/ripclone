# Build options

## Compiling the crate

By default the Rust crate uses `zlib-ng` for faster pack compression. On platforms without cmake you can build with the stock zlib instead:

```bash
cd rust
cargo build --release --no-default-features
```

## Client tuning

Environment variables for tuning clone performance:

- `RIPCLONE_FETCH_MAX_ATTEMPTS` / `RIPCLONE_FETCH_BACKOFF_MS` — retry budget and base backoff for transient download failures (defaults 3 and 100).
- `RIPCLONE_IO_URING` — the worktree writer uses io_uring by default on Linux; set `=0` to force the POSIX writer.
- `RIPCLONE_MODE` — default clone mode (`editable` or `files`) when `--mode` is omitted.
- `RIPCLONE_CACHE_DIR` / `RIPCLONE_NO_CACHE` — opt in to (or force off) a local artifact cache; off by default.

## fsync durability

A clone is crash-consistent by default: files are written to a temp directory and atomically renamed into place, so a crash mid-clone never leaves a half-written tree at the target path. It does **not** force an fsync durability barrier before reporting success — the same durability model as `git checkout`, and the extra fsyncs add latency on the clone's critical path.

If a crash immediately after the clone must not leave a torn tree that `git status` would call clean, set `RIPCLONE_FSYNC=1` (or `true`). The client then recursively fsyncs every file and directory in the materialized tree before it reports success. Off by default.

## Server-side backends

Server-side backends are configured through environment variables: storage and retention (`RIPCLONE_S3_*`, `RIPCLONE_RETENTION_*`, `RIPCLONE_REMOTE_GC_*`), the metadata store (`RIPCLONE_METADATA*`), and the build queue / farm-out workers (`RIPCLONE_QUEUE*`). See [`BACKENDS.md`](BACKENDS.md) and [`CHANGELOG.md`](CHANGELOG.md) for the full list.
</content>
