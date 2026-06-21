# Adversarial review: archive-first clone implementation

This doc records the findings from an adversarial pass over the archive-first
clone path (`rust/src/archive.rs`, `rust/src/manifest.rs`, `rust/src/extract.rs`,
`ripclone build-archive`, `ripclone extract-archive`, and the skeleton clone
flow). All findings listed here have been fixed.

## Findings and fixes

### 1. Per-blob builder was far too slow
**Finding:** The first archive builder shell'd out to `git cat-file -p` once per
file. For `oven-sh/bun` (~15k files) this would have been minutes of subprocess
overhead.

**Fix:** Replaced with a single `git archive --format=tar` stream. Build time on
bun dropped to ~3 s.

### 2. `pax_global_header` was treated as a real file
**Finding:** `git archive` emits a `pax_global_header` tar entry. The builder
was not filtering by entry type, so it could have been written into the manifest
as a tracked file.

**Fix:** Skip any tar entry whose type is not `Regular` or `Symlink`.

### 3. Symlinks were written as regular files
**Finding:** The builder read symlink entries with the same path/content path as
regular files, so symlinks would have been materialized as files containing the
target path.

**Fix:** Detect `tar::EntryType::Symlink`, store the link target as the blob
content, and set mode `0o120000`. The extractor recreates the symlink on Unix.

### 4. Re-extraction failed on broken symlinks
**Finding:** The extractor only unlinked an existing path when `target.exists()`
was true. `exists()` follows symlinks, so a broken symlink was never removed and
`symlink()` failed with `EEXIST`.

**Fix:** Always call `std::fs::remove_file(&target).ok();` before creating a
symlink or writing a regular file.

### 5. Path traversal was not rejected
**Finding:** A malicious manifest could contain absolute paths or `..`
components and write outside the target directory.

**Fix:** Added an explicit check in `write_entry` that refuses absolute paths
and any path containing `..`.

### 6. `skip-worktree` clearing broke for large repos
**Finding:** `git update-index --no-skip-worktree --stdin` was called once with
all ~15k paths. On some systems this exceeded pipe buffer limits or caused a
broken pipe.

**Fix:** Chunk the path list into 1,000-entry batches.

### 7. Hard links were accepted unnecessarily
**Finding:** The builder accepted `tar::EntryType::Link`. `git archive` does not
produce hard links for normal tree entries, and accepting them could mask
duplicates or edge cases.

**Fix:** Removed `Link` from accepted entry types.

### 8. `core.fileMode` was set to `false`
**Finding:** Both the server-side snapshot builder and the client-side skeleton
clone set `core.fileMode false`, so git would not detect mode changes. This is
unexpected for a normal clone.

**Fix:** Removed `core.fileMode false` from both `rust/src/snapshot.rs` and
`rust/src/client.rs`. Skeleton clones now keep the platform default (`true` on
Unix), and `git status` correctly reports chmod changes.

### 9. Skeleton clone had no `origin` remote
**Finding:** After `ripclone clone --skeleton`, `git remote -v` was empty. A
normal clone has `origin` pointing at the upstream repo, so fetch/push and IDE
integrations were broken.

**Fix:** `skeleton_clone` now adds `origin https://github.com/<owner>/<repo>.git`.

### 10. Manifest format had no unit tests
**Finding:** The binary manifest serialization/deserialization and archive
verification were only exercised indirectly by the E2E script.

**Fix:** Added Rust unit tests for manifest roundtrip, happy-path archive
verification, and SHA-1 mismatch detection.

### 11. E2E coverage was thin
**Finding:** The original E2E only checked `git status` clean and one symlink.

**Fix:** Expanded `scripts/e2e_archive.sh` to:
- Compare the extracted tree against an independent `git archive` extraction.
- Verify all tracked files are present.
- Verify every symlink target matches `HEAD:<path>`.
- Verify executable bits for all `100755`/`100644` files.
- Verify `git log`, `git diff`, and `core.fileMode=true` status clean.
- Verify `origin` remote is configured.
- Verify re-extraction is idempotent.
- Verify corrupted archives and missing manifests are rejected.

### 12. Custom zstd dictionaries did not help `oven-sh/bun`
**Finding:** The user suggested repo-specific zstd dictionaries could improve
compression. Training on the full HEAD working tree and applying the dictionary
to the archive was implemented.

**Result:** On `oven-sh/bun`, a 1 MB dictionary gave a ~61.9 MB archive vs.
~62.3 MB with no dictionary, but the dictionary itself is 1 MB, so the total
transfer is larger. A 100 KB dictionary was also a net loss. The working-tree
files are diverse enough that per-frame zstd context already captures the
redundancy.

**Fix/verdict:** Dictionary support is wired up (`train-dictionary`,
`--dictionary`) but not enabled by default. It remains available for repos with
more repetitive small files.

### 13. Sequential file I/O limited extraction speed
**Finding:** Writing ~15k files one at a time was the main bottleneck after
parallel decompression.

**Fix:** Two passes:
- macOS / general: parallel POSIX file writes + single-syscall file creation
  (mode set in `open()`). Extraction on bun dropped from ~4 s to ~2.2 s.
- Linux: added an opt-in io_uring worktree writer. The first implementation was
  slower than POSIX for Bun's many-small-files workload on Fly volumes, so the
  default remains POSIX and io_uring is currently an experimental fast path
  enabled with `RIPCLONE_IO_URING=1` or probed with `RIPCLONE_IO_URING=auto`.

## Residual limitations

- The extractor reads the whole archive into memory before decompressing. For
  multi-gigabyte archives this should be replaced with frame-at-a-time reads or
  mmap. Bun's 60 MB archive is fine for now.
- Manifest paths are stored as UTF-8. Repos with non-UTF-8 paths will fail to
  build. This matches Git's de-facto convention but is a real limitation.
- Submodules and Git LFS are not supported (documented in
  `docs/ADVERSARIAL_REVIEW.md`).
- The sidecar/FUSE path remains in the codebase as a fallback but is not the
  focus of the archive-first work.
