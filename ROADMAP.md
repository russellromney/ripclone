# ripclone roadmap

> Goal: the fastest practical way to clone a GitHub repo and be ready to work on it (`git status`, `git diff`, `git edit`, `git commit`) without rebuilding git or leaving GitHub.
>
> This repo is the **headless open-source backend**: the Rust server, the CLI, the archive format, and the GitHub Actions trigger. Billing, workspaces, and the web UI live in the separate `ripclone-cloud` project.

## What already works

See `CHANGELOG.md` for the full list. The important baseline for current work:

- **Clonepack format**: top-level `ClonepackManifest` + `MetadataChunk` + content-addressed archive chunks + separate head-blobs chunks.
- **Signed URLs**: ref response returns presigned Tigris URLs for the manifest, metadata chunk, archive chunks, and head-blobs chunks.
- **Shared ref store**: `RefStore` trait with file-backed, S3-backed, and caching implementations.
- **Async builds**: `/v1/build` accepts an OIDC token from GitHub Actions and enqueues the build.
- **Security**: the critical findings from the adversarial review are fixed (artifact-id validation, atomic CAS writes, hash verification, path safety, mode validation, rate limiting keyed by IP).
- **Client paths**: direct-install (`git checkout-index` using a head-blobs pack) and archive extraction (zstd frames written directly) are both implemented and A/B tested.
- **Tests**: 27 unit tests, but no CI, no integration-test suite, and no fuzz/property tests yet.

## Current plan

The next batch of work is about making the client fast, predictable, and globally consistent, while keeping the default behavior identical to what users expect from `git clone`.

### 1. Archive chunks vs. head-blobs pack

Both represent the **same** depth: the `HEAD` commit only. They are not different history depths.

- **Head-blobs pack** = every blob reachable from `HEAD`, stored as a git packfile. This is what makes `git diff`, `git show`, and `git checkout-index` work.
- **Archive chunks** = the same blob bytes, grouped and zstd-compressed for fast parallel file materialization.

For branches: each branch gets its own clonepack. For history beyond `HEAD`: not supported yet; that is the future “clonepack deltas” item.

Because the two representations contain the same data, fetching both is redundant. The modes below decide which one to use (or whether to use both in parallel for speed).

### 2. Unified async download/write pipeline

The client should not wait for the metadata chunk before it starts downloading data. The manifest is tiny and already lists every chunk hash and length.

Target sequence:

1. Resolve ref → get manifest hash + signed URLs.
2. Fetch manifest.
3. Immediately enqueue into a bounded async fetch pool:
   - metadata chunk (highest priority),
   - head-blobs chunks (for `full`/`hybrid` modes),
   - archive chunks (for `fast`/`hybrid` modes).
4. On metadata arrival: decode it, write skeleton pack/idx and prebuilt `.git/index`, and spawn archive extraction workers.
5. On each archive chunk arrival: push it to the extractor, which decompresses frames and writes files as later chunks still download.
6. On each head-blobs chunk arrival: write it to the correct byte offset in the pack file. Compute the pack SHA-256 incrementally so the filename is known as soon as the last byte lands. Do not collect the whole pack into a `Vec<u8>`.
7. Materialize the working tree:
   - `full` mode: run `git checkout-index` as soon as the head-blobs pack + idx are complete.
   - `fast`/`hybrid` modes: files are already written as archive chunks arrive.
8. The CLI only returns after the working tree **and** the expected `.git` depth for the chosen mode are ready.

Buffering rule for archive chunks that arrive before metadata: spill them to a bounded temp directory keyed by chunk index. If memory pressure is low, keep the first few in RAM; otherwise write all early chunks to disk. Clean up the spill directory on success or failure.

Install rule: write into a temp directory beside the target and atomic-rename on success. On failure, delete the temp directory.

Retry rule: every chunk download is retried with exponential backoff. Because chunks are content-addressed, a retry cannot corrupt the repo.

### 3. User-facing clone modes

Replace the hidden `RIPCLONE_EXTRACT_ARCHIVE=1` flag with explicit modes. The default must behave like `git clone --depth=1`: the repo is complete and ready to use when the command returns.

| Mode | Downloads | Result | Best for |
|---|---|---|---|
| `full` *(default)* | metadata + head-blobs pack/idx | Complete `.git`; `git status`, `git diff`, `git show`, `git log -p` all work. | **Default.** Matches normal git expectations. |
| `fast` | metadata + archive chunks | Working tree present; HEAD blobs **not** in `.git/objects`. `git status`/`git add`/`git commit` work; `git diff`/`git show` do not. | Opt-in speed mode for agents that only edit and commit. |
| `hybrid` | metadata + archive chunks + head-blobs chunks (concurrent) | Archive materializes files while head-blobs download in parallel; CLI blocks until both are done. | Opt-in when bandwidth allows both streams and checkout-index is slow. |
| `skeleton` | metadata + skeleton pack/idx | `.git` only, no working tree. | Special-purpose, already supported. |
| `lazy` *(future)* | metadata + archive chunks first; head-blobs fetched by a daemon afterwards | Fast startup; repo becomes complete in the background. | Future mode for interactive use. |

Mode is surfaced as `--mode <name>` and `RIPCLONE_MODE`.

### 4. Edge warmth with Tigris

Tigris Global buckets already cache objects near the requester, but the first request from a new region is a cold-cache miss. We keep Tigris and warm the cache instead of adding a separate CDN.

**Immediate:** evaluate switching the deployed server from the region-stamped `fly.storage.tigris.dev` endpoint to the canonical `https://t3.storage.dev` endpoint.

**Future optimizations:**

1. **Fly-region cache warmers**
   - Deploy a tiny `ripclone-warmer` daemon in multiple Fly regions.
   - After each sync/build, fetch the latest commit’s chunks for the default branch (download only; do not materialize a working tree).
   - Discard bytes locally; the goal is only to pull Tigris objects into that region’s cache.
   - Warmers authenticate the same way as clients.

2. **Tigris multi-region bucket for tip commits**
   - For the latest commit of each branch, use a Tigris Multi-region bucket so data is already in every region.
   - Older commits stay in the cheaper Global bucket.
   - This is a paid-feature tier, not the immediately important path.

### 5. Per-phase benchmark breakdown

Add a benchmark mode that reports time spent in each phase, with precise definitions:

- `resolve_ms`: ref request sent to ref response received.
- `manifest_ms`: manifest downloaded + decoded.
- `metadata_ms`: metadata chunk downloaded + decoded + skeleton/index written.
- `head_blobs_download_ms`: first head-blobs chunk request sent to last head-blobs byte received.
- `archive_download_ms`: first archive chunk request sent to last archive byte received.
- `write_ms`: first working-tree byte written to last file closed.
- `checkout_ms`: `git checkout-index` duration, or extractor worker duration for archive modes.
- `total_ms`: wall clock from CLI start to exit.

Also report bytes per phase and per-chunk throughput. This separates network wins from code wins.

### 6. Production hardening still missing

- **Prometheus `/metrics`**: replace the JSON snapshot with Prometheus text format.
- **Real `/readyz`**: check storage and ref-store health instead of always returning `ok`.
- **JWT auth flow**: `ripclone auth login` that exchanges a secret for a short-lived JWT, plus `/v1/auth/refresh`.
- **GitHub App path**: support installation tokens in addition to the env-var PAT.
- **CI and integration tests**: GitHub Actions workflow with `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`, Docker build, and an end-to-end clone test against a fixture repo.
- **Extend existing e2e scripts**:
  - `scripts/e2e_clonepack.sh` already tests default vs. archive extraction for a public fixture; extend it to test `--mode=full`, `--mode=fast`, and `--mode=hybrid` and verify `git diff`/`git show` per mode.
  - `scripts/e2e_archive.sh` already verifies content, symlinks, executable bits, and edit detection for direct-install; reuse it for all modes.
- **Fuzz/property tests**: random manifests should either produce the expected tree or return `Err`, never a silently short tree.

### 7. Clonepack deltas / compaction (future)

Once warm full clones are fast and predictable, move from full clonepacks per commit to append-only delta chunks for recent commits, with background compaction. This is on the roadmap but not the current focus.

## Storage model

- **Tigris Global object storage is the source of truth.** No separate CDN.
- **Local NVMe disk on the ripclone server is a hot cache** for recently built and recently accessed artifacts.
- **Cache warmers in Fly regions pull objects into Tigris edge caches** after each build.
- Retention evicts local-only objects only after confirming they exist in Tigris.

## Success metrics

| Metric | Target |
|---|---|
| Warm full clone of `oven-sh/bun` from a Fly client in the same region as the bucket | < 3 s |
| Warm full clone of `oven-sh/bun` from a laptop after regional cache is warm | < 5 s |
| Client setup + disk write time (after chunks land) | < 500 ms |
| `git status` after clone | clean |
| `git diff <file>` after editing | works immediately in `full`/`hybrid` modes |
| Per-phase benchmark | downloadable for every clone |

## Notes

- See `CHANGELOG.md` for completed work.
- See `docs/ADVERSARIAL_REVIEW_2026-06-18.md` for the security review that drove recent hardening.
