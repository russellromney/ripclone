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

The next batch of work is about making the client fast, predictable, and globally consistent, and giving users knobs that match their workload.

### 1. Unified async download/write pipeline

The client should not wait for the metadata chunk before it starts downloading data. The manifest is tiny and already lists every chunk hash and length.

Target sequence:

1. Resolve ref → get manifest hash + signed URLs.
2. Fetch manifest.
3. Immediately enqueue into a bounded async fetch pool:
   - metadata chunk (highest priority),
   - head-blobs chunks,
   - archive chunks.
4. On metadata arrival: decode it, write skeleton pack/idx and prebuilt `.git/index`, and spawn archive extraction workers.
5. On each archive chunk arrival: push it to the extractor, which decompresses frames and writes files as later chunks still download.
6. On each head-blobs chunk arrival: write it to the correct byte offset in `pack-{hash}.pack` (streamed, not buffered in a `Vec<u8>`). When all chunks + idx are present, the pack is valid.
7. Materialize the working tree:
   - archive-first modes: files are already written as chunks arrive.
   - direct-install mode: run `git checkout-index` as soon as the head-blobs pack is complete.

This removes the current stalls: metadata-before-data, head-blobs buffered in RAM, and checkout-after-everything.

### 2. User-facing clone modes

Replace the hidden `RIPCLONE_EXTRACT_ARCHIVE=1` flag with explicit modes that match what users expect from git:

| Mode | Downloads | Result | Best for |
|---|---|---|---|
| `fast` | metadata + archive chunks | Working tree present; HEAD blobs **not** in `.git/objects`. | Agents that edit and commit; rarely run `git diff`/`git show`. |
| `full` | metadata + head-blobs pack/idx | Complete `.git`; all git commands work. | Agents that need `git diff`, `git show`, `git log -p`, etc. |
| `hybrid` | metadata + archive chunks + head-blobs chunks (concurrent) | Working tree ready instantly; head-blobs pack written in the background so the repo becomes complete seconds later. | **Default.** Best initial experience; completeness follows. |
| `skeleton` | metadata + skeleton pack/idx | `.git` only, no working tree. | Special-purpose, already supported. |

`fast`/`full`/`hybrid` are surfaced as `--mode <name>` and `RIPCLONE_MODE`. The CLI default should be `hybrid`.

### 3. Edge warmth with Tigris

Tigris Global buckets already cache objects near the requester, but the first request from a new region is a cold-cache miss. We keep Tigris and warm the cache instead of adding a separate CDN.

Two complementary tactics:

1. **Fly-region cache warmers (first)**
   - Deploy a tiny `ripclone-warmer` app in multiple Fly regions.
   - After each sync/build, each warmer fetches the latest commit’s chunks for the default branch.
   - Local data is deleted immediately; the goal is only to pull Tigris objects into that region’s cache.
   - Cheap, simple, and works with the existing Global bucket.

2. **Tigris multi-region bucket for tip commits (optional later)**
   - Keep a Tigris Multi-region bucket (e.g., `USA`) for the latest commit of each branch.
   - Write new artifacts to both Global and Multi-region; GC old commits out of Multi-region after a configurable window.
   - Signed URLs point at the Multi-region bucket for hot commits.
   - More expensive but guarantees eager replication and strong consistency across the geography.

Also: evaluate switching the deployed server from the region-stamped `fly.storage.tigris.dev` endpoint to the canonical `https://t3.storage.dev` endpoint.

### 4. Per-phase benchmark breakdown

Add a benchmark mode that reports time spent in each phase, not just end-to-end time:

- `resolve_ms`
- `manifest_ms`
- `metadata_ms`
- `head_blobs_download_ms`
- `archive_download_ms`
- `write_ms` / `checkout_ms`
- `total_ms`

This separates network wins (Tigris warming) from code wins (pipeline overlap, streaming writes).

### 5. Production hardening still missing

- **Prometheus `/metrics`**: replace the JSON snapshot with Prometheus text format.
- **Real `/readyz`**: check storage and ref-store health instead of always returning `ok`.
- **JWT auth flow**: `ripclone auth login` that exchanges a secret for a short-lived JWT, plus `/v1/auth/refresh`.
- **GitHub App path**: support installation tokens in addition to the env-var PAT.
- **CI and integration tests**: GitHub Actions workflow with `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`, Docker build, and an end-to-end clone test against a fixture repo.
- **Fuzz/property tests**: random manifests should either produce the expected tree or return `Err`, never a silently short tree.

### 6. Clonepack deltas / compaction (future)

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
