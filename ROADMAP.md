# ripclone roadmap

> Goal: the fastest practical way to clone a GitHub repo and be ready to work on it (`git status`, `git diff`, `git edit`, `git commit`) without rebuilding git or leaving GitHub.
>
> This repo is the **headless open-source backend**: the Rust server, the CLI, the archive format, and the GitHub Actions trigger. Billing, workspaces, and the web UI live in the separate `ripclone-cloud` project.

## Clone modes & pack architecture (active plan)

We support **exactly three** clone modes — no arbitrary `--depth N` (it's finicky and almost nobody uses it; people pick depth=1 or full):

- **`editable --depth 1`** (default): HEAD snapshot, full object DB for HEAD. The agent/CI hot path.
- **`editable --depth 0` (full)**: HEAD + all history.
- **`files`**: worktree only, no git objects (zstd archive) — fastest CI path.

This collapses the server to **two pack buckets** (no range/geometric depth buckets):

- **HEAD-closure packs** — every current blob + HEAD commit/trees. **Undeltified**, many small (~2 MB) packs so the client downloads in parallel and hand-parses for the working tree. Used by *every* depth. *(done)*
- **History packs** — full history minus HEAD closure (ancestor commits/trees, old blob versions). **Deltified** (undeltified history is multi-GB), fewer/larger packs. Only **installed** for the object DB; the client never hand-parses or materializes the worktree from them (git reads them, resolving deltas). *(history bucket built; needs deltify flip)*

depth=1 clonepack lists HEAD-closure packs; full lists HEAD + history. The depth=1 set is a **content-addressed subset** of the full set — no separate HEAD pack is built.

**MIDX (the "many packs but git stays fast" lever):** the server pre-builds a `multi-pack-index` per variant (head-MIDX, full-MIDX) in a temp pack dir using the client's deterministic `pack-<trailer>.{pack,idx}` filenames, stores it as a content-addressed artifact, and the client drops it into `.git/objects/pack/`. Object lookups become O(log) across all packs regardless of count — so many small packs (great download parallelism) coexist with fast `git status`/`diff`/`log`. Cheap to build (indexes existing idx files; no pack rewrite). Optionally `--bitmap`.

**Full-clone correctness:** a "full" clone from a shallow mirror is bounded; it must either come from an **unshallow mirror** or carry a `.git/shallow` boundary marker, or `git fsck`/traversal breaks at the boundary (observed bug).

### Immediate next
1. **Deltify the history bucket** — kills the full-history size blowup (client unchanged; it only hand-parses HEAD packs).
2. **Pre-generate MIDX** (head + full) and install it on the client.
3. **Unshallow mirror + `.git/shallow`** so depth=0 is a true, fsck-clean full clone.

### Deferred
- **LSM incremental build** — don't rebuild full history every sync (append an L0 pack at HEAD, compact older into immutable range packs; LSM levels). Optimization only.
- **io_uring** durable-disk worktree writes — orthogonal speedup so `--temp` (ephemeral tmpfs) isn't required for fast durable clones.
- **Blobless partial clone** (`--filter=blob:none` via a git promisor pointing at the server) — distinct from `files`; a separate project for huge monorepos.

## What already works

See `CHANGELOG.md` for the full list. The important baseline for current work:

- **Clonepack format**: top-level `ClonepackManifest` + content-addressed depth-pack chunks + optional files artifact.
- **Signed URLs**: ref response returns presigned object-storage URLs for the manifest, pack chunks, and optional files artifact.
- **Shared ref store**: `RefStore` trait with file-backed, S3-backed, and caching implementations.
- **Async builds**: `/v1/build` accepts an OIDC token from GitHub Actions and enqueues the build.
- **Security**: artifact-id validation, atomic CAS writes, hash verification, path safety, mode validation, and IP-keyed rate limiting are implemented.
- **Client paths**: pack install + parallel worktree extraction, and optional zstd files-artifact extraction.
- **Tests**: unit tests exist, but CI and a full integration-test suite are still needed.

## Current plan

The next batch of work is about making the client fast, predictable, and globally consistent, while keeping the default behavior identical to what users expect from `git clone`.

### 1. Clonepack artifacts

A clonepack is now built from two optional artifacts:

- **Depth pack** = a git packfile containing the commit(s), tree(s), and blob(s) for a requested history depth. This is what makes `git diff`, `git show`, `git checkout`, and edit/push work.
- **Files artifact** = the working tree files as zstd-compressed raw bytes. This is the fastest path when you only need files (CI / build-only).

The server builds one depth pack per requested depth. `--depth 1` includes the HEAD commit, its tree, and every blob reachable from HEAD. `--depth 0` includes all history.

The files artifact is optional. When the server builds it, `--mode files` can skip the git pack entirely.

### 2. History depths

- The CLI clone command takes `--depth N` where `N` is the number of commits, and `0` means unlimited/full history.
- The server builds a single git pack containing the objects for that depth using `git rev-list --max-count=$DEPTH --objects HEAD | git pack-objects --window=0`.
- The pack is split into content-addressed chunks. The manifest lists the chunks (and their signed URLs) plus the pack idx.
- The client downloads the chunks, concatenates them into a valid git pack, installs it, and extracts the working tree.

Remaining work:
- **Repo/branch-specific depth configuration** (see section below).

### 2a. Repo/branch-specific configuration (planned)

Right now the server hard-codes two clonepack variants (`shallow` = depth 1, `full` = unlimited). Users and orgs should be able to configure this per repo/branch without recompiling.

Proposed design:

- Add a `RepoConfig` store backed by the same storage as the ref store (file for local dev, S3 for production).
- Key by `owner/repo[/branch]`, with branch-level entries overriding repo-level entries.
- Config fields:
  - `clonepack_depths: Vec<DepthSpec>` where `DepthSpec` is `{ name: "shallow", depth: 1 }`, `{ name: "full", depth: null }`, or arbitrary depths like `{ name: "recent", depth: 50 }`.
  - `compression_level`, `dictionary_id`, `hot_files`, `archive_chunk_size`, `head_blobs_chunk_size`.
  - `enabled_modes: ["editable", "files", "skeleton"]` if a repo wants to disable some paths.
- On sync/build, the server reads the config for the repo/branch and builds exactly the requested set of clonepacks.
- The ref endpoint accepts `?clonepack=<name>`; the name maps to one of the configured depths.
- Default config (when none is stored) produces `shallow` and `full` exactly like today, so behavior is unchanged for unconfigured repos.
- A simple admin CLI or API endpoint (`POST /v1/admin/config/{owner}/{repo}`) can write the config; eventually this is exposed in the ripclone-cloud UI.

### 3. Unified async download/write pipeline ✅

Implemented in `rust/src/client.rs`, `rust/src/extract.rs`, and `rust/src/pack.rs`. See `CHANGELOG.md` for details.

Remaining future improvements:
- Retry each chunk download with exponential backoff.
- Delete the temp install directory on failure.

### 4. User-facing clone modes ✅

Implemented as `--mode editable|files|skeleton` and `RIPCLONE_MODE`. `rcgit clone` is always `editable`.

- `editable` (default) downloads the depth pack for `--depth N` and extracts a real git repo.
- `files` downloads only the optional zstd files artifact; fastest for CI but not a usable git repo.
- `skeleton` installs only `.git` metadata with no working tree or blobs.

The old `full`, `fast`, `hybrid`, and `lazy` modes are removed.

### 5. Edge warmth with Tigris

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

### 6. Per-phase benchmark breakdown ✅

Implemented as `--bench` / `RIPCLONE_BENCH=1` with a JSON report covering all defined phases. See `CHANGELOG.md` for details.

### 7. Production hardening still missing

- **Prometheus `/metrics`**: replace the JSON snapshot with Prometheus text format.
- **Real `/readyz`**: check storage and ref-store health instead of always returning `ok`.
- **JWT auth flow**: `ripclone auth login` that exchanges a secret for a short-lived JWT, plus `/v1/auth/refresh`.
- **GitHub App path**: support installation tokens in addition to the env-var PAT.
- **CI and integration tests**: GitHub Actions workflow with `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`, Docker build, and an end-to-end clone test against a fixture repo.
- **Extend existing e2e scripts**:
  - `scripts/e2e_clonepack.sh` should test `--mode=editable`, `--mode=files`, and `--mode=skeleton` and verify `git diff`/`git show` in `editable` mode.
  - `scripts/e2e_archive.sh` should verify content, symlinks, executable bits, and edit detection for `editable` mode; reuse it for `files` mode where applicable.
- **Fuzz/property tests**: random manifests should either produce the expected tree or return `Err`, never a silently short tree.

### 8. Incremental syncs and cross-branch sharing

The long-term storage model is an append-only, chunked pack per branch with a mutable tail:

- Normal syncs append new objects to the tail chunk and seal it at the size limit.
- Branches share immutable history chunks because the chunk store is content-addressed.
- History rewrite (force-push, filter-repo) is detected by checking whether the new HEAD is a descendant of the previous HEAD. When rewrite happens, the server does a full rebuild.

The current implementation rebuilds the entire depth pack on each sync; the LSM tail model is future work once the single-pack path is proven.

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
| `git diff <file>` after editing | works immediately in `editable` mode |
| Per-phase benchmark | downloadable for every clone |

## Notes

- See `CHANGELOG.md` for completed work.

## Distribution (future)

Make ripclone installable through the two most common package managers.

### `cargo install ripclone`

Add the required metadata to `rust/Cargo.toml` (description, `license = "Elastic-2.0"`, repository, readme) and publish the crate to crates.io. The crate already exposes a lib plus four binaries (`ripclone`, `ripclone-server`, `ripclone-proxy`, `git-remote-ripclone`), so `cargo install` gives users everything.

### `pip install ripclone`

Use [maturin](https://www.maturin.rs/) to build wheels that ship the Rust executables. The simplest first version uses `bindings = "bin"` in a root `pyproject.toml` pointing at `rust/Cargo.toml`; the resulting wheel installs the binaries on `PATH`. Later we can add PyO3 bindings if we want a native Python API.

Build targets to start:
- macOS aarch64 and x86_64
- Linux x86_64 (manylinux)
- Windows x86_64

Add a GitHub Actions workflow that builds and uploads wheels on every release tag, plus a manual trigger for testing.
