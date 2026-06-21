# ripclone roadmap

> Goal: the fastest practical way to clone a GitHub repo and be ready to work on it (`git status`, `git diff`, `git edit`, `git commit`) without rebuilding git or leaving GitHub.
>
> This repo is the **headless open-source backend**: the Rust server, the CLI, the archive format, and the GitHub Actions trigger.

## Clone modes & pack architecture (active plan)

We support **exactly three** clone modes — no arbitrary `--depth N` (it's finicky and almost nobody uses it; people pick depth=1 or full). Three content tiers:

- **head** = **`editable --depth 1`** (default): HEAD snapshot, full object DB for HEAD, `.git/shallow` boundary written. The agent/CI hot path. *(done & validated clean)*
- **full** = **`editable --depth 0`**: HEAD + all history, complete `git fsck`-clean clone, no shallow marker. *(done — validated on bun: 60 packs, rev-list to root, fsck clean, shipped MIDX verifies)*
- **worktree** = **`files`**: worktree only, no git objects (zstd archive) — fastest CI path. *(done)*

This collapses the server to **two pack buckets** (no range/geometric depth buckets):

- **HEAD-closure packs** — every current blob + HEAD commit/trees. **Undeltified**, many small (~2 MB) packs so the client downloads in parallel and hand-parses for the working tree. Used by *every* depth. *(done)*
- **History packs** — full history minus HEAD closure (ancestor commits/trees, old blob versions). **Deltified** (undeltified history is multi-GB), fewer/larger packs. Only **installed** for the object DB; the client never hand-parses or materializes the worktree from them (git reads them, resolving deltas). *(history bucket built; needs deltify flip)*

depth=1 clonepack lists HEAD-closure packs; full lists HEAD + history. The depth=1 set is a **content-addressed subset** of the full set — no separate HEAD pack is built.

**MIDX (the "many packs but git stays fast" lever):** the server pre-builds a `multi-pack-index` per variant (head-MIDX, full-MIDX) in a temp pack dir using the client's deterministic `pack-<trailer>.{pack,idx}` filenames, stores it as a content-addressed artifact, and the client drops it into `.git/objects/pack/`. Object lookups become O(log) across all packs regardless of count — so many small packs (great download parallelism) coexist with fast `git status`/`diff`/`log`. Cheap to build (indexes existing idx files; no pack rewrite). Optionally `--bitmap`.

**Full-clone correctness (two distinct tiers, do not conflate):**
- **head/depth-1** is *shallow* and must carry a `.git/shallow` boundary marker so `git log`/`deepen` stop cleanly at HEAD. *(done — the client writes it)*
- **full/depth-0** is *complete* and must carry **no** marker; it requires the **mirror to hold the entire history**. The mirror was created with `--depth 50`, so "full" silently meant "last 50 commits" and `git rev-list HEAD` broke at the boundary (observed bug). Fix: the mirror is **always a complete clone** — the `--depth` knob is removed entirely; existing shallow mirrors are `fetch --unshallow`'d on next sync.

### Immediate next
1. **Deltify the history bucket** — kills the full-history size blowup (client unchanged; it only hand-parses HEAD packs). *(done)*
2. **Always-full mirror** so depth=0 is a true, fsck-clean full clone. Drop the `--depth 50` default; unshallow existing mirrors. *(done — validated on sharkdp/hyperfine: rev-list to root, fsck clean, no shallow marker)*
3. **Server-pregenerate + ship MIDX** (head + full) as a content-addressed artifact the client drops in (signed `midx_url`); the client falls back to building locally only for older manifests. *(done — `git multi-pack-index verify` passes on the server-built MIDX)*

### Known scaling cost
The full (depth=0) build **rebuilds the entire deltified history on every sync**. This is now fast enough for medium repos — bun (15.7k commits, 6.2 GiB raw) builds in ~1m40 on a 2 GB server after history packs were given their own large target (`RIPCLONE_HISTORY_PACK_BYTES`, default 512 MiB raw → ~13–39 MB download pieces; previously the 6 MB HEAD target exploded it into 1058 packs / a 26-min build that failed). depth=1 and files are unaffected. The remaining cost is that the work is O(full history) on *every* sync; the **LSM incremental build** below removes that by building history once and appending.

## LSM incremental history build

**Status: v1 shipped behind `RIPCLONE_LSM=1` (default off).** Validated live on bun: first sync (build all + seal level 0) 110s → second sync (empty tail, level 0 reused by hash) 21s; the full clone stays complete (15.7k commits, fsck-clean, client-built MIDX). Integration tests in `rust/tests/lsm_incremental.rs` cover the incremental-tail completeness (incl. the head-exclusion trap) and the empty-tail no-op. Compaction/GC remain deferred (below).

Goal: drop steady-state sync cost from O(all history) to **O(new commits since last sync)** by treating history as immutable, content-addressed **commit-range levels** instead of rebuilding it all each time.

**Model.** History is split at commit boundaries into levels; each level packs the objects introduced in its range and is content-addressed (so it is reused across syncs and shared across branches, and lives in object storage forever):
- **Sealed levels** `L0..Ln` — immutable. Level `i` = `git rev-list <Bi> --not <Bi-1> --objects` (full range, deltified). Built once, never rebuilt; the manifest just references their hashes.
- **Tail** — `git rev-list HEAD --not <last-sealed-tip> --objects`, rebuilt each sync (only the new commits). When the tail exceeds `RIPCLONE_LSM_SEAL_BYTES` (raw), it is **sealed** into a new level and the next tail starts from HEAD.
- **HEAD closure** is unchanged (undeltified small packs, depth-1 hot path) and shipped *additively*.

**Correctness rule (subtle, do not break):** sealed levels and the tail must pack the **full** range — they must **not** subtract the HEAD closure. A blob that is current at seal time but later changes would otherwise be excluded from the immutable level *and* absent from every future HEAD closure → a missing object. The HEAD closure therefore overlaps the deltified history (current blobs appear in both); git dedups by OID on read. (The non-LSM rebuild-all path can keep excluding the head set because it ships a fresh head closure every sync.)

**Coverage:** `union(sealed levels) ∪ tail` = every object reachable from HEAD (reachability set-partition), so a full clone is complete even across force-push/rebase — a rewrite just leaves some unreachable (dangling, fsck-clean) objects in old levels until compaction.

**Sync flow (behind `RIPCLONE_LSM=1`, default off):**
1. Load the previous ref's `history_levels`; `sealed_tip = levels.last().tip`.
2. Build HEAD closure + tail `(sealed_tip..HEAD)`.
3. This manifest's history = `flatten(prev levels) + tail` (prior levels referenced by hash, already in object storage).
4. **Upload + evict only the newly built packs** (head + tail); prior levels are untouched.
5. Persist `history_levels` = seal ? `prev + [tail-as-level @ HEAD]` : `prev`.

**Interactions:**
- **MIDX:** the head/shallow MIDX is still server-built (head packs are local this sync). The *full* MIDX is omitted under LSM (prior-level packs aren't local to index) and the client builds it — acceptable since full clones are rarer. Future: keep `.idx` files local (small) to pregenerate the full MIDX without re-downloading packs.
- **Eviction:** unchanged — only the new packs are evicted; prior levels were already evicted on their own sync.

**Deferred within LSM:** geometric **compaction** (merge adjacent levels so level count stays bounded) and **GC** of dangling objects after rewrites. v1 seals without compacting; level count grows slowly.

### Deferred
- **LSM compaction/GC** — see above.
- **io_uring** durable-disk worktree writes — orthogonal speedup so `--temp` (ephemeral tmpfs) isn't required for fast durable clones.
- **Blobless partial clone** (`--filter=blob:none` via a git promisor pointing at the server) — distinct from `files`; a separate project for huge monorepos.

## Cross-repo dedup & status endpoint (planned)

Forks come **first** because they change the storage model; the other two are
small endpoint additions.

### a. Fork / shared-history dedup (the deep one — do first)

Forks share ~all of their history with the upstream. If the cache namespaces
storage per repo, a popular repo's fork network stores near-identical packs
thousands of times — and **object storage is the dominant cost**, so this is the
thing that blows it up.

The LSM history levels are already content-addressed and "shared across branches
and live in object storage forever." The work is to extend that sharing **across
repos in a fork network**:

- **Global CAS, not per-repo.** Sealed levels / page-group chunks are keyed purely
  by content hash in a store that is global (or at least shared within a fork
  network), so identical history is stored once. A fork's history becomes
  `references to the upstream's existing levels + a small tail of its unique
  commits`. Sync for a fork is then O(unique commits), not O(history).
- **Authorization on fetch — a content hash must NOT be a bearer capability.**
  Once chunks are shared across trust boundaries (a private fork sharing history
  with a public upstream; two private repos that share history), "knows the hash"
  cannot imply "may fetch the bytes." The serve path must verify the caller is
  authorized for *some repo whose manifest references this chunk*, then mint a
  short-lived signed URL for that request. Signed URLs must never be globally
  guessable or long-lived. This is a security requirement, not an optimization.
- **No cross-repo leakage.** Dedup decisions and byte accounting must not let the
  reader of one repo infer another repo's private content (e.g. via "this chunk
  already existed" timing/size oracles). Treat shared-history *existence* as
  private.

### b. `GET /v1/repos/{owner}/{repo}/status`

One call returning a repo's warm coverage + storage, so an operator (or a CLI) can
see what's built and how much it occupies:

- `refs: [{ branch, commit, bytes_total, bytes_unique }]` — `bytes_unique` =
  bytes this repo adds that are *not* shared with other repos (the marginal figure
  from dedup above); `bytes_total` = the logical size.
- repo totals `{ total_bytes, unique_bytes }`.

### c. Per-branch sync

`POST /v1/repos/{owner}/{repo}/sync` currently builds HEAD only (`SyncRequest`
has just `depth`). Add `?branch=<name>` so a non-default branch can be warmed
explicitly.

### Related

- **Configurable clone depth** is already planned in **§2a Repo/branch-specific
  configuration** (`?clonepack=<name>`, configurable `DepthSpec`s) — no new work
  beyond 2a.
- **On-demand per-region warming** (warm repo X in region R) rides the **§5
  Fly-region cache warmers**.

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
- A simple admin CLI or API endpoint (`POST /v1/admin/config/{owner}/{repo}`) can write the config.

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
