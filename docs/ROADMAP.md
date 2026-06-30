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
- **Security**: artifact-id validation, atomic CAS writes, hash verification, path safety, mode validation, and IP-keyed rate limiting are implemented.
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

### 2. History depths and clonepack variants ✅

Implemented in `rust/src/lib.rs`, `rust/src/server.rs`, `rust/src/client.rs`, `rust/src/pack.rs`, and `rust/src/git.rs`. See `CHANGELOG.md` for details.

- The server now produces both a `shallow` (depth=1) and a `full` clonepack for every sync.
- The ref endpoint selects the variant with `?clonepack=shallow|full`.
- The CLI exposes `--history shallow|full` on clone and `--depth N` on sync.

Remaining work:
- **Repo/branch-specific depth configuration** (see section below).
- Support more than two hard-coded depths (e.g., depth=10, depth=50) without recompiling.

### 2a. Repo/branch-specific configuration (partly shipped)

Right now the server hard-codes two clonepack variants (`shallow` = depth 1, `full` = unlimited). Users and orgs should be able to configure this per repo/branch without recompiling.

**Shipped** (`rust/src/repo_config.rs`, admin endpoint + build wiring in `rust/src/server.rs`):

- A `RepoConfigStore` backed by the same storage as artifacts (file for local dev, S3 for production), keyed `owner/repo[/branch]` with branch-level entries overriding repo-level entries (field-level overlay). It is read at build time only — the build records what the resolve path needs into the `RefInfo`, so the clone hot path never reads config. A write is visible to the next build immediately, across processes.
- The full config schema: `clonepack_depths: Vec<DepthSpec>` (`{ name, depth: Option<usize> }`), `compression_level`, `dictionary_id`, `hot_files`, `archive_chunk_size`, `head_blobs_chunk_size`, `enabled_modes`. Validated on write.
- Admin read/write endpoint: `GET`/`POST /v1/admin/config/{owner}/{repo}` (with `?branch=` for a branch override).
- The build reads the effective config and applies `compression_level` to the archive build (single-phase and two-phase paths).
- Default config (none stored) produces `shallow` + `full` exactly like today — unconfigured repos are byte-for-byte unchanged.

**Remaining** (needs the multi-variant build):

- Producing arbitrary named depths (e.g. `recent = 50` alongside `shallow` + `full`) requires generalizing the two-phase build engine and the fixed two-slot `RefInfo` (`shallow_clonepack` + `full_clonepack`) to an arbitrary named-clonepack list. The config schema and `validate()` already model this (today `validate()` caps a config to the two structural variants the build can emit); lifting that cap is the follow-up. Until then `clonepack_depths` is accepted and validated but the build emits the default `shallow` + `full`.
- Wire `dictionary_id`, `archive_chunk_size`, `head_blobs_chunk_size` (the build is already parameterized for these) and surface the config in a CLI / the ripclone-cloud UI. `enabled_modes` needs the resolve endpoint to learn the requested clone mode before it can be enforced server-side.

### 3. Unified async download/write pipeline ✅

Implemented in `rust/src/client.rs`, `rust/src/extract.rs`, and `rust/src/pack_writer.rs`. See `CHANGELOG.md` for details.

Remaining future improvements:
- Retry each chunk download with exponential backoff. ✅ (`RIPCLONE_FETCH_MAX_ATTEMPTS`/`RIPCLONE_FETCH_BACKOFF_MS`)
- Delete the temp install directory on failure. ✅ (RAII on the temp dir + overlay staging)

(The earlier "spill early chunks to disk before metadata" idea is obsolete: the
downloader now waits for the manifest before fetching, and peak memory is bounded
by the fetch-concurrency semaphore plus the bounded channel — not the chunk count.)

Writer overlap depth is tunable via `RIPCLONE_IO_URING_DEPTH` (default 2; set 3
on throttled/shared CPUs for ~10%). We are **not** pursuing a work-stealing
writer or the dedicated submitter-pool scheduler — see
[docs/WRITER_SCHEDULER_EXPERIMENT.md](docs/WRITER_SCHEDULER_EXPERIMENT.md) for
the A/B data and reasoning.

### 4. User-facing clone modes ✅

Implemented as `--mode full|fast|hybrid|skeleton` and `RIPCLONE_MODE`. See `CHANGELOG.md` for details.

(A `lazy` mode — metadata + archive chunks first, head-blobs fetched by a
background daemon — was considered and dropped: people want a clone to be fast
and complete, not a half-materialized one. Not pursuing it.)

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

### 7. Production hardening

- **Prometheus `/metrics`** ✅: served in Prometheus text exposition format.
- **Real `/readyz`** ✅: probes storage + ref-store health (write probe for local backends; bucket reachability for S3); 503 when a dependency is down, cached briefly to damp flapping.

Still missing:
- **JWT auth flow**: `ripclone auth login` that exchanges a secret for a short-lived JWT, plus `/v1/auth/refresh`.
- **GitHub App path**: support installation tokens in addition to the env-var PAT.
- **CI and integration tests**: GitHub Actions workflow with `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`, Docker build, and an end-to-end clone test against a fixture repo.
- **Extend existing e2e scripts**:
  - `scripts/e2e_clonepack.sh` already tests default vs. archive extraction for a public fixture; extend it to test `--mode=full`, `--mode=fast`, and `--mode=hybrid` and verify `git diff`/`git show` per mode.
  - `scripts/e2e_archive.sh` already verifies content, symlinks, executable bits, and edit detection for direct-install; reuse it for all modes.
- **Fuzz/property tests**: random manifests should either produce the expected tree or return `Err`, never a silently short tree.
- **Drop always-on config knobs** (done): two-phase publish and async builds are
  now hard defaults. The `RIPCLONE_TWO_PHASE` / `RIPCLONE_ASYNC_BUILD` env toggles
  and the now-dead single-phase build + synchronous `/sync` paths were removed.
  Drop the redundant `RIPCLONE_ASYNC_BUILD` from the production Soup environment
  (a no-op now that the toggle is gone).

### 8. Clonepack deltas / compaction (future)

Once warm full clones are fast and predictable, move from full clonepacks per commit to append-only delta chunks for recent commits, with background compaction. This is on the roadmap but not the current focus.

### 9. Trim the editable-clone build further (future)

The editable full clone now publishes as soon as history is built (the archive is built separately for files mode). On a single-commit linux re-sync that publish is ~7s, and the two biggest remaining costs on that path are the reachability-bitmap write (~2.4s) and the history build. Moving the bitmap write off the editable path — it's only needed to speed history enumeration, which is small for a delta — is the next lever for pushing depth=0-editable lower.

### 10. Build-before-clone trigger follow-ups (future)

The trigger surface that builds artifacts ahead of a clone is in place: a native GitHub push-webhook receiver (`/v1/webhooks/github`, HMAC-verified), the GitHub Actions trigger, and a polling fallback (`RIPCLONE_POLL_INTERVAL_SECS`). Two extensions are deferred until the product reaches that scale:

- **Multi-provider webhooks.** The receiver is GitHub-only. GitLab (plaintext `X-Gitlab-Token`), Bitbucket (`X-Event-Key`, often unsigned), and Gitea (`X-Gitea-Signature`) each use different signature schemes, event headers, payload shapes, and repo-path semantics (GitLab subgroups). Add a per-kind `WebhookProvider` trait behind a `/v1/webhooks/{provider}` route, keyed off the existing provider registry, with per-provider webhook secrets (`RIPCLONE_PROVIDER_<ID>_WEBHOOK_SECRET`, mirroring per-provider tokens). Do this when onboarding a non-GitHub provider.
- **Poll scaling: adaptive backoff + cross-replica coordination.** The poll loop currently sweeps every known repo on a fixed interval. At many repos, add per-repo poll state (last-seen tip, quiet-since) to poll active repos frequently and quiet ones rarely. Across multiple server replicas, each loop would multiply the `ls-remote` chatter (the SQL queue dedups the enqueues, not the probes); coordinate via a DB leader-lease (fits the queue's worker-lease idea in `DISPATCHER.md`), repo-set sharding, or a dedicated singleton poller. Both are farm-out-era work.

### 11. Empty-repository clones (future)

Cloning a repository with zero commits (an unborn HEAD) is not yet supported.
The server resolves `HEAD` to a commit, so an empty repo returns 404 at
resolve and "no objects to pack" at build, and the client would also bail in
`index_pack` on a 0-object pack. Real support means detecting an unborn HEAD in
sync/build (skip packs, store an "empty" ref), returning that from resolve
instead of 404, and having the client `git init` an empty repo with the unborn
default branch (no checkout). Deferred — it touches the central resolve/build
path and is a rare case (only freshly created, never-pushed repos).

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
