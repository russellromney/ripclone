# Changelog

This file tracks what has already landed in ripclone. For upcoming work see `ROADMAP.md`.

## Distribution

- **Three install channels**, all driven by a `v*` tag (`.github/workflows/release.yml`):
  - **Shell installer** — prebuilt per-platform binaries + checksums on a GitHub Release, fetched by `install.sh` (`curl … | sh`).
  - **crates.io** — `cargo install ripclone` / `cargo add ripclone` (validated end-to-end with `cargo publish --dry-run`).
  - **PyPI** — `pip install ripclone`, a maturin-built wheel of the CLI binary.
- **`ripclone update`** checks the latest GitHub release and shows how to update (a repo with no releases yet reports "no published releases yet" instead of a fetch error).
- Binaries build natively per platform (no cross-compile) and link C libraries dynamically — see the README for runtime packages; static binaries are a future refinement. The release jobs need `CARGO_REGISTRY_TOKEN` (crates.io) and PyPI Trusted Publishing configured, and prove out on the first real tag.

## Licensing

- **ripclone is now licensed `MIT OR Apache-2.0`** (the Rust ecosystem default): `license` set in `rust/Cargo.toml`, with `LICENSE-MIT` and `LICENSE-APACHE` at the repo root. This also unblocks crates.io/PyPI publishing.
- **cargo-deny now enforces a permissive license allow-list** (`rust/deny.toml`) — a new dependency under a copyleft or unlisted license fails CI for a human to evaluate.

## Benchmark harness

- **`benchmark/fly_shaped_benchmark.sh`** now prints the resolved commit in its header instead of `commit=latest`, and prefers `RIPCLONE_SERVER_TOKEN` (falling back to the deprecated `RIPCLONE_TOKEN`).
- Documentation and example workflow updated to use `RIPCLONE_SERVER_TOKEN` consistently.

## Sync / ref-store correctness

- **Commit-keyed ref-store keys for rev-targeted builds** (`rust/src/server.rs`): `sync --at <rev>` and `sync?rev=<rev>` now store artifacts under `{branch}#{commit}` instead of `{branch}#{rev}`. This prevents stale/incomplete rev-keyed refs from blocking future syncs of the same tag and makes different revs that resolve to the same commit share a build.
- **Commit-keyed reuse for file and S3 metadata stores** (`rust/src/ref_store.rs`): `RefStore::load_build` is now implemented for `FileRefStore` and `S3RefStore`, so a sync of branch `bar` can reuse a completed build of branch `foo` at the same commit instead of rebuilding.
- **Don't reuse completed builds that lack a files archive** (`rust/src/server.rs`): `reuse_existing_build` no longer returns a full clonepack whose archive chunks are empty (unless archive generation is still in progress), which previously left files-mode clones polling forever.
- **git index-pack fallback** (`rust/src/git.rs`): when gix fails to index a pack containing ref deltas (e.g. `oven-sh/bun`), ripclone falls back to the stock `git index-pack` subprocess.

## Version reconciliation (CLI ↔ server)

- **`ripclone --version` and `ripclone-server --version`** now report the build version (they previously errored).
- **`/v1/version`** (`rust/src/server.rs`): a public, unauthenticated endpoint returning `{ version, protocol }` so a client can check compatibility without credentials.
- **`ripclone version`** (`rust/src/bin/cli.rs`): prints the CLI's version + protocol, queries the configured server's `/v1/version`, and reports a compatibility verdict. Compatibility is keyed on a new wire **`PROTOCOL_VERSION`** (`rust/src/lib.rs`), not the build version — so the CLI and server can be released on independent cadences as long as their protocol versions match. Bump `PROTOCOL_VERSION` only on a breaking protocol change.
- **Server enforces the protocol** (`rust/src/client.rs`, `rust/src/server.rs`): the client sends its `PROTOCOL_VERSION` on authenticated requests, and the server rejects a *newer-than-it-understands* client with `426 Upgrade Required` and an actionable message instead of a confusing downstream error. A missing header (legacy client) or an older/equal protocol is allowed, so this never breaks existing clients.

## Supply chain

- **Dependencies are pinned and move only deliberately.** `Cargo.lock` is committed and every CI/Docker `cargo` invocation uses `--locked`, so the resolved versions never drift on their own. Updates land only through reviewed **Dependabot** PRs (`.github/dependabot.yml`): one grouped PR per week for Rust crates and one for GitHub Actions — none auto-merged.
- **`cargo-deny` guards the tree** (`rust/deny.toml`, `.github/workflows/deny.yml`): known security advisories (and yanked crates) fail CI, including on a weekly schedule so a new advisory against an unchanged dependency still surfaces; only crates.io is an allowed source; and every dependency's license must be on a permissive allow-list.
- **Dependency/security changes get a changelog line here** stating what moved and why (CVE, feature need, or transitive requirement).
- **Removed the vestigial `ripclone mount` (FUSE) experiment** — the `fusefs` module, the `Mount` command, and the `fuser` dependency are deleted (~1.1k lines). This clears `RUSTSEC-2021-0154` by removal; the `git2` advisories (`RUSTSEC-2026-0183`/`-0184`) were already cleared by the gix migration removing `git2`. The three now-stale advisory `ignore` entries are dropped from `rust/deny.toml`. Can be re-added later if FUSE mounting is wanted.
- **Allow `MPL-2.0` in `deny.toml`** (weak/file-level copyleft, safe to depend on from a permissive project) — the gix migration pulls in `uluru` (MPL-2.0), which the license check was rejecting.

## Backend config in config.toml + `ripclone backend` CLI

- **Server-side backends are now configurable from `config.toml`**, not just env vars (`rust/src/config.rs`, `rust/src/backends.rs`). New `[storage]`, `[metadata]`, and `[queue]` sections feed the same selection logic. The matching `RIPCLONE_*` env vars **always override** the file (consistent with the existing `--flag > env > config` precedence). The config is loaded once and consulted as a fallback by `queue_kind`/`queue_db_url`/metadata selection and `S3Storage::from_env_or_config`.
- **`ripclone backend` CLI** (`rust/src/bin/cli.rs`): `show` prints each backend field's effective value and source (marking env overrides); `queue` / `metadata` / `storage` set the corresponding section in the global `config.toml` (only the flags you pass change). The config file is written `0600` since these sections can hold connection settings.
- **Credentials stay in the environment.** `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are never read from config. DB tokens (`[queue].token` / `[metadata].token`) are supported in-file for now (no keyring yet) and masked by `backend show`.
- **Server reads the global config only, with an explicit path override.** Backend selection now uses `load_global` (not the cwd-walked `load`), so a stray project `ripclone.toml` in the server's working directory can no longer silently change its backends. `RIPCLONE_CONFIG` points the global config at an explicit file (e.g. `/etc/ripclone/config.toml`) — useful for a daemon/container without a `$HOME` — and `backend show` prints which file is in effect.

## Pluggable build queue, standalone worker, and SQL metadata store

- **Pluggable build queue** (`RIPCLONE_QUEUE` = `local` | `sqlite` | `postgres` | `mysql` | `libsql`). A `JobQueue` trait; the in-process channel is now `local` (default). The SQL backends share one `SqlJobQueue` orchestration over a per-engine `QueueDb` adapter (`rust/src/queue/`): atomic conditional-`UPDATE` claim, best-effort coalescing with a partial-unique-index backstop where supported, crashed-worker reclaim (`RIPCLONE_QUEUE_STALE_SECS`, default 1800), and failed-job pruning (`RIPCLONE_QUEUE_FAILED_RETENTION_SECS`, default 7d; `done` jobs kept as build history). `libsql` is remote-only (Turso Cloud).
- **Standalone `ripclone-worker` binary** (`rust/src/bin/ripclone-worker.rs`): claims jobs from a SQL queue and runs the same build as the in-process worker, so `/sync` can be farmed out to other processes/machines. `/sync` polls the job's status to completion. Credentials are never persisted in the queue — the worker resolves its own from its provider config. One scratch `--repo-root` per worker.
- **Pluggable metadata store** (`RIPCLONE_METADATA` = `file` | `s3` | `sqlite` | `postgres` | `mysql` | `libsql`), decoupled from storage; unset follows storage (S3 if configured, else file). A `MetaDb` adapter + `SqlRefStore` implementing `RefStore` over one `repo_key` (`RepoId::storage_key`) + branch row (`rust/src/meta/`), with the same save-ordering policy as the S3 store and a `health()` probe. Holds pointers only — never file bytes.
- **Shared backend wiring** (`rust/src/backends.rs`): `Backends::from_env` + queue/metadata selection, used by both the server and the worker so they configure identically.
- Cross-process correctness: the server invalidates its ref caches after a worker build, and a HEAD sync resolves from the metadata store alone (a `HEAD` ref alias) so a server without the local mirror never returns an empty ref. `?rev=` builds are rejected on the cross-process queue (use the local queue). Real-DB tested on Postgres/MySQL (`scripts/test-queue-sql.sh`) and a local `sqld` for libsql, plus a diskless (separate-`repo_root`) farm-out e2e. See `docs/BACKENDS.md`.

## Multi-provider auth (Phases 1 & 2)

- **Breaking: explicit-provider addressing** (`rust/src/server.rs`, `rust/src/client.rs`, `rust/src/bin/cli.rs`, `rust/src/bin/git-remote-ripclone.rs`)
  - Legacy `/v1/repos/{owner}/{repo}/...` routes are removed. All repos are now addressed as `/v1/repos/{provider}/{repo-path}/...`, including GitHub (`/v1/repos/github/owner/repo/...`).
  - The CLI and git remote helper accept provider-qualified paths (`github/owner/repo`, `gitlab/group/sub/project`).
- **Provider registry + presets** (`rust/src/provider.rs`)
  - New `ProviderKind` enum: `github`, `gitlab`, `bitbucket`, `gitea`, `generic`.
  - `ProviderRegistry::load()` reads instances from `RIPCLONE_PROVIDERS` JSON or `RIPCLONE_PROVIDERS_CONFIG`, merged with the built-in `github` default.
  - Each instance defines `clone_url(path)` and `auth_header(token)` so the server can speak the right auth dialect to each host.
- **Credential-header injection** (`rust/src/git.rs`, `rust/src/auth/broker.rs`)
  - `sync_bare_mirror` builds a clean clone URL and injects credentials via `git -c http.extraHeader="Authorization: ..."`. Secrets no longer appear in URLs.
  - New `CredentialBroker` seam with a v1 `StaticBroker` (request token → configured token → none). This is the foundation for Tier-A token minting and OIDC in Phase 3.
- **Origin URL returned to clients** (`rust/src/server.rs`, `rust/src/client.rs`)
  - `RefResponse` now carries `origin_url`, `provider`, and `host`. Clients use the server-returned URL when configuring the `origin` remote, removing hardcoded `github.com` assumptions.
- **Per-provider validation** (`rust/src/validation.rs`)
  - GitHub default keeps strict `owner/repo` rules; other providers accept opaque variable-depth paths (`group/sub/project`, `~user/repo`, etc.).
- **X-Upstream-Token** (`rust/src/server.rs`, `rust/src/client.rs`)
  - The canonical upstream credential header is now `X-Upstream-Token`; `X-GitHub-Token` is still accepted as an alias.

## Client robustness + server observability

- **Chunk download retry with backoff** (`rust/src/client.rs`): all artifact/chunk fetches retry transient failures (transport errors, 5xx/429/408, mid-stream body errors) with jittered exponential backoff; permanent failures (other 4xx, deterministic hash mismatch) fail fast; a failed/expired presigned URL falls back to the gateway. Tunable via `RIPCLONE_FETCH_MAX_ATTEMPTS` (3) and `RIPCLONE_FETCH_BACKOFF_MS` (100).
- **No orphaned install dir on failure** (`rust/src/client.rs`, `rust/src/overlay.rs`): a failed clone now removes its partial temp install dir (RAII) and its overlay staging tree; `RIPCLONE_NO_OVERLAY` is a real opt-out.
- **Real `/readyz`** (`rust/src/server.rs`, `rust/src/storage/`, `rust/src/ref_store.rs`): returns 503 when storage or the ref store is unreachable (write probe for local backends, bucket reachability for S3), result cached ~3s; generic public body with details logged.
- **Prometheus `/metrics`** (`rust/src/metrics.rs`): served as Prometheus text exposition format (`text/plain; version=0.0.4`) instead of JSON. Fixed a `build_queue_depth` gauge underflow (async `/sync` builds now balance the gauge; decrements saturate at 0).

## io_uring worktree writer: default on Linux + tunable overlap

- **io_uring is now the default worktree writer on Linux** (`rust/src/worktree_writer.rs`)
  - With `RIPCLONE_IO_URING` unset, the writer uses io_uring on Linux (auto mode: falls back to POSIX if the kernel lacks support); other platforms stay POSIX. Set `RIPCLONE_IO_URING=0` to force POSIX. Real-clone A/B on Fly `/data` showed io_uring faster-or-equal vs POSIX on dedicated cores.
- **Graceful fallback when a ring can't be allocated** (`rust/src/worktree_writer.rs`): auto mode previously fell back to POSIX only when the *kernel* lacked io_uring support. Per-thread rings are created lazily at write time, so under heavy parallelism (many concurrent clones, or the test suite) ring creation could fail with `ENOMEM`/the locked-memory rlimit *after* startup and hard-fail the clone. Now any ring-creation failure disables io_uring for the rest of the run and the writer degrades to POSIX instead of failing; threads that already hold a working ring keep using it so deferred writes still flush.
- **Tunable per-thread ring overlap depth** via `RIPCLONE_IO_URING_DEPTH` (default 2). Throttled/shared CPUs can set `=3` for ~10% on the write phase; dedicated cores are best at 2.
- An opt-in submitter-pool scheduler (`RIPCLONE_IO_URING_SCHEDULER`) was prototyped and rejected — see `docs/WRITER_SCHEDULER_EXPERIMENT.md`. Superseded by the depth knob and slated for removal.

## Shallow/full clonepacks, archive chunk sizing, and sync depth

- **Dual clonepacks: shallow (depth=1) and full history** (`rust/src/lib.rs`, `rust/src/server.rs`, `rust/src/client.rs`, `rust/src/pack.rs`, `rust/src/git.rs`)
  - The server now builds two clonepack manifests per sync:
    - `shallow` = single commit + HEAD trees (matches `git clone --depth=1`).
    - `full` = all reachable commits/trees (existing behavior).
  - Archive and head-blobs chunks are shared between the two variants; only the skeleton pack, idx, and prebuilt index differ.
  - The ref endpoint accepts `?clonepack=shallow|full` (default `full`).
  - The CLI clone command gained `--history shallow|full` (default `shallow`).
  - Shallow clones write `.git/shallow` so `git log` and other history walkers stop at the boundary instead of failing on missing parents.

- **Configurable sync depth** (`rust/src/bin/cli.rs`, `rust/src/client.rs`, `rust/src/server.rs`)
  - `ripclone sync owner/repo --depth N` now passes `N` to the server, controlling how much history the bare mirror fetches.
  - `--depth 1` gives a shallow mirror; omitting it uses the server's configured default.

- **Archive chunks capped at 8 MB** (`rust/src/archive.rs`)
  - The chunker now finalizes the current chunk before a new frame would push it over the target.
  - The max uncompressed frame size is lowered to the frame target (6 MB) so a single compressed frame can never overflow an 8 MB chunk.
  - Added `archive::tests::archive_chunks_respect_target_size` to enforce the cap.

- **Idempotent CAS writes** (`rust/src/cas.rs`)
  - `Cas::put_with_hash` now skips writing when the target object already exists and tolerates concurrent writers racing to create the same content-addressed object.
  - Uses a per-process unique temp filename to avoid collisions between concurrent writers.

- **e2e script improvements** (`scripts/e2e_clonepack.sh`)
  - `SYNC_DEPTH` environment variable lets the script sync at a chosen depth.
  - Clone commands now pass `--bench` so the per-phase JSON report is captured.

## Unified async pipeline, clone modes, and per-phase benchmarks

- **User-facing clone modes** (`rust/src/mode.rs`, `rust/src/bin/cli.rs`, `rust/src/client.rs`)
  - Replaced the hidden `RIPCLONE_EXTRACT_ARCHIVE=1` flag with `--mode full|fast|hybrid|skeleton`.
  - `full` is the default and behaves like `git clone --depth=1`: complete `.git`, head-blobs pack, and `git checkout-index`.
  - `fast` materializes the working tree directly from archive chunks; no head-blobs pack.
  - `hybrid` downloads archive chunks and head-blobs chunks concurrently; the working tree is extracted while the pack is written.
  - `skeleton` installs only `.git` (commit + tree objects, prebuilt index) with no working tree.
  - Mode can also be set with `RIPCLONE_MODE`.

- **Unified async download/write pipeline** (`rust/src/client.rs`, `rust/src/extract.rs`, `rust/src/pack_writer.rs`)
  - After resolving the ref, the client fetches the manifest, metadata chunk, archive chunks, and head-blobs chunks concurrently.
  - Archive chunks are pushed into a channel consumed by a new `extract_archive_from_chunk_receiver` worker, so files are written while later chunks are still downloading.
  - Head-blobs chunks are pushed into a channel consumed by `HeadBlobsWriter`, which writes each chunk to the correct pack-file offset and computes the SHA-256 hash incrementally.
  - The install is written into a temp directory and atomically renamed onto the target on success.

- **Per-phase benchmark instrumentation** (`rust/src/bench.rs`, `rust/src/bin/cli.rs`)
  - Added `--bench` and `RIPCLONE_BENCH=1` to print a JSON report with `resolve_ms`, `manifest_ms`, `metadata_ms`, `head_blobs_download_ms`, `archive_download_ms`, `write_ms`, `checkout_ms`, and `total_ms` plus bytes per phase.

- **Updated e2e coverage** (`scripts/e2e_clonepack.sh`, `scripts/e2e_archive.sh`)
  - `e2e_clonepack.sh` now tests `full`, `fast`, `hybrid`, and `skeleton` modes and verifies blob availability per mode.
  - `e2e_archive.sh` now tests both `full` and `fast` modes against `oven-sh/bun`.

## Head-blobs pack chunking and repository cleanup

- **Split head-blobs pack into parallel-fetch chunks** (`rust/proto/clonepack.proto`, `rust/src/server.rs`, `rust/src/client.rs`, `rust/src/lib.rs`, `rust/src/ref_store.rs`)
  - The head-blobs pack is no longer embedded in the metadata chunk or fetched as a single monolithic object.
  - `ClonepackManifest` now carries `repeated ChunkRef head_blobs_chunks` (default 8 MB each).
  - `RefInfo` and `RefResponse` carry the chunk hashes and signed URLs.
  - Client `fetch_chunk_refs` downloads chunks concurrently with configurable `RIPCLONE_FETCH_CONCURRENCY`.
  - Old single-pack manifests are still parsed for compatibility.

- **Fixed `benchmark/remote.sh` manifest parsing**
  - The script no longer stores binary protobuf in a shell variable, which corrupted the data and reported `archive chunks: 1`.
  - It now writes the clonepack manifest to a temp file and reports archive-chunk and head-blobs-chunk counts correctly.

- **Removed Python prototype and committed binaries**
  - Deleted `lazygit.py`, the `ripclone/` Python package, `pyproject.toml`, `requirements.txt`, and committed `dist/` release binaries.
  - Added `dist/` and `target/` to `.gitignore`.
  - Created public repo at `https://github.com/russellromney/ripclone` and rewrote history with `git filter-repo` to purge binaries.

## Adversarial review fixes

- **Fixed Fly client archive-extraction benchmark** (`scripts/fly_client_test.sh`, `docs/ARCHIVE_AB_RESULTS.md`)
  - The benchmark script now captures `ripclone` logs so performance debugging is possible.
  - It also unmounts overlay targets and deletes `/dev/shm/ripclone-overlay-*` staging directories between runs.
  - Previously, leftover staging consumed `/dev/shm` and forced archive extraction to fall back to the slow rootfs, making it look ~8× slower than it really is.
  - Updated A/B numbers: archive extraction on Fly client (overlay) is ~10.5 s vs ~6.0 s for `git checkout-index`, not ~49 s.

- **Fixed overlay space estimation** (`rust/src/client.rs`)
  - `should_use_overlay` was using only the last archive chunk's size as the compressed estimate.
  - It now sums `compressed_len` across all frames, which equals the total compressed archive size.

- **Fixed benchmark cleanup** (`benchmark/archive.sh`, `benchmark/latency.sh`, `benchmark/remote.sh`, `scripts/e2e_clonepack.sh`)
  - Added overlay unmount and `/dev/shm/ripclone-overlay-*` cleanup so repeated local runs do not fall back to rootfs.
  - Removed undefined `frame_count` references from benchmark summaries.

- **Removed stale server env vars** (`Dockerfile`)
  - Dropped unused `REPOLAYER_*` variables left over from an earlier name.

## Roadmap cleanup

- Moved completed roadmap items to this changelog: clonepack format, integration tests, overlay staging, S3/storage backend, retention, smart-HTTP fallback, git remote helper, token auth, rate limiting, metrics, and health endpoints.
- Added the four cloud-session changes to the active plan in `ROADMAP.md`.

## Protobuf clonepack format

- **Protobuf schema for all clonepack artifacts** (`rust/proto/clonepack.proto`, `rust/build.rs`, `rust/Cargo.toml`)
  - Added `prost` + `prost-build` and a `proto/clonepack.proto` schema.
  - `ClonepackManifest` is the top-level per-commit protobuf.
  - `MetadataChunk` bundles skeleton pack/idx, HEAD-blobs pack/idx, prebuilt `.git/index`, frame table, and file table.
  - `ChunkRef` stores SHA-256 hash bytes + byte length for every content-addressed chunk.

- **CAS uses SHA-256** (`rust/src/cas.rs`, `rust/src/retention.rs`)
  - Content-addressed chunks are now hashed with SHA-256 instead of SHA-1.
  - Updated retention scanning to recognize 64-byte hashes.

- **Archive builder emits content-addressed chunks** (`rust/src/archive.rs`)
  - `ArchiveBuilder::build_chunks` groups zstd frames into 1–8 MB archive chunks.
  - Frame table records `chunk_index` + `chunk_offset` so commits can share chunks.
  - `ArchiveBuilder::build` still writes a single local archive file for CLI/debug use.

- **Metadata chunk helpers** (`rust/src/manifest.rs`, `rust/src/clonepack.rs`)
  - Replaced the custom binary manifest with protobuf `MetadataChunk` encode/decode.
  - Added `verify_archive`, `fragments_by_frame`, and `archive_chunk_lengths` helpers.

- **Server builds and stores clonepack manifests** (`rust/src/server.rs`, `rust/src/lib.rs`)
  - `do_sync` assembles the full `MetadataChunk`, stores archive chunks separately, builds a `ClonepackManifest`, and stores it in the CAS.
  - `RefInfo` and `RefResponse` now include `clonepack_manifest`.
  - Storage upload and retention protection cover archive chunks and the clonepack manifest.

- **Client downloads the metadata chunk** (`rust/src/client.rs`)
  - `install_ref`, `install_worktree_files`, and `install_git_dir` now fetch a single metadata chunk protobuf and write the skeleton/HEAD-blobs packs and prebuilt index from it.
  - Eliminates five separate artifact round-trips on the hot clone path.

- **Client fetches the top-level clonepack manifest** (`rust/src/client.rs`, `rust/src/extract.rs`)
  - `Client::fetch_clonepack` decodes the `ClonepackManifest`, then fetches the metadata chunk it references.
  - `install_repo` and `add_worktree` now discover metadata and archive chunks through the clonepack manifest.
  - Added `extract::extract_clonepack_streaming` to materialize the working tree directly from archive chunks.
  - Archive-chunk extraction is behind `RIPCLONE_EXTRACT_ARCHIVE=1` so it can be A/B tested against the default `git checkout-index` path.

- **A/B test archive extraction vs. checkout-index** (`docs/ARCHIVE_AB_RESULTS.md`)
  - Measured locally, macOS → Fly, and Fly client → Fly server for `pandas-dev/pandas` and `oven-sh/bun`.
  - Checkout-index is faster over the network and in the cloud; archive extraction wins only locally.
  - Decision: keep `git checkout-index` as the default; archive extraction stays opt-in.

- **Cleaned up legacy `RefResponse` fields** (`rust/src/server.rs`, `rust/src/client.rs`)
  - Removed `skeleton_pack`, `skeleton_idx`, `head_blobs_pack`, `head_blobs_idx`, `prebuilt_index`, `archive`, and `manifest` from the public `/v1/repos/.../refs/...` JSON response.
  - Wired `install_git_dir` and `skeleton_clone` through `fetch_clonepack` so the git remote helper no longer relies on the legacy `manifest` field.
  - Updated benchmark scripts to use `clonepack_manifest` instead of the removed fields.

- **Integration test for clonepack round-trip** (`scripts/e2e_clonepack.sh`)
  - Starts a local server, syncs `octocat/Hello-World`, decodes the clonepack manifest, verifies the metadata chunk protobuf, and clones with both default and archive paths.
  - Confirms both paths produce a clean `git status` and identical file lists.

## Overlay staging and fast worktrees

- **Overlay staging for `ripclone clone`** (`rust/src/overlay.rs`, `rust/src/client.rs`)
  - On Linux, materializes the working tree in a fast staging dir (`/dev/shm` or
    `RIPCLONE_STAGING_DIR`) and overlay-mounts it at the target.
  - Avoids slow rootfs write storms on cloud VMs.
  - Falls back to direct extraction if overlay is unavailable or space is insufficient.
  - Tunable via `RIPCLONE_NO_OVERLAY`, `RIPCLONE_OVERLAY_THRESHOLD_MB`, and
    `RIPCLONE_OVERLAY_MARGIN_MB`.

- **`ripclone worktree`** (`rust/src/bin/cli.rs`, `rust/src/client.rs`, `rust/src/git.rs`)
  - Adds `ripclone worktree <path> -b <branch>` for fast worktree creation.
  - Reuses the main clone's local `.git/index` and object DB when the commit
    matches, so no network download is needed for the common case.
  - Falls back to fetching prebuilt artifacts for a different commit.
  - Uses the same overlay-staging path as `ripclone clone`.


## Client auth and cache cleanup

- **Pre-hashed token support** (`rust/src/bin/cli.rs`, `rust/src/bin/git-remote-ripclone.rs`)
  - Added `RIPCLONE_TOKEN_HASH` env var so CI and 1Password users can provide the
    SHA-256 hash directly instead of the raw secret.
  - `RIPCLONE_TOKEN` is still hashed before sending.

- **Opt-in local cache** (`rust/src/client.rs`, `docs/BACKENDS.md`)
  - Removed the default `~/.cache/ripclone` artifact cache.
  - Caching is now opt-in via `RIPCLONE_CACHE_DIR`.
  - `RIPCLONE_NO_CACHE=1` forcibly disables caching.

## GitHub Actions workflow example

- **Updated trigger example** (`README.md`, `docs/examples/github-actions-trigger.yml`)
  - Replaced the old `/v1/build` webhook example with the current
    `/v1/repos/{owner}/{repo}/sync` endpoint.
  - Shows how consumer repos can notify a ripclone server on every push using
    `Authorization: Ripclone <token>`.
  - Notes that private repos require `RIPCLONE_GITHUB_TOKEN` on the server.

## Smart-HTTP fallback endpoints

- **Vanilla git compatibility** (`rust/src/server.rs`)
  - `GET /v1/git/{owner}/{repo}/info/refs?service=git-upload-pack` advertises
    refs using the local bare mirror.
  - `POST /v1/git/{owner}/{repo}/git-upload-pack` runs `git upload-pack
    --stateless-rpc` against the mirror so a plain `git clone
    http://server/v1/git/owner/repo` works without the archive-first path.
  - Useful for cold caches or clients that cannot use the remote helper.

- **Validation**
  - `scripts/e2e_smart_http.sh` verifies `git clone` through the fallback
    endpoints end-to-end.

## Private-repo sync

- **`RIPCLONE_GITHUB_TOKEN`** (`rust/src/server.rs`, `rust/src/git.rs`)
  - Server reads `RIPCLONE_GITHUB_TOKEN` and passes it to `git::sync_bare_mirror`.
  - `sync_bare_mirror` embeds the token in the HTTPS URL as
    `https://x-access-token:<token>@github.com/<owner>/<repo>.git`, which works
    for both personal access tokens and GitHub App installation tokens.

## Local CAS retention / eviction

- **Retention manager** (`rust/src/retention.rs`)
  - Scans the local content-addressed store on a configurable interval.
  - Keeps a persisted set of "protected" hashes (artifacts referenced by the
    current HEAD of each synced repo).
  - Evicts unprotected objects by age (`RIPCLONE_RETENTION_MAX_AGE_DAYS`,
    default 7 days) and by disk pressure (`RIPCLONE_RETENTION_MAX_GB`,
    default 100 GB), removing oldest unprotected objects first.
  - Tunable interval via `RIPCLONE_RETENTION_INTERVAL_SECS` (default 300 s).
  - Exposes retention counters on `/metrics`: runs, evicted bytes/objects,
    errors.

- **Server integration** (`rust/src/server.rs`)
  - The retention task starts automatically when the server boots.
  - After each successful sync, the current HEAD's artifact hashes are marked
    protected so they survive the next eviction pass.

## Server hardening (auth, metrics, rate limiting)

- **Token auth** (`rust/src/server.rs`)
  - Optional `RIPCLONE_TOKEN` env var on the server. If set, all non-health
    endpoints require `Authorization: Ripclone <sha256(token)>`.
  - Both server and client hash the raw token with SHA-256 once at startup/login
    so the hash, not the raw secret, travels on the wire.
  - `Client::new_with_token` sends the hashed token on every request.

- **Metrics and readiness** (`rust/src/metrics.rs`, `rust/src/server.rs`)
  - `GET /metrics` returns JSON counters: ref lookups, syncs, sync duration,
    artifact requests, bytes served, and errors.
  - `GET /readyz` reports readiness and includes a timestamp; `GET /healthz`
    remains public for load balancer health checks.

- **Rate limiting** (`rust/src/rate_limit.rs`)
  - Token-bucket rate limiter keyed by auth header or `anonymous`.
  - Env tunables: `RIPCLONE_RATE_LIMIT_BURST` (default 60) and
    `RIPCLONE_RATE_LIMIT_PER_SEC` (default 10.0).
  - Returns `429 Too Many Requests` with a `Retry-After` header when the bucket
    is empty.

## Native git remote helper

- **`git-remote-ripclone`** (`rust/src/bin/git-remote-ripclone.rs`)
  - Speaks the git remote-helper protocol (`capabilities`, `list`, `option`,
    `connect git-upload-pack`).
  - Parses `ripclone://owner/repo.git` and `ripclone://owner/repo.git#branch`.
  - Resolves the ref through the ripclone server, downloads the prebuilt
    skeleton pack, head-blobs pack, and prebuilt `.git/index`, and seeds the
    local object database so `git clone` can finish with its normal checkout.
  - Runs a local `git upload-pack` against the seeded repo so the rest of the
    clone is a normal git transport.
  - Reads `RIPCLONE_URL` and hashes `RIPCLONE_TOKEN` with SHA-256 before
    sending it in the `Authorization` header.

- **`Client` additions** (`rust/src/client.rs`)
  - `Client::new_with_token` builds an HTTP client that sends the ripclone
    token on every request.
  - `Client::install_git_dir` downloads only the `.git` artifacts needed by
    the remote helper.
  - Ref responses now include `default_branch` so the helper can create the
    correct local branch ref for `HEAD` clones.

- **Validation**
  - `scripts/e2e_remote_helper.sh` verifies `git clone ripclone://oven-sh/bun.git`
    end-to-end; the resulting repo has a clean `git status` and working `git log`.

## Direct index mutation

- **No more `git update-index` on the clone path** (`rust/src/git.rs`)
  - `set_skip_worktree_all` and `clear_skip_worktree_index` now mutate the
    `.git/index` directly through `git2`.
  - Replaced the remaining subprocess callers in `extract.rs`, `sidecar.rs`,
    `snapshot.rs`, and `cli.rs`.
  - Verified with the full `oven-sh/bun` e2e suite and the S3/MinIO e2e suite.

## S3-compatible object storage

- **`S3Storage` backend** (`rust/src/storage/s3_storage.rs`)
  - Implements the `StorageBackend` trait for S3, R2, Tigris, and MinIO.
  - Configured with `RIPCLONE_S3_ENDPOINT`, `RIPCLONE_S3_REGION`,
    `RIPCLONE_S3_BUCKET`, `RIPCLONE_S3_PREFIX`, and `RIPCLONE_S3_CACHE_DIR`.
  - Credentials come from standard `AWS_*` environment variables via the `s3`
    crate's `Auth::from_env()`.
  - Reads the local cache first; on miss fetches the full object or a byte range
    from S3 and writes it into the local cache.
  - Supports `Range: bytes=start-end` via `get_range` so the server can still
    proxy partial requests when signed URLs are unavailable.

- **Signed-URL serving** (`rust/src/server.rs`)
  - `serve_artifact` calls `storage.signed_url()` and returns a
    `307 Temporary Redirect` when the backend supports direct client reads.
  - Clients range-GET archives directly from object storage/CDN; the server only
    serves the small manifest/ref response.

- **Server upload after build** (`rust/src/server.rs`)
  - `do_sync` pushes every built artifact (skeleton pack/idx, head-blobs pack/idx,
    prebuilt index, archive, manifest) to the configured storage backend.
  - For local storage this is a no-op; for S3-compatible backends it makes the
    artifacts durable and CDN-addressable.

- **Validation**
  - End-to-end test passed against a local MinIO container: server uploads
    artifacts, client follows signed-URL redirects, and the resulting repo has a
    clean `git status`.

## Archive-first clone (v1)

- **Archive builder** (`rust/src/archive.rs`)
  - Walks the git tree directly so every tracked file is included, even files
    that `git archive` would omit because of `export-ignore` attributes.
  - Streams files into 2 MB zstd frames.
  - Default archive compression level is **zstd 6** (tuned for size; build time
    is server-side).
  - Optional custom zstd dictionary training (`train-dictionary`, `--dictionary`);
    measured as a net loss on `oven-sh/bun`, so it remains opt-in.

- **Binary manifest** (`rust/src/manifest.rs`)
  - Content-addressed frame table + per-file entries (path, mode, git blob
    SHA-1, frame index, frame offset, compressed/raw length).
  - Rust unit tests for roundtrip, happy-path verification, and SHA-1 mismatch
    detection.

- **Parallel extractor** (`rust/src/extract.rs`)
  - Decompresses all zstd frames in parallel.
  - POSIX path: parallel file writes with modes set in `open()`.
  - Linux path: io_uring batched `open/write/close` syscalls.
  - Batches directory creation up front.
  - Verifies every extracted file against its manifest SHA-1.
  - Sets deterministic mtime so repeated extractions are idempotent.

- **Skeleton clone**
  - Server builds a git packfile containing the commit object and every
    reachable tree.
  - Client installs a bare `.git` with `HEAD`, index, `skip-worktree`, and an
    `origin` remote pointing at the upstream GitHub URL.
  - Removed `core.fileMode=false`; skeleton clones keep the platform default.

- **CLI commands**
  - `ripclone sync <owner/repo>`
  - `ripclone build-archive <owner/repo> --archive <path> --manifest <path>`
  - `ripclone clone <owner/repo> --dir <path> --skeleton`
  - `ripclone extract-archive --archive <path> --manifest <path> --dir <path>`

- **E2E coverage** (`scripts/e2e_archive.sh`)
  - Compares the extracted tree against an independent `git archive` reference
    (for repos without `export-ignore`).
  - Verifies all tracked files present, symlinks, executable bits, `git status`,
    `git diff`, `git log`, `core.fileMode=true`, `origin` remote, idempotent
    re-extraction, corruption detection, and missing-manifest failure.

- **Benchmarks**
  - `benchmark/archive.sh` compares zstd levels and extracts level 6.
  - Measured on macOS and in a Linux Docker container (io_uring path).

## Parallel downloads, streaming extraction, and range requests

- **Parallel artifact downloads** (`rust/src/client.rs`)
  - The client now fetches skeleton pack/idx, head-blobs pack/idx, prebuilt
    index, and manifest concurrently with `tokio::try_join!`.

- **Streaming frame extraction** (`rust/src/extract.rs`)
  - Added `extract_archive_with_fetcher`, which decouples archive layout from
    I/O so the same extraction code can read from a local file or from
    arbitrary byte sources.
  - Added `extract_archive_streaming`, which fetches each zstd frame with an
    HTTP range request and decompresses/writes files without loading the whole
    archive into memory.

- **Server range-request support** (`rust/src/server.rs`, `rust/src/storage.rs`)
  - Added a `StorageBackend` trait so local CAS and future object-storage
    backends share one interface.
  - Artifact endpoints (`/v1/artifacts/{hash}`, `/v1/archives/{hash}`,
    `/v1/manifests/{hash}`) now honor `Range: bytes=start-end` and return
    `206 Partial Content`.
  - Backends that support signed URLs can return a `307 Temporary Redirect` so
    clients range-GET directly from object storage/CDN.

- **Updated E2E** (`scripts/e2e_archive.sh`)
  - Content verification now compares every regular file's SHA-1 against the
    bare mirror's HEAD blobs, so export-ignored files are also checked.

## Prebuilt `.git` artifacts + direct install

- **Server-side artifact builders** (`rust/src/pack.rs`)
  - Skeleton pack + `.idx`: commit object + every reachable tree + symlink blobs.
  - HEAD-blobs pack + `.idx`: every blob referenced by `HEAD` so `git diff`,
    `git show`, and edits work immediately.
  - Prebuilt `.git/index`: built from the skeleton pack with accurate cached
    blob sizes and `skip-worktree` set on every tracked path.

- **CAS storage for all artifacts** (`rust/src/cas.rs`)
  - Packs, indexes, archives, and manifests are stored by content hash.

- **Extended ref entry** (`rust/src/lib.rs`)
  - `RefInfo` now carries hashes for `skeleton_pack`, `skeleton_idx`,
    `head_blobs_pack`, `head_blobs_idx`, `prebuilt_index`, `archive`,
    `manifest`, and the optional `full_pack`.

- **New artifact endpoints** (`rust/src/server.rs`)
  - `/v1/artifacts/{hash}` serves any CAS object.
  - `/v1/archives/{hash}` and `/v1/manifests/{hash}` are convenience aliases.

- **Direct `.git` install** (`rust/src/client.rs`)
  - `Client::install_repo` downloads all prebuilt artifacts and writes `.git/`,
    the object packs, and the working tree without running `git init`,
    `index-pack`, `read-tree`, or `update-index`.

- **Removed client-side HEAD blob pack generation** (`rust/src/extract.rs`)
  - The temporary loose-object/pack helper that ran on every extraction is gone;
    blob objects now arrive in the server-built head-blobs pack.

- **Updated scripts**
  - `scripts/e2e_archive.sh` now tests the direct-install path end-to-end.
  - `benchmark/archive.sh` reports artifact sizes and install time.

## Removed / reversed

- **CoW extracted-tree cache** was implemented and then removed. For repos the
  size of `oven-sh/bun`, copying 15k files with APFS clonefile was slower than
  re-extracting the local zstd archive, so the `--cache-dir` option was dropped.
