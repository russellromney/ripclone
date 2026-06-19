# ripclone roadmap

> Goal: the fastest possible way to clone a GitHub repo and be ready to work on it (`git status`, `git diff`, `git edit`) without rebuilding git or leaving GitHub.
>
> This repo is the **headless open-source backend**: the Rust server, the CLI, the archive format, and the GitHub Actions trigger. Billing, workspaces, and the web UI live in the separate `ripclone-cloud` project.
>
> See `CHANGELOG.md` for what is already implemented.

## Current focus

The next four changes (from the cloud session) are, in order:

1. **Signed chunk URLs in the ref response.** Generate presigned storage URLs at ref-lookup time and return them alongside the clonepack manifest, so the client downloads chunks directly from object storage instead of knocking the gateway for every chunk.
2. **Per-request GitHub token on `/sync`.** Accept `X-GitHub-Token` to override the env var for a single sync, enabling multi-tenant private repos without giving the backend a god token.
3. **Shared ref store.** Move the repo→ref mapping from a local JSONL file to a trait-backed store with a local default and an object-store implementation, so multiple backends can share state.
4. **Async `/v1/build` with GitHub OIDC verification.** Verify the OIDC token from the Actions workflow, return `202 Accepted`, and queue the build in the background.

See the detailed plan at the end of this document. Completed work has been moved to `CHANGELOG.md`.

## The bet

Trade server-side compute and storage for client-side clone speed. The server pre-builds self-contained, immutable artifacts for the commits agents actually hit. The client downloads them and lays them down with minimal work.

Artifacts per commit (the "clonepack") are all protobuf (or raw bytes for the compressed chunks):

1. **Clonepack manifest** — small protobuf listing the metadata chunk and archive chunk refs.
2. **Metadata chunk** — protobuf containing skeleton pack/`.idx`, HEAD blobs pack/`.idx`, prebuilt `.git/index`, frame table, and file table.
3. **Archive frame chunks** — raw zstd frames grouped into 1–8 MB content-addressed chunks.
4. **Frame table** (inside metadata chunk) — maps each zstd frame to chunk index, chunk offset, compressed length, and raw length.
5. **File table** (inside metadata chunk) — maps each path to mode, blob SHA-1, and fragments.
6. **Delta chunks** (future) — append-only changed blobs/frames; older commits reference ranges in shared chunks.

This is **clonepack-first**. The client critical path is: resolve ref → fetch clonepack manifest → fetch metadata chunk → write `.git/` → stream archive frame chunks → extract working tree.

## Storage model

**Cloud object storage is the source of truth.** S3 / R2 / Tigris stores every built artifact durably. The local NVMe disk on the ripclone server is a **ring-buffer hot cache** for recently built and recently accessed artifacts.

Artifacts per commit:

- A clonepack manifest for every retained commit.
- Full metadata chunks for `HEAD` of every tracked branch.
- Archive frame chunks shared across commits (content-addressed).
- Delta chunks for recent commits, compacted into larger chunks over time.
- CAS/object pool for cross-commit blob/frame dedup and lazy sparse reads.

Local cache policy:

- Keep all skeletons and indexes (small).
- Keep archives for HEAD of tracked branches.
- Keep archives for recently built/accessed commits (LRU/LFU).
- Evict oldest to cloud when local disk is ~80% full.
- Serve cloud-backed artifacts via signed URLs or proxied range GETs.

## Why this wins

- **One protobuf manifest per commit, signed-URL chunks for data.** The manifest is tiny and cacheable; large streams are fetched in parallel or streamed.
- **No client-side git setup.** No `git init`, `git index-pack`, `git read-tree`, or `git update-index`.
- **Parallel download and decompression.** Frames are independent; large archives can be fetched with ranged GETs directly from cloud storage.
- **Agent-ready git repo.** After extraction `git status` is clean, `git diff` works, and the agent can edit and commit from HEAD using normal git.
- **Immutable and CDN-friendly.** Every artifact is content-addressed and cacheable forever.
- **GitHub Actions trigger.** No GitHub App webhook setup; a lightweight workflow notifies ripclone on every push.
- **Tiered storage.** Hot commits stay on local NVMe; everything else lives cheaply in object storage.

## What this is not

- It is not a byte-for-byte `git clone`. It has one commit, no remote refs, no tags, and no history beyond HEAD. It is functionally equivalent for agent read/edit/commit workflows.
- It is not a commit/push proxy. Agents push to GitHub directly with their own tokens. Ripclone only accelerates reads.
- It is not the absolute fastest clone possible. Reflink/COW copies and FUSE mounts can be faster. This is the fastest practical *download-and-extract* design for standard client environments.
- It does not eliminate lazy loading. Files not in the archive are fetched lazily from the CAS.

## Build trigger model

Builds are triggered by a lightweight **GitHub Actions workflow**, not by a GitHub App webhook. This avoids public webhook endpoints and per-repo webhook setup.

### Workflow

A reusable workflow (or a small inline workflow) runs on every push. Set `RIPCLONE_URL` as a repository variable and `RIPCLONE_TOKEN` as a repository secret.

```yaml
name: ripclone cache
on: push
jobs:
  notify:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
    steps:
      - name: Get OIDC token
        id: oidc
        run: |
          token=$(curl -fsSL \
            -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
            "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=${{ vars.RIPCLONE_URL }}" | jq -r '.value')
          echo "id_token=$token" >> "$GITHUB_OUTPUT"
      - name: Notify ripclone
        run: |
          curl -fsSL -X POST \
            -H "Authorization: Bearer ${{ steps.oidc.outputs.id_token }}" \
            -H "X-Ripclone-Token: ${{ secrets.RIPCLONE_TOKEN }}" \
            -H "Content-Type: application/json" \
            -d '{"owner":"${{ github.repository_owner }}","repo":"${{ github.event.repository.name }}","commit":"${{ github.sha }}","ref":"${{ github.ref }}"}' \
            "${{ vars.RIPCLONE_URL }}/v1/build"
```

Ripclone verifies the OIDC token with GitHub, validates the ripclone token, then queues the build asynchronously and returns `202 Accepted`.

**Why Actions:**

- No public ingress required.
- No GitHub App webhook configuration.
- Users control triggers via workflow YAML (branch filters, path filters, etc.).
- GitHub handles retries and observability.

**Caveats:**

- Requires a workflow file in each repo (or an org-level required/reusable workflow).
- If Actions are disabled, builds fall back to on-demand when a clone request arrives.
- Build endpoint must verify OIDC tokens and ripclone tokens; public endpoints need IP rate limiting.

## Clonepack format

A commit is represented by a **clonepack manifest** plus content-addressed **chunks**.

### Chunks

```
metadata-<hash>        protobuf: skeleton.pack + skeleton.idx + head-blobs.pack + head-blobs.idx + index + frame table + file table
archive-<hash>         one or more zstd frames (1–8 MB compressed, raw bytes)
```

- The metadata chunk bundles all `.git` artifacts and the file/frame tables into one protobuf object. It is usually small (< 8 MB).
- Archive chunks are append-only logs of zstd frames. Recent commits share unchanged frames by hash.
- Every chunk is content-addressed (SHA-256) and stored in the CAS / object storage.

### Clonepack manifest

Returned by the ref endpoint, the clonepack manifest is a protobuf that lists:

- `metadata_chunk`: hash and byte length of the metadata chunk.
- `archive_chunks`: content-addressed refs for each archive chunk.

The metadata chunk contains the frame table and file table, so the client can start materializing files as soon as it arrives. Archive chunks hold the actual zstd frames and are shared across commits.

### Frames and ordering

- Frames are independent zstd compressed blocks.
- Directory-major order with top-level files first, so common files materialize early during streaming.
- Frame size targets fast parallel decompression; chunk size targets efficient range GETs.

## Server-side changes

### 1. Clonepack build

For every commit that gets a full clonepack:

- Build `skeleton.pack` + `.idx`.
- Build `head-blobs.pack` + `.idx`.
- Build prebuilt `.git/index`.
- Serialize skeleton pack/`.idx`, HEAD blobs pack/`.idx`, prebuilt index, frame table, and file table into a **metadata chunk** protobuf.
- Build working-tree zstd frames and group them into **archive chunks**.
- Write a **clonepack manifest** protobuf that points to the metadata chunk and archive chunks.
- Store every chunk and the manifest in the CAS / object storage.

Existing `rust/src/pack.rs` and `rust/src/archive.rs` already produce the underlying packs, index, and frames; the new work is chunking and manifest generation.

### 2. Object storage

- Canonical storage for clonepack chunks and manifests is object storage (S3 / R2 / Tigris).
- Local NVMe is a hot cache.
- Chunks are capped at ~8 MB so single PUTs suffice. The metadata chunk protobuf is usually small enough for a single PUT; if it ever exceeds the limit it can be split.

### 3. Build queue

- Async, bounded builders.
- Per-repo serialization so the same commit is not built twice.
- Latest-first prioritization during bursts.
- Configurable mirror depth (default 50 commits).
- Fallback to skeleton + lazy blobs or on-demand fetch if a clonepack is missing.

### 4. Ref entry

```json
{
  "commit": "df55ab7...",
  "parent_commit": "...",
  "default_branch": "main",
  "clonepack_manifest": "sha256:..."
}
```

The manifest is fetched first; it contains all chunk hashes and byte lengths.

### 5. Clonepack deltas and compaction

- Recent commits produce append-only delta chunks (changed blobs/frames).
- Older commits reference ranges in shared chunks.
- Background compaction merges small/old chunks into larger ones.
- After compaction, update stored ref entries to point to ranges in the new chunks and delete orphaned old chunks.

## Auth

Ripclone itself is read-only. Writes go to GitHub directly.

### Self-hosted deployments

- No `RIPCLONE_TOKEN` configured → instance is open (useful for local dev).
- `RIPCLONE_TOKEN` configured → server stores its SHA-256 hash. Clients can send either the raw token (hashed by the client) or a pre-hashed `RIPCLONE_TOKEN_HASH`.

```bash
Authorization: Ripclone <sha256_hex_of_token>
```

### CLI auth (recommended)

The CLI exchanges the admin-provided secret once for a short-lived JWT:

```bash
ripclone auth login https://ripclone.example.com
# stores JWT in ~/.config/ripclone/credentials
```

Subsequent requests use:

```bash
Authorization: Bearer <jwt>
```

A `/v1/auth/refresh` endpoint rotates the JWT.

### Private repos

- **GitHub App** (recommended): server owns the app private key, exchanges a JWT for an installation access token scoped to the repo, and uses that to sync.
- **PAT fallback**: server configured with `RIPCLONE_GITHUB_TOKEN`.

### Build endpoint auth

The `POST /v1/build` endpoint accepts a GitHub OIDC token from the Actions workflow and a ripclone token. Ripclone verifies the OIDC token with GitHub, validates the ripclone token, then queues the build.

## Retention and tiering

| Tier | Storage | Contents | Policy |
|---|---|---|---|
| Hot | Local NVMe | Skeletons, indexes, recently accessed archives | LRU/LFU eviction; keep HEADs hot |
| Warm | Object storage | All built artifacts | Source of truth; served via signed URLs or range-proxy |
| Cold | Deleted | Old commits beyond retention window | Rebuild on demand if requested |

Default retention (tunable once cost/access metrics exist):

- Keep HEAD of the default branch indefinitely.
- Keep HEAD of other tracked branches for a configurable window.
- Keep recently built/accessed commits (LRU/LFU) as local disk allows.
- Evict to object storage when local disk is full; delete cold cloud objects based on age/cost policy.
- Rebuild on demand if a client requests a missing commit.

Clients never see retention directly; a miss just means a slower on-demand build or a fallback to skeleton + lazy blobs.

## Operations and observability

The server must expose enough telemetry to tune retention, capacity, and performance without guessing:

- **Metrics endpoint** (`/metrics` or via `tracing`/OpenTelemetry): clone latency percentiles, artifact download size, cache hit/miss rate, build queue depth, build duration, sync duration, error rate.
- **Health/readiness endpoints**: `/healthz` (liveness) and `/readyz` (able to serve clones).
- **Build queue visibility**: current queue length, per-repo build status, last successful build timestamp.
- **On-demand fallback behavior**: if artifacts are missing, either build inline and wait, or return `202 Accepted` and let the client poll/retry.
- **Rate limiting and abuse protection**: per-IP and per-token limits on public endpoints.

These are required before running a real deployment, not afterthoughts.

## Client-side changes

### 1. Clonepack fetch and install

The client critical path:

1. Resolve the ref; receive a `clonepack_manifest` hash.
2. Fetch the manifest.
3. Fetch the metadata chunk and write `.git/` artifacts as bytes arrive.
4. Start `git checkout-index` as soon as the index + head-blobs pack are written.
5. Fetch archive frame chunks (parallel or streamed) and extract files while later frames download.
6. Write `.git/config` with the upstream GitHub URL.

No `git init`, `git index-pack`, `git read-tree`, or `git update-index`. `git push` is not configured; agents push to GitHub directly with their own tokens.

### 2. Streaming extraction

- Metadata chunk is small; write `.git` artifacts once it fully arrives.
- Archive frames are independent zstd blocks.
- Decompress and write files frame-by-frame while the network is still busy.
- Clear `skip-worktree` for materialized paths and set stat info via `git2`.

### 3. Overlay staging (Linux)

- Materialize files into a fast staging dir (`/dev/shm` or a mounted volume).
- Overlay-mount the staged tree at the target path.
- Falls back to direct extraction if overlay is unavailable or space is insufficient.

### 4. Update flow (future)

- Reject dirty working trees.
- Fetch the target commit’s clonepack manifest.
- Reuse local chunks already present in the client cache.
- Apply delta chunks or fall back to a full clonepack.

### 5. Integrity

- Verify every chunk hash against the manifest.
- Verify each extracted file against its manifest SHA-1.
- Use zstd frame checksums.
- Reject path traversal, escaping symlinks, and expansion bombs.

## Speedups and experiments

| Speedup | Approach | Status |
|---|---|---|
| Signed chunk URLs in ref response | Return presigned storage URLs so clients fetch chunks directly. | Current focus (#1). |
| Per-request GitHub token | `X-GitHub-Token` header overrides env var for one sync. | Current focus (#2). |
| Shared ref store | Move repo→ref mapping off local disk into a trait-backed store. | Current focus (#3). |
| Async `/v1/build` with OIDC | Verify GitHub OIDC token, queue build, return `202`. | Current focus (#4). |
| Clonepack format | Single manifest + chunked metadata/archive streams. | Shipped. |
| Clonepack deltas | Append-only chunks + background compaction (LSM-style). | Planned. |
| GitHub Actions build trigger | Use OIDC-verified workflow to queue builds on push. | Current focus (#4). |
| Local ring-buffer cache | Keep hot artifacts on NVMe; evict cold to object storage. | Partial; S3 cache + retention exists. |

See `CHANGELOG.md` for completed work.

## Phases

Phases 1–3 are complete. See `CHANGELOG.md` for the detailed list of shipped features.

### Phase 4: clonepacks, tests, auth, and observability (current)

This phase reworks artifacts into **clonepacks**: a single manifest returned by the ref endpoint, plus content-addressed chunks fetched via signed URLs. It also adds the operational fundamentals needed for a real deployment.

#### 4.1 Clonepack format

Shipped. See `CHANGELOG.md` for details.

- [x] Define protobuf clonepack manifest schema (metadata chunk + archive frame chunks).
- [x] Server builds metadata chunk protobuf containing skeleton pack/idx, head-blobs pack/idx, prebuilt index, frame table, and file table.
- [x] Server builds archive frame chunks (zstd frames grouped into 1–8 MB content-addressed chunks).
- [x] Ref endpoint returns `clonepack_manifest` hash instead of individual artifact hashes.
- [x] Client fetches protobuf manifest, then metadata chunk, then archive frame chunks.
- [x] Client streams metadata write and starts `checkout-index` before archive frames finish downloading.

#### 4.2 Clonepack deltas and compaction

- [ ] Append-only delta chunks per commit (new/changed blobs and frames).
- [ ] Manifests for older commits reference byte ranges in shared chunks.
- [ ] Background compaction merges old chunks and shifts manifest pointers.
- [ ] Retention policy deletes unreferenced chunks after compaction.

#### 4.3 Tests and CI

- [ ] Unit tests for CAS, manifest, archive round-trip, pack builder.
- [x] Integration tests: server + client clone + worktree with fixture repos.
- [ ] Negative tests: missing artifacts, auth failure, invalid repo, overlay fallback.
- [ ] GitHub Actions CI for this repo: `cargo test`, `cargo clippy`, `cargo fmt --check`, Docker build.
- [ ] End-to-end test of the consumer-repo Actions workflow.

#### 4.4 Auth

- [ ] `ripclone auth login` CLI command that exchanges a secret for a JWT.
- [ ] JWT refresh endpoint.
- [ ] OIDC token verification for GitHub Actions-triggered builds.
- [ ] GitHub App installation-token path for private repo sync.

#### 4.5 Metrics

- [ ] Atomic server metrics (ref lookups, sync/build latency, artifact bytes, cache hits, errors).
- [ ] Prometheus text format on `/metrics`.
- [ ] Client-side tracing spans for clone/worktree phases.

#### 4.6 Build trigger and queue

- [ ] `POST /v1/build` endpoint that queues artifact builds.
- [ ] Asynchronous build worker with per-repo serialization.
- [ ] Reusable GitHub Actions workflow using OIDC + ripclone token.
- [ ] On-demand build fallback when a requested commit is missing.

## Success metrics

| Metric | Hypothesis |
|---|---|
| Warm full clone of `oven-sh/bun` | < 3 s on 1 Gb/s after download; download-dominated on slower links. |
| Artifact download time for bun | Measured separately from disk writes. |
| Client setup + disk write time (after artifacts land) | < 500 ms. |
| Extraction + index setup for bun | < 3 s on NVMe/cloud SSD after benchmark. |
| `git status` after clone | clean. |
| `git diff <file>` after editing | works immediately. |
| Cold full-archive build time | server-side; not client-critical. |
| Delta apply for a typical commit | < 500 ms after download (future). |

## Cloud session build plan

The four changes below are ordered by bang-for-buck and implementation risk.

### 1. Signed chunk URLs in the ref response

**Goal:** eliminate the “one request per chunk” gateway traffic. The client resolves the ref once, gets presigned URLs for the metadata chunk and every archive chunk, and downloads directly from object storage.

**Key design points:**
- The stored `ClonepackManifest` in the CAS still contains only `hash` + `len`; it stays content-addressed.
- The ephemeral `/v1/repos/{owner}/{repo}/refs/{branch}` response gains new optional fields, e.g. `metadata_chunk_url` and `archive_chunk_urls`.
- Add `StorageBackend::signed_url(&self, hash: &str, expires_in: Duration) -> Option<String>`. Local storage can return a plain `/v1/artifacts/{hash}` URL or `None`; S3/Tigris/R2 return presigned URLs.
- Client prefers URLs when present, otherwise falls back to `/v1/artifacts/{hash}`.
- TTL should survive slow clones (start with 15–30 minutes; tune with metrics).

**Files touched:** `rust/src/storage.rs`, `rust/src/storage/s3_storage.rs`, `rust/src/storage/local_storage.rs`, `rust/src/server.rs`, `rust/src/client.rs`.

**Caveats:** ref responses must not be cached; signed URLs are a read window for private blobs; backend without signing support must fall back cleanly.

### 2. Per-request GitHub token on `/sync`

**Goal:** let a multi-tenant backend sync private repos under different GitHub tokens without a single env-var god token.

**Key design points:**
- Accept `X-GitHub-Token` header on `/v1/repos/{owner}/{repo}/sync`.
- Header value overrides `RIPCLONE_GITHUB_TOKEN` for exactly one sync.
- Pass the token through to `git::sync_bare_mirror` (which already takes an optional token).
- Never log, persist, or return the token. Use `secrecy::SecretString` or custom redaction in tracing.
- If the header is absent, keep the existing env-var behavior.

**Files touched:** `rust/src/server.rs` (sync handler), `rust/src/git.rs` (already accepts token), tracing configuration.

**Caveats:** logging middleware must be audited; error responses must not echo the token.

### 3. Shared ref store

**Goal:** move the repo→`RefInfo` mapping off local disk so multiple backends can share state.

**Key design points:**
- Define a `RefStore` trait:
  ```rust
  async fn load(&self, owner: &str, repo: &str) -> Result<Option<RefInfo>>;
  async fn save(&self, owner: &str, repo: &str, info: &RefInfo) -> Result<()>;
  async fn list(&self) -> Result<Vec<(String, String)>>;
  ```
- Default implementation keeps the current JSONL file on disk.
- Optional object-store implementation stores one small object per repo (e.g. `refs/{owner}/{repo}.json`) in the configured storage backend.
- Use conditional writes / ETag when available to avoid losing newer commits to older ones.
- Cache reads in memory with a TTL; writes always go to the store first.

**Files touched:** new `rust/src/ref_store.rs`, `rust/src/server.rs` (wire up trait, replace direct file I/O), `rust/src/lib.rs` (server state).

**Caveats:** concurrent syncs can race; object-store listing may be slow; migration path needed for existing single-node deployments.

### 4. Async `/v1/build` with GitHub OIDC verification

**Goal:** provide an authenticated, non-blocking build trigger from GitHub Actions.

**Key design points:**
- `POST /v1/build` accepts a JSON body and an OIDC token (e.g. in `Authorization: Bearer <oidc>` or a dedicated header).
- Verify the JWT locally against GitHub’s JWKS (`https://token.actions.githubusercontent.com/.well-known/jwks`). Cache and refresh the JWKS.
- Validate `iss`, `aud`, `repository`, and `exp`. Allow small clock skew.
- Also require the existing Ripclone token (`Authorization: Ripclone <hash>` or `Bearer <jwt>`) for backend auth.
- Return `202 Accepted` immediately and enqueue the build.
- Worker task picks up queue items, syncs the repo, builds the clonepack, and updates the shared ref store.
- Store build status (`queued`/`building`/`done`/`failed`) in `RefInfo` so a future status endpoint can report it.
- Add metrics: queue depth, build duration, build failures.

**Files touched:** `rust/src/server.rs` (new endpoint + worker), `rust/src/git.rs` (pass token), `rust/Cargo.toml` (JWT crate), new `rust/src/oidc.rs` or inline verification, `rust/src/ref_store.rs`.

**Caveats:** JWT verification is security-critical; in-memory queue loses jobs on restart unless state is persisted; duplicate push events can enqueue the same commit; self-hosters must allow outbound access to GitHub JWKS.

### Recommended order

1. **Signed chunk URLs** — highest payoff, lowest risk.
2. **Per-request GitHub token** — tiny change, unblocks private multi-tenant use.
3. **Shared ref store** — required before running more than one backend.
4. **Async `/v1/build` with OIDC** — most work, but on the roadmap and needed for push-triggered cloud builds.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Storage cost explodes | Full artifacts only for HEAD; deltas for recent commits; tier to cheap object storage; evict cold artifacts. |
| Build latency on push | Bounded async queue with latest-first priority; fallback to skeleton/full pack if archive not ready. |
| zstd worse than git deltas for some repos | Measure per repo; serve full git pack when zstd archive is larger. |
| Many-small-files overhead | Batched directory creation; parallel decompression; prebuilt index. |
| Manifest too large | Protobuf from the start; path compression if needed. |
| Archive corruption | Hash verification + per-file SHA-1 check + zstd checksums. |
| `.gitattributes` eol handling | Store canonical blob bytes; apply eol transformations during extraction; distinguish `blob_oid` from `payload_hash` in the manifest. |
| Public endpoint abuse / DDoS | IP rate limiting, token verification, and abuse monitoring on all public endpoints. |

---

## Notes

- See `CHANGELOG.md` for completed work.
- This roadmap only tracks current and upcoming work.
