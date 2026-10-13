# Ripclone integration / backend map

Repo: ripclone worktree at origin/main 85cf0e0. Read-only mapping. Line counts from `wc -l` (include tests in-file unless noted).
Paths are relative to `rust/` unless noted.

Status: complete.

## Global facts

- `src/server.rs` is 10,968 lines: production code is lines 1-7918, `mod tests` starts at 7919 (~3,050 test lines).
- Lines 56-805 of server.rs (~750 lines) are test-hook infrastructure compiled into the production binary (`ArtifactBarrier`, `AdmissionTestProbe`, `TestStage` with ~30 variants, `test_hook()` called 44 times in server.rs, 32 in client.rs). Gated at runtime by `RIPCLONE_TESTING=1`.
- Binaries (`Cargo.toml`): `ripclone` (cli.rs 1,894), `ripclone-server` (server.rs bin 63), `ripclone-worker` (408), `ripclone-proxy` (250), `writer_bench` (383).
- Total `rust/src` = 47,121 lines.

---

## 1. PROVIDERS

Files:
- `src/provider.rs` 914 lines (prod 1-546, tests 547-914).
- `src/provider_config.rs` 122 (prod 1-48).
- `src/validation.rs` 236 (provider-aware `validate_repo_path` at :41-60).
- `src/config.rs` 506 (TOML `[providers.*]` tables, `default_provider`).
- CLI `provider add/list/rm/test` subcommand: `src/bin/cli.rs:156-158, 222-258`, `run_provider_add` ~:800-860.

What the abstraction is: data only. `ProviderKind` = GitHub | GitLab | Gitea (also forgejo/codeberg aliases) | Generic (`provider.rs:34-63`). A `ProviderInstance` is `{id, kind, host, auth_template, auth_header_name}` (`:100-113`). It implements exactly two behaviors:
- `clone_url(path)` (`:123-131`) — `https://host/path.git`.
- `auth_header(token)` (`:140-181`) — GitHub `Basic x-access-token:`, GitLab `Basic oauth2:`, Gitea `token X`, Generic = template only.

There is no per-provider ls-remote, fetch, or API trait. All git transport is generic `git` subprocess using `http.extraHeader` (`git.rs:1295-1330` `upstream_url_and_auth`; `git.rs:1453+` `ls_remote_tip`). The kind only selects the auth header shape and the webhook adapter.

Selection:
- Registry = built-in `github` instance (`provider.rs:284-302`) + `RIPCLONE_PROVIDERS` JSON (`provider_config.rs:24-30`) + TOML `[providers]` (`config.rs`).
- Server routes are provider-qualified `/v1/repos/{provider}/{path...}` via catch-all (`server.rs:1366-1369`, `resolve_repo_id` :1162). CLI accepts `provider:path` or uses `default_provider` (`cli.rs:271-293`, `:916-920`).
- Repo identity `RepoId{provider, path}` (`provider.rs:406-480`) with escaped storage key `{provider}/{escaped}` used by control DB and mirror dir names.

Special-casing of provider kind/name outside provider.rs (non-test):
| Site | What | Note |
|---|---|---|
| `server.rs:4425-4478` `preflight_repo_size_bytes` | GitHub REST `GET /repos/{o}/{r}` to read `size` for first-build size class | GitHub only; GHE derives `https://{host}/api/v3`. Other providers get `None` -> largest class. |
| `server.rs:4755-4912` `build_handler` (`/v1/build`) | Hard-codes `RepoId::github(...)` and `provider_registry.get("github")` (:4831-4837) | GitHub Actions OIDC trigger. GitHub-only endpoint. |
| `server.rs:3689-3692`, `:4008-4011` | `github_owner_repo()` to fill `owner`/`repo` response fields; non-GitHub falls back to `(provider, path)` / empty | Legacy response shape. |
| `server.rs:5021-5025` `webhook_repo_allowed` | Accepts `github/owner/repo` in addition to bare `owner/repo` | Allowlist asymmetry for the default instance. |
| `auth/broker.rs:291` | GitHub App broker serves only `is_github_default()` | |
| `validation.rs:43-49` | Strict owner/repo validation only for default github | Others: opaque path with minimal checks. |
| `git.rs:1305`, `client.rs:444-455` | `RIPCLONE_ORIGIN_BASE` override only for default github | Test/bench hook to point github at a local bare repo. |
| `client.rs:1552-1559, 1567` | Client default `ProviderInstance` hard-coded to github | `with_provider` drops the instance for non-github. |
| `client.rs:3092-3096` | If server omits `origin_url`, client synthesizes `https://github.com/...` | Fallback for old servers. |
| `cli.rs:283-291` | GitHub repos normalized to `owner/name` | |
| `cli.rs:469` | `ripclone update` hits GitHub releases API | Not a provider integration; release check. |
| `webhook/mod.rs:79-88` | Kind -> webhook adapter | Expected dispatch. |

Verdict: the provider abstraction is thin and mostly uniform for the core build/clone path (fetch, ls-remote, mirror, artifacts all go through `clone_url`+`auth_header`). GitHub special-cases are at the edges: size preflight, `/v1/build` OIDC, GitHub App broker, response `owner/repo` fields, and test origin override.

Who needs it: OSS self-host (GitLab/Gitea users), tests (`tests/e2e_gitea_provider.rs`, `scripts/e2e_full_topup_gitea.sh`). The hosted service is UNVERIFIED (private repo) but the CLI defaults to github.

Cost: ~600 prod lines total (provider.rs ~546 + provider_config ~48 + a few validation lines) plus the CLI `provider` subcommand (~150 lines). Low branching.

Tentative: **keep**. It is cheap and generic. Simplify: drop the `Generic` header-name override if unused; replace `owner/repo` response fields with `provider/path` only.

---

## 2. AUTH

Files (prod lines / total):
- `src/auth/broker.rs` 412 / 786 — `CredentialBroker` trait, `StaticBroker`, `GitHubAppBroker` (RS256 app JWT -> installation token, cached), `ProviderAwareBroker`, `broker_from_env`.
- `src/auth/access.rs` 188 / 220 — `HttpAccessVerifier`: per-repo read authz via anonymous then credentialed `info/refs` probe against the provider, 60s TTL cache.
- `src/auth/jwt.rs` 165 / 285 — HS256 session tokens for `ripclone auth login`.
- `src/auth/token_store.rs` 113 / 156 — CLI file token store (`~/.config/ripclone/...`).
- `src/oidc.rs` 160 — GitHub Actions OIDC JWKS verifier (issuer hard-coded to `token.actions.githubusercontent.com`).
- `src/job_token.rs` 141 / 191 — HMAC `rcjt1.` worker tokens; `mint-worker-token` CLI.
- server.rs auth code: `auth_middleware` :1488-1524, `check_bearer_token` :1534, login page HTML + handlers :1542-1808 (~270 lines), `check_auth_header` :1809-1829, rate limiter :933-1018 + :1831-1925, `authorize_worker_token` :1956-2004, `authorize_repo_read` :3847-3870, `build_handler` OIDC :4755-4912, token hash startup :7691-7760.
- CLI: `run_login` device flow `cli.rs:519-638` (talks to `/cli/device`, an endpoint NOT in the OSS router -> hosted-only), `run_auth_login` :672-~800 (browser loopback to OSS `/login`), `auth status/logout`, credential precedence :940-975.

Auth mechanisms the server accepts (by route group, `server.rs:1362-1432`):
1. Protected routes (`/v1/repos/*`, `/v1/artifacts/*`, `/v1/git/*`, admin config, clone metrics): shared server token in one of three encodings — `Ripclone <sha256-hash>`, `Basic user:<hash>` (for vanilla git smart-HTTP), or `Bearer <session JWT>` (`:1495-1500`, `:1809-1829`).
2. Then per-repo read authz on repo reads: `authorize_repo_read` (:3847) -> `AccessVerifier` Public / PrivateAuthorized / Denied, OR bypass when `RIPCLONE_TRUST_GATEWAY=1` (then privacy comes from a client header, `visibility_is_private` :3805).
3. Upstream credential selection per request/job: `X-Upstream-Token` header > GitHub App installation token (github only) > configured provider token > anonymous (`broker.rs:53-63, 279-298, 379-397`). Jobs also persist a per-job credential in the queue and ship it to API workers in the claim response (`api_job_queue.rs` header comment).
4. Worker routes `/v1/refs`, `/v1/jobs/*`: HMAC job token, secret from `RIPCLONE_JOB_TOKEN_SECRET` or derived from raw `RIPCLONE_SERVER_TOKEN` (`job_token.rs:54-68`; `server.rs:1956`).
5. `/v1/build`: GitHub OIDC bearer + `X-Ripclone-Token` (:4772-4830). Returns 501 unless `RIPCLONE_OIDC_AUDIENCE` set.
6. `/webhooks/{provider}`: provider HMAC/secret (`webhook/*`).
7. `/v1/auth/login`, `/login`: secret in body, rate-limited only.

Count: a protected repo-read request branches on 3 token encodings x {enforced authz, trust-gateway} x {request token, app token, static token, anonymous}. The server overall has 6 distinct credential schemes (shared token, session JWT, job token, OIDC, webhook HMAC, upstream provider credential) plus the authz verifier.

Notes:
- `token_hash` is always `Some` in the real server (startup refuses without a token, :7691-7720), so every `if let Some(expected) = &state.token_hash` branch (:1495, :1913, :4792) only matters in tests.
- `ServerState::for_worker` (:860-905) fills auth fields with dummies; the worker never serves HTTP.
- The CLI has two login commands: `ripclone login` (device flow to the hosted default `https://ripclone.com`, `cli.rs:18, 928-933`) and `ripclone auth login` (OSS session JWT). `login` falls through to `auth login` for non-default servers.

Who needs what:
| Piece | OSS self-host | Hosted | Notes |
|---|---|---|---|
| Shared server token (Ripclone/Basic) | yes | UNVERIFIED (hosted gateway likely fronts its own auth) | Core. |
| Session JWT + `/login` page + `auth login/refresh/status` (~270 server lines + jwt.rs 165 + CLI ~150) | optional nicety | no | Lets users avoid storing the raw secret. |
| Device-flow `ripclone login` (cli ~120 lines) | no | yes | Server side lives in ripclone-cloud. |
| AccessVerifier per-repo authz | yes for multi-user self-host with private repos | UNVERIFIED | `TRUST_GATEWAY` name suggests a gateway in front. |
| GitHub App broker (~230 prod lines) | yes for org self-host | likely (UNVERIFIED) | |
| Job token | only with API workers | yes (farm-out workers per job_token.rs:3) | |
| OIDC `/v1/build` (oidc.rs 160 + handler ~160) | only with GitHub Actions trigger | UNVERIFIED | Overlaps with webhooks + poller. |
| Rate limiter | yes | probably gateway-level | |

Tentative:
- Shared token: **keep**.
- Session JWT/login page: **simplify or cut** — a second credential system for one shared secret; a self-hoster can keep the token in env/config.
- Device flow: **move** to hosted-only client path or keep as tiny CLI branch (it is client-only).
- AccessVerifier: **keep** (security fix AU1), but consider removing the `TRUST_GATEWAY` mode so there is one authz path.
- GitHub App broker: **keep** (real self-host need), it is isolated behind the broker trait.
- OIDC `/v1/build`: **cut** — webhooks + poller + `sync` cover triggering; it is GitHub-only and special-cases github in core admission.
- Job token: keep only if API worker is kept (see section 5).

---

## 3. CONTROL DATABASE

Files (prod / total):
- `src/control.rs` 918 / 1,358 — `ControlSettings` (env + TOML resolution, removed-config guards), `ControlDb::open`, schema check, claim-authorized publish (`publish_{head,full,files}_for_claim` :292-480, three near-duplicate ~60-line functions), repository config get/put (:482-540), atomic admission `admit_exact_and_job`/`admit` (:542-676), process lock `ControlPathLock` (:860-910).
- `src/meta/mod.rs` 203 / 328 — `SqlRefStore` (RefStore impl; CAS-retry update loop :28-62).
- `src/meta/libsql.rs` 196 — `LibsqlMeta`: `results`, `added_repos` tables + queries.
- `src/queue/mod.rs` 198 — `BuildJob`, `JobQueue`/`WorkerQueue` traits (two impls: `SqlJobQueue`, `ApiJobQueue`).
- `src/queue/sql.rs` 841 / 2,766 — `SqlJobQueue` (claim, capped claim, ack with retry backoff, stale reclaim, heartbeat registry, prune).
- `src/queue/libsql_db.rs` 556 — `LibsqlDb`: raw SQL for jobs/workers.
- `src/queue/size_class.rs` 189 / 372 — see section 5.
- `src/ref_store.rs` 90 — `RefStore` trait (impls: `SqlRefStore`, `ApiRefStore`, `ClaimScopedRefStore` in server.rs:7095-7240, test stores).

Backends: one driver, `libsql` (git-pinned prerelease rev `c942c82`, `Cargo.toml` libsql line, with a comment about a double-close bug). Two modes:
1. Plain local SQLite file (default path `<repo_root parent>/control.db`, or `RIPCLONE_CONTROL_DB_PATH` / `[control].path` / `--control-db`).
2. Turso embedded replica at the same path when both `RIPCLONE_TURSO_DATABASE_URL` and `RIPCLONE_TURSO_AUTH_TOKEN` are set (`control.rs:60-80`).

Branches on backend: exactly one site, `ControlDb::open` (`control.rs:215-249`): `Builder::new_local` vs `Builder::new_remote_replica(...).read_your_writes(true).sync_interval(1s)` + bootstrap `sync()`, and WAL pragma only for local. Plus a log field (`server.rs:7782`). No query-level branching. Module doc comments still say "over the server's Turso embedded-replica handle" (`meta/libsql.rs:1`, `queue/libsql_db.rs:1`) even though the handle is usually plain SQLite.

Remnants of other backends: no postgres/mysql/redis code. `control.rs:103-121` `REMOVED_ENVIRONMENT` lists 16 removed env vars (`RIPCLONE_METADATA*`, `RIPCLONE_QUEUE*`, `RIPCLONE_DISPATCH*`, `RIPCLONE_HEARTBEAT_URL`, ...) and `validate_removed_config` (:170-183) rejects removed `[metadata]`/`[queue]` TOML. These are fail-loud guards for old configs; ~80 lines incl. `validate_worker_environment` (:140-168).

Schema: `SCHEMA_VERSION = 3` (`control.rs:17`). No migrations: an existing DB with a different version or without the `control_schema` marker fails closed (`preflight_sqlite_schema` :726-784, `validate_or_initialize_schema` :786-859). DDL is spread over three owners: `control.rs` (control_schema, repository_configs), `meta/libsql.rs:37-60` (results, added_repos, `IF NOT EXISTS`), `queue/sql.rs:790-835` (jobs, workers, indexes, `IF NOT EXISTS`). SQL text lives in 4 files (control.rs 22 statements, meta/libsql.rs 11, queue/libsql_db.rs 30, queue/sql.rs 6 constants); control.rs reads/writes the `results` and `jobs` tables directly, bypassing the meta/queue layers, for the atomic admission and claim-authorized publish transactions.

Layering: `SqlRefStore` -> `LibsqlMeta` and `SqlJobQueue` -> `LibsqlDb` are leftover two-layer splits from when `MetaDb`/`QueueDb` traits had several impls (commit 935ecd0 inlined the traits but kept both layers).

What Turso adds: off-box durability / read replicas of the control DB for the hosted deployment (UNVERIFIED that hosted uses it; Fly files exist in `tests/fly`). Cost: ~35 lines of code, two env vars + two TOML keys, and it is the reason the project uses libsql (a git-pinned prerelease dependency) instead of plain rusqlite. Compile-time/binary-size cost of libsql vs rusqlite is UNVERIFIED.

Dead code found:
- `SqlJobQueue::live_worker_count{,_at}`, `live_worker_count_capable{,_at}`, `pending_by_class` (`queue/sql.rs:385-455`) have no non-test callers (only `tests/e2e_token_only_worker.rs` uses `live_worker_count`). They fed the deleted dispatcher.
- `SqlJobQueue::prune_failed` (`queue/sql.rs:457`) is never called in production: the server's embedded worker loop never prunes, and the API worker calls `ApiJobQueue::prune_failed` which is a no-op (`api_job_queue.rs:339-343`). So `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` has no effect and failed jobs are never pruned.
- `LibsqlMeta::connect` / `LibsqlDb::connect` (path-based openers) are test-only (callers only in `mod tests` regions).

Who needs it: everyone (control DB is core). Turso: hosted only (UNVERIFIED).

Tentative:
- SQLite control DB: **keep**.
- Turso replica: **cut from OSS** (or keep behind a cargo feature). Litestream or volume snapshots cover self-host durability; dropping it allows moving to rusqlite and removing the git-pinned dep.
- Layering: **simplify** — merge `LibsqlMeta` into `SqlRefStore`, `LibsqlDb` into `SqlJobQueue`, collect DDL in one place; dedupe the three `publish_*_for_claim` functions.
- Dead worker-registry/prune code: **cut** or wire `prune_failed` into the server.

---

## 5. WORKERS

Embedded workers (default): `spawn_durable_build_worker` `server.rs:7442-7606` (~165 lines). `RIPCLONE_BUILD_CONCURRENCY` slots (default 2, :7423), each claims directly from `SqlJobQueue`, wakes on an in-process `Notify` from admission or a 250ms poll, heartbeats active claims, and runs `process_build_job_with_foreground_release` (a slot can start the next Head while Full/Files continue).

Standalone API worker `src/bin/ripclone-worker.rs` 408 lines (prod ~360):
- Talks only HTTP: `ApiJobQueue` (`src/api_job_queue.rs` 383 prod / 466) for `/v1/jobs/claim|ack|heartbeat`, and `ApiRefStore` (`src/api_ref_store.rs` 466 prod / 551) for `GET/POST /v1/refs`. Server side: `job_claim_handler` :2031-2093, `job_ack_handler` :2094-2175, `job_heartbeat_handler` :2176-2212, `ref_read_handler` :2349-2394, `ref_report_handler` :2395-2547, manifest validation :2213-2348. Total server-side ~520 lines.
- Auth: job token (section 2). `validate_worker_environment` refuses DB credentials (`control.rs:140`).
- Claim response ships the per-job upstream credential to the worker (`api_job_queue.rs` claim shape).
- Lifecycle flags: `--max-size-class`/`RIPCLONE_MAX_SIZE_CLASS`, `--idle-exit-secs`/`RIPCLONE_IDLE_EXIT_SECS` (scale-to-zero), `--max-jobs`/`RIPCLONE_MAX_JOBS` (one-shot, "e.g. Lambda"), `--idle-poll-ms`, `--cas-dir`, `--repo-root`.
- Heartbeat knobs: `RIPCLONE_WORKER_HEARTBEAT` (idle registry, default off), `RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS` (default 60), `RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS` (default timeout/3). Active-claim renewal is unconditional.
- Stale docs: the header says `RIPCLONE_QUEUE_STALE_SECS` and `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` configure the worker (`ripclone-worker.rs:14-18`), but `ApiJobQueue` reads neither; they only matter on the server, and failed-retention is not wired at all (section 3).
- Comment at `ripclone-worker.rs:118` references "the cloud reconcile cron": idle-exit was built for the hosted service's scale-to-zero fleet.

Size classes (`src/queue/size_class.rs`, 189 prod lines; TOML `[control].size_classes`): ordered `{name, max_bytes, machine}`; default `small` (1 GiB) / `large`. `machine` label is "for later cloud dispatch" and unused in OSS (:22-25). Jobs get a class from prior clonepack bytes or the GitHub size preflight (`server.rs:4516-4530`). The only consumer is `--max-size-class` on API workers (`server.rs:2055`). Embedded workers claim everything.

Who needs the API worker today: nobody in OSS self-host needs it for a single box; it matters for (a) splitting builds across machines, and (b) the hosted service's farm-out fleet (job_token.rs:3 "Cloud/farm-out workers", "cloud reconcile cron"). With the dispatcher deleted, nothing in this repo launches API workers; operators must run them by hand. Tests: `tests/e2e_token_only_worker.rs`.

Cost of API worker path: worker bin ~360 + api_job_queue ~383 + api_ref_store ~466 + server endpoints ~520 + job_token ~141 + size_class ~189 + idle/heartbeat registry code in queue/sql.rs (~150) ≈ 2,200 prod lines. It also forces `ClaimScopedRefStore` (`server.rs:7095-7240`) and the claim-authorized publish paths in control.rs, since results from a remote worker must be checked against the claim. (The embedded worker also uses claim-scoped publish, so part of that cost stays.)

Tentative:
- Embedded workers: **keep**.
- API worker: **keep only if the hosted service uses it** (likely; UNVERIFIED). If kept, move hosted-only knobs (size-class `machine`, idle registry, idle-exit) to the hosted repo or behind one "fleet" doc section. If the hosted service does not use it, **cut** — ~2,200 lines and the job token.
- Idle worker registry (`RIPCLONE_WORKER_HEARTBEAT`, `live_worker_count*`): **cut** — its consumer (dispatcher) is gone.

---

## 4. ARTIFACT STORAGE

Files (prod / total):
- `src/storage/mod.rs` 194 / 297 — `StorageBackend` trait (get, get_range, open_file, put, put_async, put_file_async, size, verify_durable_copy, signed_url, is_remote, regions, health) and `LocalStorage` (:107-190), which is the CAS dir itself.
- `src/storage/s3_storage.rs` ~585 / 697 — `S3Storage` (crate `s3` 0.1): retries/timeouts (`RIPCLONE_S3_REQUEST_TIMEOUT_SECS`, `_MAX_ATTEMPTS`, `_MAX_RETRY_DELAY_MS`), multipart upload with backend-wide semaphore (:193-298), sync-over-async `block_on` bridge (:300-357), presigned GET (:537+). Config from `RIPCLONE_S3_{ENDPOINT,REGION,BUCKET,PREFIX}` (+ `BUCKET_NAME` fallback) or TOML `[storage]`.
- `src/cas.rs` 472 / 621 — local content-addressed store (always present: build scratch on S3 deployments, durable store on local deployments).
- `src/retention.rs` 249 / 336 — `LocalCacheRetention`: age/size eviction of the local CAS, enabled only when storage `is_remote()` (`retention.rs:25`). Env: `RIPCLONE_RETENTION_{INTERVAL_SECS,MAX_AGE_DAYS,MAX_GB}`.
- `src/backends.rs` 61 — chooses S3 if configured, else local (`backends.rs:37-49`).
- Server artifact path: `get_artifact`/`serve_artifact`/`artifact_body`/range parsing `server.rs:5098-5473` (~375 lines, of which ~60 are test barrier/fault-injection plumbing).
- Client side: `artifact_endpoint` (`client.rs:2122-2135`) picks signed URL (no-auth client) vs `/v1/artifacts/{hash}` (authenticated); optional client cache `RIPCLONE_CACHE_DIR` / `RIPCLONE_NO_CACHE` (`client.rs:1498-1530`).

Where it branches into the core path (all on `is_remote()` / `signed_url()` returning Some):
1. Ref responses: `build_ref_response` signs every manifest/chunk/pack URL when the backend can (`server.rs:3669-3717`); `None` means the client uses the gateway.
2. `serve_artifact` (`:5315-5323`): S3 -> 307 redirect to a presigned URL; local -> stream file with Range support.
3. `settle_storage` (`:5863-5874`): after upload to S3, delete uploaded objects from local CAS.
4. `LocalCacheRetention` active only for remote storage.
5. `/v1/repos/.../status` reports `regions()` (:4000); only S3 has a region.

That is 5 small, well-contained branches. The trait boundary is clean. Proxy-by-hash (`/v1/artifacts/{hash}` streaming) is the only path for local storage and the fallback when signing fails; the client comment at `client.rs:2140-2142` says the cloud no longer serves content by hash, so on S3 the by-hash route only redirects.

Who needs it: local = OSS single box + all tests; S3 = OSS at scale + hosted (UNVERIFIED for hosted but likely). Multipart: needed for >5 GiB objects (S3 single-PUT limit) and throughput.

Tentative: **keep** both. **Simplify**: `S3Storage` sync `block_on` bridge exists because the trait mixes sync and async methods; making the trait async-only would remove ~60 lines and a class of runtime-nesting bugs. `regions()` is a single-element vec for S3 and empty for local; it can go unless multi-region is planned.

---

## 6. TRIGGERS

All triggers end in the same admission core: `admit_commit` (`server.rs:4497-4540`) -> `enqueue_admitted_build` (:4541-4579) / `trigger_build` (:4600-4637). That part is shared and clean.

| Trigger | Code | Lines | Auth | Providers | Who needs it |
|---|---|---|---|---|---|
| `POST /v1/repos/{p}/{path}/sync` (CLI `sync`, `sync --at`) | `sync_repo_inner` :4026-4175, `sync_repo_at_revision` :4176-4335 | ~310 | shared token | all | core |
| `POST .../add` (CLI `add`) | `add_repo_inner` :4336-4393 + GitHub size preflight :4425-4483 | ~115 | shared token | all (preflight GitHub only) | core |
| Exact-ref lookup enqueue tail | in `get_ref_inner` :3308-3607 | part of ~300 | shared token | all | core |
| Webhooks `POST /webhooks/{provider}` | `webhook_handler`/`handle_webhook`/`webhook_dispatch_push` :4913-5097 (~185) + `webhook/` 240+115+75+118 prod ≈ 550 total | ~735 | provider HMAC (GitHub `X-Hub-Signature-256`, Gitea HMAC, GitLab shared token header) | GitHub, GitLab, Gitea/Forgejo; Generic none | OSS self-host that wants push-latency builds |
| Poller | `poll_once`/`spawn_poll_loop` :7607-7687 | ~80 | n/a | all (ls-remote via broker credential) | OSS self-host default (every 300s, `RIPCLONE_POLL_INTERVAL_SECS`, 0 disables) |
| `POST /v1/build` (GitHub Actions OIDC) | `build_handler` :4755-4912 + `oidc.rs` 160 | ~320 | GitHub OIDC JWT + `X-Ripclone-Token` | GitHub only (hard-coded) | Nobody documented: `docs/examples/github-actions-trigger.yml` now calls `/sync` with the shared token, and `docs/CHANGELOG.md:402` says the old `/v1/build` example was replaced. `RIPCLONE_OIDC_AUDIENCE` is not in CONFIG.md. |
| GitHub Actions example | `docs/examples/github-actions-trigger.yml` | 33 | shared token hash | GitHub | docs only; uses `/sync` |

Tentative:
- sync/add: **keep**.
- Poller: **keep** (cheap, self-healing).
- Webhooks: **keep** GitHub; GitLab/Gitea adapters are small (~75-118 lines each) and isolated behind one trait, so **keep** unless trimming for its own sake.
- `/v1/build` + `oidc.rs`: **cut** (~320 lines, a 4th trigger path, GitHub-only, undocumented, overlaps the Actions example that already uses `/sync`).

---

## 7. OTHER SURFACE

| Item | Where / lines | Purpose | Branches into core? | Who needs it | Tentative |
|---|---|---|---|---|---|
| `ripclone-proxy` bin | `src/bin/ripclone-proxy.rs` 250 | Latency/bandwidth shaping proxy for benchmarks | No | benchmarks only (`benchmark/latency.sh`, `matrix.sh`, `profile_one.sh`, `tests/fly/Dockerfile.client`). Also `benchmark/latency_proxy.py` duplicates the idea. | **move** to `benchmark/` or a dev-only crate/example; not shipped in release (`release.yml:65` builds only cli/server/worker) but maturin `bindings = "bin"` may package every `[[bin]]` into the wheel (UNVERIFIED). |
| `writer_bench` bin | `src/bin/writer_bench.rs` 383 | Worktree writer micro-benchmark | No | perf work only; `scripts/ci-build-artifacts.sh:42` builds it for `Dockerfile.client.ci` | **move** to `examples/` (like `examples/bench.rs`, `mem_probe.rs`). |
| `--temp` overlay clone | `src/overlay.rs` 188 prod; client use ~32 refs incl. `should_use_overlay` `client.rs:4049-4110`, mount at :3112, staging :2900-2910 | tmpfs overlay mount for ephemeral clones, Linux only | Yes, client install path (`overlay_dirs` option threaded through the install) | Agent/CI fleets; opt-in via `--temp`/`RIPCLONE_TEMP`; `RIPCLONE_NO_OVERLAY`, `RIPCLONE_STAGING_DIR` | **simplify or cut**: niche, Linux-only, needs mount privileges; UNVERIFIED that anyone uses it. Undocumented in CONFIG.md. |
| LSM history levels + compaction | `server.rs:5503-5622` (~120) + `pack.rs` `build_history_tail`/`compact_levels` (UNVERIFIED size, part of 807) + `HistoryLevel` in lib.rs; build at :6751-6797 | Incremental Full builds: reuse sealed history packs by hash | Yes, Full build path. `RIPCLONE_LSM=0` selects a second non-LSM path (:6764-6779). | Core perf for repeat syncs; test `tests/local_lsm.rs` | **keep LSM, cut the `RIPCLONE_LSM` toggle** (one path). Keep `RIPCLONE_LSM_MAX_LEVELS` as a constant unless tuned. |
| Smart-HTTP git fallback `/v1/git/*` | `server.rs:2608-2800` + `:2984-3101` ≈ 310 lines; Basic-auth branch in `check_auth_header` :1813-1826 and `WWW-Authenticate` branch :1504-1513 | Lets vanilla `git clone http://server/v1/git/...` work by running `git upload-pack` on the server's bare mirror | Own path; `ensure_mirror` (:3069) fetches upstream on the server | README.md:330 advertises it as a fallback. The CLI never uses it. With API workers the server has no warm mirror. | **cut** (or keep as explicitly unsupported). It is a second clone path with its own auth encoding and server-side fetch. |
| Prometheus metrics | `src/metrics.rs` 358 prod; `/metrics` route | Operator metrics | Recording calls sprinkled in server | OSS ops | **keep**. |
| Clone metrics (CLI -> hosted) | `src/clone_metrics.rs` 79 prod; `client.rs:3206-3260`; OSS sink `server.rs:1943` drops it | Hosted analytics; fires only when server returns `X-Ripclone-Clone-Id` | No (post-clone, fire-and-forget) | hosted only | **keep** (small) or move behind the hosted default check. |
| Perf/bench counters | `src/perf.rs` 145, `src/bench.rs` 118, `--bench`, `RIPCLONE_BENCH` | Per-phase timing report | Counters in client hot path (atomic adds) | dev/perf | keep (cheap). |
| Hidden CLI cmds | `cli.rs:161-210` defs + `:1231-1367` impl (~190 total): `build-archive`, `extract-archive`, `train-dictionary` | Manual archive tooling and zstd dictionary training | No | dev only. Dictionary support is half-built: `repo_config.rs:44-47` says "dictionary lookup is a follow-up"; server passes `None` for the dictionary at both archive build sites (`server.rs:~7010-7028`). | **cut** `train-dictionary` and dictionary plumbing in archive/extract; move the other two to an example or test helper. |
| Device-flow login / `ripclone update` | `cli.rs:519-638`, `:463-512` | Hosted login; GitHub releases check | No | hosted / all | keep (client-only). |
| Test hooks in prod binary | `server.rs:56-805` (~750 lines), 44 `test_hook` calls in server.rs, 32 test refs in client.rs, 28 `RIPCLONE_TEST_*` vars read in non-test code | Deterministic concurrency tests | Yes, inline in every build phase | tests only | **simplify**: compile under `#[cfg(any(test, feature = "test-hooks"))]` so production code carries no-op calls only. |
| Python/maturin | `pyproject.toml` 23 lines, `release.yml:117-160` | `pip install ripclone` ships binaries | No | distribution | keep. Note: release build deps still install `libgit2`, `fuse`, `openssl` (`release.yml` before-script-linux, mac brew) though `Cargo.toml` has no git2/fuser deps (stale). |
| Docker/Fly | `tests/fly/` (Dockerfile, Dockerfile.ci, Dockerfile.client{,.ci,.overlay}, docker-compose.yml, 4 fly*.toml), `scripts/fly_deploy.sh`, `fly_client_test.sh`, `trigger_fly_client_test.sh`, `benchmark/fly_shaped_benchmark.sh` | Benchmark and dev deployment on Fly | No | maintainer benchmarking; not a supported self-host artifact (lives under `tests/`). `tests/fly/Dockerfile` installs `libfuse-dev libgit2-dev` (stale). | **simplify**: one Dockerfile for self-host, keep Fly bits as maintainer scripts. |
| `spikes/` crate | `spikes/` (own Cargo.toml) | Experiments | No | nobody at runtime | **cut or archive** (UNVERIFIED contents not reviewed). |
| `admin/config` endpoints | `server.rs:4638-4754` (~115) + `repo_config.rs` 139 prod | Per-repo build settings (depth, chunk sizes, dictionary id) | Admission snapshots config | OSS | keep; drop `dictionary_id`. |

---

## 8. CONFIG

Method: regex `RIPCLONE_[A-Z0-9_]+` over `rust/src/**/*.rs`, excluding lines after each file's `mod tests`, excluding `RIPCLONE_TEST_*`/`RIPCLONE_TESTING`, excluding comment-only hits. Then whole-word match against `docs/CONFIG.md` (79 lines) and other docs.

Totals:
- 90 distinct names read in non-test code (excluding a test-only `RIPCLONE_GITHUB_APP_` prefix artifact).
- 16 are removed-config guards (`control.rs:103-121`): read only to fail startup. None documented in CONFIG.md.
- **74 active env vars** (one is a prefix family, `RIPCLONE_WEBHOOK_SECRET_<provider>`). **48 documented in CONFIG.md**, 8 only in other docs, **18 undocumented anywhere** in `docs/` (non-internal) or README.
- Plus 28 `RIPCLONE_TEST_*`/`RIPCLONE_TESTING` vars read in production code paths (test hooks).
- Plus non-prefixed env: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL_S3`, `BUCKET_NAME` (S3 fallbacks), `FLY_MACHINE_ID`/`HOSTNAME` (worker id), `HOME`, `RUST_LOG`.
- TOML (`config.rs`): `server`, `default_provider`, `agent`, `[clone] depth/mode`, `[providers.<id>] kind/host/token/auth_template/auth_header_name`, `[storage] backend/endpoint/region/bucket/prefix`, `[control] path/turso_url/turso_token/size_classes`; removed `[metadata]`, `[queue]` fail closed.

Legend: C = in CONFIG.md, D = other docs only, U = undocumented.

| Area | Vars |
|---|---|
| Server auth (7) | `SERVER_TOKEN` C, `SERVER_TOKEN_HASH` C, `JWT_SECRET` C, `JWT_TTL_SECS` C, `JWT_SESSION_MAX_SECS` C, `TRUST_GATEWAY` C, `JOB_TOKEN_SECRET` U |
| Rate limit / proxy (3) | `RATE_LIMIT_BURST` D, `RATE_LIMIT_PER_SEC` D, `TRUST_FORWARDED_FOR` C |
| GitHub App broker (5) | `GITHUB_APP_ID` U, `GITHUB_APP_INSTALLATION_ID` U, `GITHUB_APP_PRIVATE_KEY` U, `GITHUB_APP_PRIVATE_KEY_PATH` U, `GITHUB_API_BASE` U (only `docs/internal/GITHUB_INTEGRATION.md`) |
| OIDC (1) | `OIDC_AUDIENCE` U |
| Providers (2) | `PROVIDERS` C, `UPSTREAM_TOKEN` C (CLI) |
| Webhooks / triggers (3) | `WEBHOOK_SECRET_<id>` C, `WEBHOOK_ALLOWLIST` C, `POLL_INTERVAL_SECS` C |
| Control DB (4) | `CONTROL_DB_PATH` C, `TURSO_DATABASE_URL` C, `TURSO_AUTH_TOKEN` C, `SIZE_CLASSES` C |
| Queue (4) | `QUEUE_STALE_SECS` C, `QUEUE_MAX_ATTEMPTS` C, `QUEUE_RETRY_BACKOFF_MS` C, `QUEUE_FAILED_RETENTION_SECS` C (no effect, see section 3) |
| API worker (9) | `QUEUE_API_URL` C, `METADATA_REPORT_URL` C, `METADATA_JOB_TOKEN` C, `MAX_SIZE_CLASS` U, `IDLE_EXIT_SECS` U, `MAX_JOBS` U, `WORKER_HEARTBEAT` D, `WORKER_HEARTBEAT_INTERVAL_SECS` D, `WORKER_HEARTBEAT_TIMEOUT_SECS` D |
| Build tuning (6) | `BUILD_CONCURRENCY` U, `LSM` C, `LSM_MAX_LEVELS` C, `HEAD_REBASE_BYTES` C, `ARCHIVE_BOUNDED` U, `LS_REMOTE_TIMEOUT_SECS` D |
| Storage S3 (7) | `S3_ENDPOINT` C, `S3_REGION` C, `S3_BUCKET` C, `S3_PREFIX` C, `S3_REQUEST_TIMEOUT_SECS` U, `S3_MAX_ATTEMPTS` U, `S3_MAX_RETRY_DELAY_MS` U |
| Local cache retention (3) | `RETENTION_INTERVAL_SECS` C, `RETENTION_MAX_AGE_DAYS` C, `RETENTION_MAX_GB` C |
| Signed URLs (2) | `SIGNED_URL_TTL_SECS` C, `SIGNED_URL_TTL_PRIVATE_SECS` C |
| Client (15) | `SERVER` C, `CONFIG` C, `AGENT` C, `MODE` C, `CACHE_DIR` C, `NO_CACHE` C, `FETCH_MAX_ATTEMPTS` C, `FETCH_BACKOFF_MS` C, `FSYNC` C, `IO_URING` C, `NO_METRICS` C, `VERIFY_UPSTREAM` C, `BENCH` C, `NO_BROWSER` U, `ORIGIN_BASE` U (test/bench hook) |
| Overlay `--temp` (3) | `TEMP` U, `NO_OVERLAY` D, `STAGING_DIR` D |

Undocumented (18): `JOB_TOKEN_SECRET`, `GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_APP_PRIVATE_KEY_PATH`, `GITHUB_API_BASE`, `OIDC_AUDIENCE`, `MAX_SIZE_CLASS`, `IDLE_EXIT_SECS`, `MAX_JOBS`, `BUILD_CONCURRENCY`, `ARCHIVE_BOUNDED`, `S3_REQUEST_TIMEOUT_SECS`, `S3_MAX_ATTEMPTS`, `S3_MAX_RETRY_DELAY_MS`, `NO_BROWSER`, `ORIGIN_BASE`, `TEMP`.

A/B toggles that keep two code paths alive: `RIPCLONE_LSM` (LSM vs flat history), `RIPCLONE_ARCHIVE_BOUNDED` (bounded vs incremental archive), `RIPCLONE_TRUST_GATEWAY` (authz on/off), `RIPCLONE_IO_URING` (writer backend; legitimate platform fallback).

CLI flags:
- `ripclone-server` (`src/bin/server.rs`): `--cas-dir`, `--repo-root`, `--control-db`, `--host` (0.0.0.0), `--port` (8000). Everything else is env/TOML.
- `ripclone-worker` (`src/bin/ripclone-worker.rs:96-131`): `--cas-dir` (/data/cache), `--repo-root` (/data/repos), `--idle-poll-ms` (1000), `--max-size-class` (env), `--idle-exit-secs` (env), `--max-jobs` (env). Note default paths differ from the server's (`~/.local/share/ripclone/...`).
- `ripclone` CLI globals: `--server`/`RIPCLONE_SERVER`, `--provider`, `--token`/`RIPCLONE_UPSTREAM_TOKEN`. Subcommands: login, logout, auth {login,logout,status}, version, update, mint-worker-token, add, list, rm, sync, clone (`--branch --mode --depth --at --temp --bench --no-metrics --verify-upstream`), provider {add,list,rm,test}; hidden: build-archive, extract-archive, train-dictionary.

Tentative: **simplify** — remove the 16 removed-var guards after a release or two; remove the A/B toggles (`LSM`, `ARCHIVE_BOUNDED`); document or delete the 18 undocumented vars; move hosted-fleet knobs (`IDLE_EXIT_SECS`, `MAX_JOBS`, `WORKER_HEARTBEAT*`, size-class `machine`) out of the OSS surface if the hosted repo can own them.

---

## Summary table

Prod lines are approximate (excluding in-file test modules).

| # | Integration | Prod lines | Core-path branch points | Needed by | Tentative | Reason |
|---|---|---|---|---|---|---|
| 1 | Provider registry (GitHub/GitLab/Gitea/Generic) | ~650 + CLI ~150 | Low: `auth_header` shape; GitHub-only size preflight, `/v1/build`, owner/repo response fields | OSS | keep | Thin, data-only, generic git transport |
| 2a | Shared server token (Ripclone/Basic) | ~100 | Middleware; Basic exists only for smart-HTTP | OSS, hosted? | keep (drop Basic if smart-HTTP cut) | Core auth |
| 2b | Session JWT + `/login` page + `auth login` | ~165 + ~270 server + ~150 CLI | Middleware (3rd token encoding) | OSS nicety | simplify/cut | Second credential system for one shared secret |
| 2c | Per-repo AccessVerifier + `TRUST_GATEWAY` | ~190 + ~30 | 7 call sites: git info/refs + upload-pack (:2643, :2740), ref (:3351), status (:3896), sync (:4060), sync --at (:4223), add (:4352). Not on by-hash `/v1/artifacts` | OSS multi-user | keep verifier, cut trust mode | Security fix; one authz path |
| 2d | GitHub App broker | ~230 | Credential selection for github only | OSS orgs / hosted? | keep | Isolated behind broker trait; document env vars |
| 2e | GitHub OIDC `/v1/build` | ~320 (oidc 160 + handler 160) | Separate admission entry, hard-coded github | nobody documented | cut | Undocumented, GitHub-only, duplicates `/sync` |
| 2f | Job token | ~140 + ~50 | Worker endpoints only | API worker | tie to 5 | |
| 3a | SQLite control DB | ~2,300 (control 918, meta ~400, queue sql+libsql ~1,400) | Core | everyone | keep, simplify layers | Two-layer leftovers, DDL in 3 places, 3 duplicate publish fns |
| 3b | Turso embedded replica | ~35 + libsql git-pinned dep | One branch in `ControlDb::open` | hosted (UNVERIFIED) | cut from OSS or feature-gate | Cheap in code, costly in deps |
| 3c | Removed-config guards | ~80 | Startup only | upgraders | cut later | Temporary |
| 4 | Storage local + S3 (multipart, presign, retention) | ~1,500 (storage ~780, cas 472, retention 249) | 5 small `is_remote`/`signed_url` branches | OSS, hosted | keep; async-only trait | Clean boundary |
| 4b | By-hash artifact proxy | ~375 (incl ~60 test plumbing) | Local storage path + S3 redirect | OSS local | keep | Only path for local storage |
| 5a | Embedded workers | ~165 | Core | everyone | keep | |
| 5b | Standalone API worker + `/v1/jobs`, `/v1/refs` | ~2,200 incl. size classes and registry | Claim-scoped publish, credential shipping | hosted (UNVERIFIED), multi-machine OSS | keep if hosted uses it, else cut | Biggest single optional subsystem |
| 5c | Idle worker registry / live counts | ~150 | None (consumer deleted) | nobody | cut | Dead after dispatcher removal |
| 5d | Size classes | ~190 | Admission classification, claim filter | API worker fleets | simplify (drop `machine`) | Only used with `--max-size-class` |
| 6a | Webhooks GitHub/GitLab/Gitea | ~735 | Shared admission core | OSS | keep | Isolated trait |
| 6b | Poller | ~80 | Shared admission | OSS | keep | Cheap safety net |
| 7a | Smart-HTTP git fallback | ~310 + auth branches | Own clone path + server-side fetch | README fallback only | cut | Second clone path, extra auth encoding |
| 7b | `--temp` overlay | ~190 + client threading | Client install path | niche Linux CI | simplify/cut | Mount privileges, undocumented |
| 7c | LSM history + `RIPCLONE_LSM` toggle | ~120 + pack.rs share | Full build | core perf | keep LSM, cut toggle | Toggle keeps a 2nd path |
| 7d | zstd dictionary + `train-dictionary` | ~100 + plumbing in archive/extract | Archive compress param (always None) | nobody | cut | Half-built, never used by server |
| 7e | `ripclone-proxy`, `writer_bench` bins | 633 | None | benchmarks | move to examples/benchmark | Not product |
| 7f | Test hooks in prod code | ~750 + ~76 call sites | Every build phase | tests | feature-gate | Production code carries test seams |
| 7g | Clone metrics -> hosted | ~80 + ~60 | None (post-clone) | hosted | keep | Small |
| 7h | Docker/Fly, release deps | n/a | None | maintainer | simplify | Stale libgit2/fuse deps |
| 8 | Config surface | 74 active env vars, 18 undocumented, 4 A/B toggles | — | — | simplify | |

## Integrations adding the most branching to the core path

1. **Standalone API worker** (5b): forces the `RefStore`/`WorkerQueue` trait split, `ApiRefStore`/`ApiJobQueue`, claim-scoped publication, per-job credential persistence and shipping, size classes, heartbeat/registry, job token, and 6 HTTP endpoints. ~2,200 prod lines.
2. **Auth multiplicity** (2): protected routes accept 3 token encodings; repo reads branch on enforced authz vs `TRUST_GATEWAY`; credentials come from 4 sources; plus job token, OIDC, webhook HMAC, and login secret. 6 credential schemes total.
3. **Test hooks** (7f): ~750 lines and ~76 inline `test_hook`/`RIPCLONE_TEST_*` call sites across server and client build/install phases.
4. **GitHub-specific edges** (1, 2d, 2e): `/v1/build` OIDC, GitHub size preflight, GitHub App broker, owner/repo response fields — the only places that break the otherwise generic provider model.
5. **Smart-HTTP fallback** (7a): a second clone path with its own auth encoding (Basic) and server-side mirror fetch.
6. **A/B toggles** (`RIPCLONE_LSM`, `RIPCLONE_ARCHIVE_BOUNDED`, `RIPCLONE_TRUST_GATEWAY`): each keeps two implementations alive in the build or authz path.

Not a big source of branching despite the suspicion: **Turso** (one branch at open) and **storage backends** (five small `is_remote`/`signed_url` checks). Turso's cost is the dependency, not branching.

## Findings worth acting on regardless of keep/cut

- Failed-job pruning is dead: `SqlJobQueue::prune_failed` has no production caller; `ApiJobQueue::prune_failed` is a no-op (`api_job_queue.rs:339-343`). `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` (documented in CONFIG.md) does nothing.
- `ripclone-worker.rs:14-18` docs say `RIPCLONE_QUEUE_STALE_SECS` / `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` configure the worker; the API worker reads neither.
- Module docs `meta/libsql.rs:1`, `queue/libsql_db.rs:1` say "Turso embedded-replica handle" for the default plain SQLite handle.
- Build images install `libgit2`/`fuse` (`tests/fly/Dockerfile`, `release.yml` before-script) although `Cargo.toml` has no git2/fuser dependency.
- GitHub App broker env vars are user-undocumented (only `docs/internal/`).

## Not verified

- Which of these the hosted service (ripclone-cloud) uses: Turso, API worker, GitHub App broker, AccessVerifier, size-class `machine`. Hints in code: `job_token.rs:3` "Cloud/farm-out workers", `ripclone-worker.rs:118` "cloud reconcile cron", `client.rs:2140` "the cloud no longer serves content by hash".
- Whether maturin `bindings = "bin"` packages `ripclone-proxy` and `writer_bench` into the PyPI wheel.
- Compile-time / binary-size cost of libsql vs rusqlite.
- Size of LSM-specific code inside `pack.rs`.
- `spikes/` contents were not reviewed.
