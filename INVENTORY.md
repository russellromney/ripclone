Generated 2026-07-04 — work queue for the G2 gate; DELETE after wave-4 closeout. Not documentation.

# Feature inventory — turbogit + ripclone-cloud

This is the E6 read-only scan of every user-visible surface across both repos,
prepared for the G2 gate review.  Columns are intentionally judgment-free;
keep/flag/cut decisions are left to Fable + Russell.

| Surface | Repo | Documented? | e2e-tested? | Works? (best evidence) |
|---|---|---|---|---|

## Pre-flagged decisions

| `ripclone worktree` add | turbogit | README.md | no | **Flagged**: writes in place with no temp-dir staging and no chunk-fetch retry; an interrupt leaves an unrecoverable half-repo.  See `client.rs` worktree path. |
| Empty-repo clone | turbogit | docs/ROADMAP.md §11 notes unsupported | no | **Flagged**: returns a bare 404 at resolve.  If kept unsupported, the error must say "repository has no commits" instead of a generic 404. |

## turbogit — CLI / binaries

| `ripclone` global flags (`--server`, `--provider`, `--token`) | turbogit | README.md, docs/BACKENDS.md, docs/AUTH.md | yes (`e2e_login_logout.rs`, `e2e_auth.rs`, `e2e_provider_cli.rs`, `e2e_config_*.rs`) | Works for self-host; `--server` defaults to managed cloud. |
| `ripclone login` (device flow) | turbogit | README.md | yes (`e2e_login_logout.rs`) | **Cloud-only**: calls `/cli/device` + `/cli/device/token`, which are not implemented in the OSS server. |
| `ripclone logout` | turbogit | README.md | yes (`e2e_login_logout.rs`) | Removes saved server token. |
| `ripclone version` | turbogit | README.md | yes (`e2e_version.rs`) | Calls `/v1/version`. |
| `ripclone update` | turbogit | README.md | no | Checks latest GitHub release. |
| `ripclone auth login` | turbogit | docs/AUTH.md | yes (`e2e_auth.rs`) | Self-hosted session-token login via `/login` + `/v1/auth/login`. |
| `ripclone auth logout` | turbogit | docs/AUTH.md | yes (`e2e_auth.rs`) | Removes saved JWT. |
| `ripclone auth status` | turbogit | docs/AUTH.md | yes (`e2e_auth.rs`) | Shows saved JWT expiry. |
| `ripclone sync <repo>` (`--depth`, `--at`) | turbogit | README.md | yes (`e2e_roundtrip.rs`, `e2e_sync_at_rev.rs`, `e2e_two_phase.rs`) | Posts to `/v1/repos/{provider}/{repo}/sync`. |
| `ripclone clone <repo>` (`--dir`, `--branch`, `--mode`, `--depth`, `--at`, `--temp`, `--bench`) | turbogit | README.md | yes (`e2e_roundtrip.rs`, `e2e_config_clone_mode.rs`, `e2e_equivalence.rs`) | Default mode `editable`, default depth 1. |
| `ripclone sidecar` | turbogit | README quick start (implicit) | no | Finishes snapshot clone in background. |
| `ripclone cat <repo> <path>` | turbogit | not in README | no | Reads file from skeleton clone via `/cat`. |
| `ripclone provider add/list/rm/test` | turbogit | README.md, docs/BACKENDS.md | yes (`e2e_provider_cli.rs`) | Writes/reads provider config and tests connectivity. |
| `ripclone backend show/queue/metadata/storage` | turbogit | docs/BACKENDS.md | yes (`e2e_config_*.rs`) | Configures backend values in config.toml. |
| `ripclone snapshot create/extract` | turbogit | not in README | no | Builds/downloads snapshot tarball. |
| `ripclone prefetch <repo>` | turbogit | not in README | no | Fetches hot files into skeleton. |
| `ripclone build-archive` / `extract-archive` | turbogit | not in README | yes (`archive_bounded.rs`) | Local archive builder/extractor. |
| `ripclone worktree <path>` | turbogit | README.md | no | Adds worktree via overlay staging. **Pre-flagged** (see above). |
| `ripclone train-dictionary` | turbogit | not in README | no | Trains zstd dict from repo blobs. |
| `ripclone track/untrack/tracked` | turbogit | docs/WEBHOOKS.md:185-187 | no | **Not implemented** in `cli.rs` or server router; doc is ahead of code. |
| `ripclone-server` (`--cas-dir`, `--repo-root`, `--host`, `--port`) | turbogit | README.md, docs/BACKENDS.md | yes (many e2e tests start server) | Defaults changed to `~/.local/share/ripclone/` by F1. |
| `ripclone-worker` (`--cas-dir`, `--repo-root`, `--idle-poll-ms`) | turbogit | docs/BACKENDS.md, ripclone-worker.rs doc comments | yes (`e2e_worker_*.rs`, `e2e_farmout_concurrency.rs`) | Claims jobs from SQL queue. |
| `git-remote-ripclone` | turbogit | README.md | yes (`e2e_remote_helper.rs`) | Supports `capabilities`, `list`, `option`, `connect git-upload-pack`; push rejected. |
| `ripclone-proxy` (`listen`, `upstream`, `latency`, `bandwidth_mbps`, `--forward-auth`) | turbogit | not in README | no (`benchmark/latency_proxy.py`) | Latency/bandwidth shaping proxy for tests. |
| `writer_bench` | turbogit | docs/WRITER_SCHEDULER_EXPERIMENT.md | no | Internal writer benchmark binary. |

## turbogit — environment variables

| `RIPCLONE_CONFIG` | turbogit | docs/BACKENDS.md | yes (`e2e_config_global_and_overrides.rs`) | Explicit global config path; default `~/.config/ripclone/config.toml`. |
| `RIPCLONE_SERVER` | turbogit | README.md | yes (many) | Server URL precedence for CLI / git-remote. |
| `RIPCLONE_PROVIDER` | turbogit | README.md | yes (`e2e_multi_provider.rs`) | Default provider instance. |
| `RIPCLONE_UPSTREAM_TOKEN` | turbogit | README.md | yes (`e2e_auth.rs`) | Sent as `X-Upstream-Token`. |
| `RIPCLONE_MODE` | turbogit | README.md | yes (`e2e_config_clone_mode.rs`) | Default clone mode (`editable`). |
| `RIPCLONE_BENCH` | turbogit | README.md | yes (many) | Enables `--bench` output. |
| `RIPCLONE_NO_BROWSER` | turbogit | not documented | tests | Skips browser open in login. |
| `RIPCLONE_TEMP` | turbogit | README.md | no | Ephemeral tmpfs clone. |
| `RIPCLONE_SERVER_TOKEN` / `RIPCLONE_SERVER_TOKEN_HASH` | turbogit | README.md, docs/BACKENDS.md | yes (`e2e_auth.rs`) | Raw or pre-hashed server token; required for server startup and client auth. |
| `RIPCLONE_TOKEN` / `RIPCLONE_TOKEN_HASH` (deprecated) | turbogit | docs/BACKENDS.md | yes (`e2e_config_legacy_token_migration.rs`) | Deprecated aliases still accepted. |
| `RIPCLONE_JWT_SECRET` / `RIPCLONE_JWT_TTL_SECS` / `RIPCLONE_JWT_SESSION_MAX_SECS` | turbogit | docs/AUTH.md | yes (`e2e_auth.rs`) | Session-token signing and lifetime. |
| `RIPCLONE_TRUST_GATEWAY` | turbogit | docs/BACKENDS.md | yes (`e2e_auth.rs`) | Skips per-repo access check (single-tenant self-host). |
| `RIPCLONE_REPO_AUTH_TTL_SECS` | turbogit | docs/BACKENDS.md | no | Cache TTL for upstream per-repo auth. |
| `RIPCLONE_OIDC_AUDIENCE` | turbogit | not in README | yes (`e2e_billing.rs`) | Enables OIDC verification for `/v1/build`. |
| `RIPCLONE_S3_ENDPOINT` / `REGION` / `BUCKET` / `PREFIX` / `CACHE_DIR` | turbogit | README.md, docs/BACKENDS.md | yes (`e2e_remote_gc_s3.rs`) | S3-compatible storage config. |
| `RIPCLONE_S3_REQUEST_TIMEOUT_SECS` / `RIPCLONE_STORAGE_REGIONS` | turbogit | not documented | no | S3 request timeout and multi-region storage. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | turbogit | docs/BACKENDS.md | yes (`e2e_remote_gc_s3.rs`) | S3 credentials. |
| `RIPCLONE_METADATA` / `RIPCLONE_METADATA_DB_URL` / `RIPCLONE_METADATA_DB_TOKEN` | turbogit | docs/BACKENDS.md | yes (`e2e_metadata_*.rs`, `e2e_worker_libsql.rs`) | Metadata/ref-store backend selector. |
| `RIPCLONE_QUEUE` / `RIPCLONE_QUEUE_DB_URL` / `RIPCLONE_QUEUE_DB_TOKEN` | turbogit | docs/BACKENDS.md | yes (`e2e_sql_queue.rs`, `e2e_worker_*.rs`) | Build-queue backend selector. |
| `RIPCLONE_QUEUE_STALE_SECS` / `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` | turbogit | docs/BACKENDS.md | yes (`e2e_worker_*.rs`) / no | Crashed-worker reclaim and failed-job pruning. |
| `RIPCLONE_FETCH_CONCURRENCY` / `RIPCLONE_ARCHIVE_FETCH_CONCURRENCY` / `RIPCLONE_EDITABLE_DOWNLOAD_CONCURRENCY` | turbogit | README.md | yes (many) / no / no | Concurrent chunk downloads. |
| `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` | turbogit | README.md | no | Archive fetcher / worktree writer threads. |
| `RIPCLONE_PACK_PARSE_THREADS` | turbogit | not documented | no | Pack parse threads. |
| `RIPCLONE_FETCH_MAX_ATTEMPTS` / `RIPCLONE_FETCH_BACKOFF_MS` | turbogit | README.md | no | Download retry budget and backoff. |
| `RIPCLONE_CLONE_MAX_ATTEMPTS` / `RIPCLONE_SYNC_MAX_ATTEMPTS` / `RIPCLONE_ARCHIVE_CHANNEL_DEPTH` | turbogit | not documented | no | Polling retry / channel depths. |
| `RIPCLONE_UPLOAD_CONCURRENCY` / `RIPCLONE_BUILD_CONCURRENCY` | turbogit | not documented | no | Server upload and build concurrency. |
| `RIPCLONE_SYNC_WAIT_SECS` | turbogit | not documented | yes (`e2e_roundtrip.rs`) | `/sync` wait timeout. |
| `RIPCLONE_POLL_INTERVAL_SECS` | turbogit | README.md | yes (`e2e_freshness.rs`) | Polling fallback interval. |
| `RIPCLONE_MIRROR_FRESH_TTL_SECS` | turbogit | docs/BACKENDS.md | yes (`e2e_freshness.rs`) | Mirror freshness TTL. |
| `RIPCLONE_IO_URING` / `RIPCLONE_IO_URING_DEPTH` / `RIPCLONE_IO_URING_SQPOLL` | turbogit | README.md / not documented / not documented | no | Linux io_uring writer controls. |
| `RIPCLONE_IO_URING_SCHEDULER` and related knobs | turbogit | docs/WRITER_SCHEDULER_EXPERIMENT.md | no | Deprecated scheduler flags. |
| `RIPCLONE_PACK_BYTES` / `RIPCLONE_HEAD_REBASE_BYTES` / `RIPCLONE_HISTORY_PACK_BYTES` / `RIPCLONE_HISTORY_MAX_PACK_BYTES` | turbogit | not documented | no | Pack sizing thresholds. |
| `RIPCLONE_ARCHIVE_BUNDLE_BYTES` / `RIPCLONE_ARCHIVE_BOUNDED` / `RIPCLONE_ARCHIVE_BOUNDED_MAX_BYTES` | turbogit | not documented | yes (`archive_bounded.rs`) | Archive build controls. |
| `RIPCLONE_LSM` / `RIPCLONE_LSM_MAX_LEVELS` | turbogit | not documented | yes (`e2e_lsm.rs`) | LSM-style history packs. |
| `RIPCLONE_MAX_THREADS` / `RIPCLONE_HASH_THREADS` / `RIPCLONE_PACK_ENCODE_THREADS` / `RIPCLONE_GIX_INDEX_THREADS` / `RIPCLONE_LOOKUP_THREADS` | turbogit | not documented | no | gix thread ceilings. |
| `RIPCLONE_GIX_PACK` | turbogit | not documented | no | Use gix pack encoding. |
| `RIPCLONE_BLOB_PACK_THREADS` / `RIPCLONE_BLOB_PACK_CHANNEL_DEPTH` / `RIPCLONE_BLOB_PACK_COMPRESSION_LEVEL` | turbogit | not documented | no | Blob-pack parallelism. |
| `RIPCLONE_CACHE_DIR` / `RIPCLONE_NO_CACHE` | turbogit | README.md, docs/BACKENDS.md | yes (many) | Client artifact cache. |
| `RIPCLONE_NO_OVERLAY` / `RIPCLONE_STAGING_DIR` / `RIPCLONE_OVERLAY_MARGIN_MB` / `RIPCLONE_OVERLAY_THRESHOLD_MB` | turbogit | docs/BACKENDS.md | no | Overlay staging controls. |
| `RIPCLONE_FSYNC` / `RIPCLONE_EXTRACT_ARCHIVE` | turbogit | not documented | no | Durability barrier and forced archive extraction. |
| `RIPCLONE_WEBHOOK_SECRET_<provider>` / `RIPCLONE_WEBHOOK_SECRET` | turbogit | README.md, docs/WEBHOOKS.md | yes (`e2e_webhook.rs`) | Webhook HMAC secrets. |
| `RIPCLONE_WEBHOOK_ALLOWLIST` / `RIPCLONE_WEBHOOK_WARM_ALL` | turbogit | README.md, docs/WEBHOOKS.md | yes (`e2e_webhook.rs`) | Webhook filtering / branch warming. |
| `RIPCLONE_RETENTION_INTERVAL_SECS` / `RIPCLONE_RETENTION_MAX_AGE_DAYS` / `RIPCLONE_RETENTION_MAX_GB` | turbogit | docs/BACKENDS.md | yes (`e2e_remote_gc_s3.rs`) | Local retention sweep. |
| `RIPCLONE_REMOTE_GC_INTERVAL_SECS` / `RIPCLONE_REMOTE_GC_GRACE_SECS` / `RIPCLONE_REMOTE_GC_DRY_RUN` | turbogit | docs/BACKENDS.md, docs/GC.md | yes (`e2e_remote_gc_s3.rs`) | Remote-object GC. |
| `RIPCLONE_NO_METRICS` | turbogit | not documented | no | Suppress post-clone metrics. |
| `RIPCLONE_RATE_LIMIT_PER_SEC` / `RIPCLONE_RATE_LIMIT_BURST` | turbogit | not documented | no | Public-endpoint rate limits. |
| `RIPCLONE_SIGNED_URL_TTL_SECS` / `RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS` | turbogit | not documented | no | Signed URL TTLs. |
| `RIPCLONE_TRUST_FORWARDED_FOR` | turbogit | not documented | no | Trust `X-Forwarded-For`. |
| `RIPCLONE_ORIGIN_BASE` | turbogit | not documented | unit tests | Test override for upstream base URL. |
| `RIPCLONE_TEST_FAIL_FIRST_FETCHES` / `RIPCLONE_TEST_ARCHIVE_DELAY_MS` / `RIPCLONE_TEST_PG_URL` / `RIPCLONE_TEST_MYSQL_URL` | turbogit | not documented | yes (`e2e_freshness.rs`, metadata tests) | Test-only hooks. |
| `RIPCLONE_PROVIDERS` / `RIPCLONE_PROVIDERS_CONFIG` | turbogit | README.md, docs/BACKENDS.md | yes (`e2e_multi_provider.rs`, `e2e_provider_cli.rs`) | JSON provider config. |
| `RIPCLONE_PROVIDER_<ID>_TOKEN` / `RIPCLONE_GITHUB_TOKEN` | turbogit | README.md (implicit), docs/BACKENDS.md | yes (`e2e_provider_cli.rs`, many) | Per-provider static token / legacy GitHub PAT. |
| GitHub App env vars (`RIPCLONE_GITHUB_APP_ID`, `INSTALLATION_ID`, `PRIVATE_KEY`, `PRIVATE_KEY_PATH`, `API_BASE`) | turbogit | docs/GITHUB_INTEGRATION.md | no (marked `#[ignore]`) | GitHub App token minting. |

## turbogit — HTTP API endpoints

| `GET /healthz` | turbogit | README.md | no | Liveness. |
| `GET /v1/version` | turbogit | README.md, CHANGELOG.md | yes (`e2e_version.rs`) | Version + protocol. |
| `GET /readyz` | turbogit | README.md | yes (`e2e_version.rs`) | 503 when deps unhealthy. |
| `GET /metrics` | turbogit | README.md | no | Prometheus text. |
| `GET /login` / `POST /v1/auth/login` / `POST /v1/auth/refresh` | turbogit | docs/AUTH.md | yes (`e2e_auth.rs`) | Session-token login/refresh. |
| `POST /v1/build` | turbogit | README.md (Actions trigger) | yes (`e2e_billing.rs`) | Fire-and-forget build (OIDC + token). |
| `POST /webhooks/{provider}` / `POST /v1/webhooks/github` | turbogit | README.md, docs/WEBHOOKS.md | yes (`e2e_webhook.rs`) | Provider webhook receivers. |
| `GET /v1/repos/{provider}/{repo}/refs/{branch}` | turbogit | README.md (implicit) | yes (many) | Resolve ref + clonepack info. |
| `GET /v1/repos/{provider}/{repo}/status` | turbogit | not documented | no | Repo build status. |
| `GET /v1/repos/{provider}/{repo}/cat` | turbogit | not documented | no | Read single file. |
| `GET /v1/repos/{provider}/{repo}/sizes` | turbogit | not documented | no | File sizes. |
| `GET /v1/repos/{provider}/{repo}/hotfiles` | turbogit | not documented | no | Hot file list. |
| `POST /v1/repos/{provider}/{repo}/sync` | turbogit | README.md | yes (many) | Trigger sync build. |
| `POST /v1/repos/{provider}/{repo}/snapshot` | turbogit | not documented | no | Create snapshot. |
| `POST /v1/repos/{provider}/{repo}/batch` | turbogit | not documented | no | Batch file download. |
| `GET/POST /v1/admin/config/{owner}/{repo}` | turbogit | docs/CHANGELOG.md | yes (`e2e_repo_config.rs`) | Per-repo config. |
| `GET /v1/git/{provider}/{repo}/info/refs` / `POST …/git-upload-pack` | turbogit | README.md | yes (`e2e_remote_helper.rs`) | Smart-HTTP fallback. |
| `GET /v1/packs/{hash}` / `/v1/objects/{sha}` / `/v1/artifacts/{hash}` / `/v1/archives/{hash}` / `/v1/manifests/{hash}` | turbogit | not documented | yes (many use artifact paths) | Artifact/object download endpoints. |
| `POST /cli/device`, `POST /cli/device/token` | turbogit | implied by `ripclone login` | no | **Managed-cloud only**; not in OSS server.rs. |

## turbogit — webhook providers & install channels

| GitHub webhook (`/webhooks/github`, `/v1/webhooks/github`) | turbogit | README.md, docs/WEBHOOKS.md | yes (`e2e_webhook.rs`) | HMAC-SHA256 verified. |
| GitLab webhook (`/webhooks/gitlab`) | turbogit | docs/WEBHOOKS.md | yes (`e2e_webhook.rs`) | Shared-token verified. |
| Gitea/Forgejo/Codeberg webhook (`/webhooks/gitea`) | turbogit | docs/WEBHOOKS.md | yes (`e2e_webhook.rs`) | HMAC-SHA256 verified. |
| Bitbucket webhook | turbogit | docs/WEBHOOKS.md says "would follow same trait" | no | **Not implemented**: `webhook::provider_for` returns `None`. |
| Shell installer (`curl …/install.sh \| sh`) | turbogit | README.md, CHANGELOG.md | no | Release workflow exists; unverified without published release. |
| Cargo / crates.io (`cargo install ripclone`) | turbogit | README.md, CHANGELOG.md | no | Cargo.toml configured; not yet published. |
| pip / PyPI (`pip install ripclone`) | turbogit | README.md, CHANGELOG.md | no | **Stale doc claim**: `pyproject.toml` and Python package were removed. |

## ripclone-cloud — UI pages

| Landing `/` | ripclone-cloud | README.md, SCREENS.md §1 | no | Implemented. |
| Docs `/docs` | ripclone-cloud | SCREENS.md §1a | no | Implemented. |
| Pricing `/pricing` | ripclone-cloud | PRICING.md, SCREENS.md §1a | no | Implemented. |
| Sponsors `/sponsors` | ripclone-cloud | SPONSORSHIP.md | no | Implemented. |
| CLI device auth `/cli` | ripclone-cloud | SCREENS.md §3, UX.md §6 | yes (`cli-device-route.test.ts`) | Implemented. |
| Repos home `/repos` | ripclone-cloud | SCREENS.md §4/§5 | yes (`journey-onboarding.test.ts`, `journey-sync.test.ts`) | Implemented. |
| Repo detail `/repos/[owner]/[name]` | ripclone-cloud | SCREENS.md (no v1 per-repo page) | yes (`journey-sync.test.ts`) | Implemented. |
| Org page `/orgs/[slug]` | ripclone-cloud | SCREENS.md §5/§6 | yes (`billing-actions.test.ts`, `journey-entitlement.test.ts`) | Implemented. |
| Org settings `/orgs/[slug]/settings` | ripclone-cloud | SCREENS.md §6 | yes (`action-authz.test.ts`, `orgs.test.ts`) | Implemented. |
| Org tokens `/orgs/[slug]/tokens` | ripclone-cloud | SCREENS.md §6 (#4) | yes (`repos-actions.test.ts`) | Implemented. |
| Org usage `/orgs/[slug]/usage` | ripclone-cloud | UX.md §9 | yes (`usage.test.ts`) | Implemented. |
| Public commons `/public` | ripclone-cloud | SCREENS.md §4 | no | Implemented; **contradicts SCREENS.md**: page has a Remove button (`unpinPublicRepoAction`) despite spec saying "NO remove." |
| Personal settings `/settings` | ripclone-cloud | SCREENS.md §6 (avatar menu) | no | Implemented; sign-out only. |
| Invites `/invites` | ripclone-cloud | SCREENS.md §6/§7 | no | Implemented. |
| Token redirect `/tokens` / usage redirect `/usage` | ripclone-cloud | UX.md §5 / §9 | no | Implemented redirects. |
| `/dashboard`, `/dashboard/add` redirects | ripclone-cloud | UX.md §C (replaced) | no | Redirect to `/repos`. |
| `/start` funnel page | ripclone-cloud | SCREENS.md §2 | no | **Not implemented**. |
| `/security`, `/terms`, `/privacy` | ripclone-cloud | SCREENS.md §1a | no | **Not implemented**. |

## ripclone-cloud — server actions

| `addRepoAction` | ripclone-cloud | SCREENS.md §5 | yes (`repos-actions.test.ts`) | Private repo add via installation; membership-gated. |
| `addRepoByPathAction` | ripclone-cloud | SCREENS.md §4/§5 | yes (`repos-actions.test.ts`) | Public→commons; private→requires owning install. |
| `addPublicRepoAction` / `pinPublicRepoAction` / `unpinPublicRepoAction` | ripclone-cloud | SCREENS.md §4 | yes/no/no | Public pin/unpin; `unpinPublicRepoAction` contradicts "NO remove." |
| `warmNowAction` / `repoSyncStatusAction` | ripclone-cloud | SCREENS.md §5, UX.md §8 | yes (`journey-sync.test.ts`) / no | Sync enqueue and status polling. |
| `addBranchAction` / `removeBranchAction` | ripclone-cloud | SCREENS.md §5 | no | Branch management, membership-gated. |
| `createTeamOrgAction` / `setCurrentOrgAction` | ripclone-cloud | SCREENS.md §6/§7, §7 switcher | yes (`orgs.test.ts`) / no | Org creation and active-org cookie. |
| `inviteMemberAction` / `revokeInviteAction` / `removeMemberAction` | ripclone-cloud | SCREENS.md §6 (#5), §6 | yes (`orgs.test.ts`) | Member invite and removal. |
| `acceptInviteAction` / `declineInviteAction` | ripclone-cloud | SCREENS.md §7 | yes (`orgs.test.ts`) | Invite acceptance. |
| `startCheckoutAction` / `openPortalAction` | ripclone-cloud | PRICING.md, SCREENS.md §6/§7 | yes (`billing-actions.test.ts`, `stripe.sandbox.ts`) | Stripe Checkout + Portal. |
| `setSponsorshipAction` / `cancelSponsorshipAction` | ripclone-cloud | SPONSORSHIP.md | yes (`stripe.sandbox.ts`) | Sponsorship line item. |
| `createTokenAction` / `revokeTokenAction` | ripclone-cloud | SCREENS.md §6 (#4), UX.md §5 | yes (`tokens.test.ts`) | Personal + agent tokens. |
| `approveDeviceAction` / `denyDeviceAction` | ripclone-cloud | UX.md §6 | yes (`cli-device-route.test.ts`) | Device-flow approval. |

## ripclone-cloud — API / webhook / gateway routes

| NextAuth catch-all `/api/auth/[...nextauth]` | ripclone-cloud | auth.ts, IDENTITY_DESIGN.md | yes (`auth-account.test.ts`, `users.test.ts`) | GitHub App user-to-server + Resend magic-link. |
| GitHub App webhook `POST /api/github/webhook` | ripclone-cloud | ROADMAP.md, BACKEND_INTEGRATION.md, SYNC_LATENCY_DESIGN.md | yes (`github-webhook-route.test.ts`) | push, installation, installation_repositories, membership. |
| Stripe webhook `POST /api/stripe/webhook` | ripclone-cloud | PRICING.md, SPONSORSHIP.md | yes (`stripe-webhook-route.test.ts`, `stripe.sandbox.ts`) | checkout.session.completed, subscription changes. |
| Gateway catch-all `/v1/[...path]` | ripclone-cloud | BACKEND_INTEGRATION.md, ROADMAP.md | yes (`gateway-route.test.ts`, `access-per-repo.test.ts`) | Validates token, delegates GitHub access, meters, forwards to backend. |
| Clone metrics `POST /v1/clones/[cloneId]/metrics` | ripclone-cloud | METRICS.md, UX.md §10 | yes (`metrics-route.test.ts`) | Client-reported metrics ingestion. |
| Device flow start `POST /cli/device` | ripclone-cloud | UX.md §6 | yes (`cli-device-route.test.ts`) | Returns device_code/user_code/verification URI. |
| Device flow poll `POST /cli/device/token` | ripclone-cloud | UX.md §6 | yes (`cli-device-route.test.ts`) | Polls for approved token. |
| GitHub App setup callback `/connect/callback` | ripclone-cloud | ROADMAP.md, BACKEND_INTEGRATION.md | yes (`connect-callback.test.ts`) | Claim-check + org attachment. |
| Health `/healthz` / readiness `/readyz` | ripclone-cloud | fly.toml | yes (`healthz/route.test.ts`) / no | Liveness and DB+backend readiness. |

## ripclone-cloud — gateway behaviors

| Token resolution (`rc_live_…` / sha256) | ripclone-cloud | ROADMAP.md §M3 | yes (`tokens.test.ts`, `gateway-route.test.ts`) | Personal + agent tokens; updates `lastUsedAt`. |
| Per-IP rate limiting | ripclone-cloud | not documented | yes (`gateway.test.ts`) | Token bucket, in-memory, resets on deploy. |
| Path classification (control/content/passthrough) | ripclone-cloud | BACKEND_INTEGRATION.md | yes (`gateway.test.ts`) | `/v1/repos/...`, `/v1/git/...`, `/v1/version`, content prefixes. |
| Provider restriction (`github` only) | ripclone-cloud | BACKEND_INTEGRATION.md | yes (`gateway-route.test.ts`) | Returns `400 unsupported_provider`. |
| Account-required gate | ripclone-cloud | PRICING.md, ROADMAP.md | yes (`gateway-route.test.ts`) | `401 need_account` for anonymous control-plane. |
| GitHub read-access delegation + 60s cache | ripclone-cloud | ROADMAP.md, PRICING.md | yes (`access-per-repo.test.ts`) | Per-repo, not org membership. |
| Billing entitlement for private repos | ripclone-cloud | PRICING.md | yes (`journey-entitlement.test.ts`, `stripe-webhook-route.test.ts`) | Off when `STRIPE_SECRET_KEY` unset. |
| Internal backend auth (`Ripclone <sha256>`) | ripclone-cloud | BACKEND_INTEGRATION.md | yes (`backend.test.ts`) | `RIPCLONE_INTERNAL_TOKEN` hashed on wire. |
| `X-Upstream-Token` for private builds | ripclone-cloud | BACKEND_INTEGRATION.md | yes (`journey-sync.test.ts`) | Mints GitHub App installation token. |
| `X-Ripclone-Visibility` header | ripclone-cloud | BACKEND_INTEGRATION.md | no | Public/private TTL hint to backend. |
| Cold-clone on-demand trigger (`202 building`) | ripclone-cloud | SYNC_LATENCY_DESIGN.md §4 | yes (`journey-sync.test.ts`) | Triggers `/sync` for tracked repos or installed owners. |
| Usage event metering (`usage_events`) | ripclone-cloud | PRICING.md, METRICS.md | yes (`gateway-route.test.ts`) | One event per successful ref resolve. |
| Clone ID issuance + metrics join | ripclone-cloud | METRICS.md | yes (`metrics-route.test.ts`) | Echoed as `X-Ripclone-Clone-Id`. |
| Content-plane block (`no_content_plane`) | ripclone-cloud | CONTENT_AUTH_DESIGN.md | yes (`gateway-route.test.ts`) | Returns `404`; bytes are presigned-URL only. |
| Path-traversal guard | ripclone-cloud | ROADMAP.md §known traps | yes (`gateway-route.test.ts`) | Rejects `.`, `..`, encoded separators. |
| 307 passthrough (no follow) | ripclone-cloud | ROADMAP.md §known traps | yes (`gateway-route.test.ts`) | Bytes skip the cloud. |
| Signed content-plane capabilities (`CAPABILITY_SIGNING_KEY`) | ripclone-cloud | CONTENT_AUTH_DESIGN.md, `.env.example` | no | **Design-only**: gateway returns `404 no_content_plane`; key unused. |
| Multi-provider abstraction (`GitProvider` interface) | ripclone-cloud | ROADMAP.md, IDENTITY_DESIGN.md | no | **Design-only**: only GitHub implementation exists. |

## ripclone-cloud — emails / notifications

| Magic-link sign-in emails | ripclone-cloud | auth.ts comments, UX.md §6 | no | Requires `AUTH_RESEND_KEY` + `EMAIL_FROM`. Only transactional email path. |
| Billing/payment-failed emails | ripclone-cloud | SCREENS.md §6, ROADMAP.md mention Resend | n/a | **Not implemented**. |
| Account-deletion confirmation email | ripclone-cloud | SCREENS.md §F7 | n/a | **Not implemented**. |
| Org invite emails | ripclone-cloud | SCREENS.md §6 (#5) | n/a | **Not implemented**; invites are in-app only. |

## ripclone-cloud — operator env vars / config knobs

| `TURSO_DATABASE_URL` / `TURSO_AUTH_TOKEN` | ripclone-cloud | `src/db/index.ts`, `drizzle.config.ts` | yes (tests use test DB) | libSQL/Turso DB URL and token. |
| `RIPCLONE_BACKEND_URL` | ripclone-cloud | `src/app/v1/[...path]/route.ts`, `src/lib/backend.ts`, `src/app/readyz/route.ts` | yes | OSS backend control-plane URL. |
| `RIPCLONE_INTERNAL_TOKEN` | ripclone-cloud | `src/app/v1/[...path]/route.ts`, `src/lib/backend.ts` | yes | Cloud↔backend shared secret (hashed on wire). |
| `RIPCLONE_PUBLIC_URL` | ripclone-cloud | `src/lib/gatewayUrl.ts`, `src/lib/urls.ts` | yes | Public URL for CLI `--server` and device-flow links. |
| `AUTH_SECRET` / `AUTH_URL` | ripclone-cloud | NextAuth, `src/app/connect/callback/route.ts` | yes | JWT/session secret and public origin. |
| `GITHUB_APP_ID` / `GITHUB_APP_SLUG` / `GITHUB_APP_CLIENT_ID` / `CLIENT_SECRET` / `GITHUB_APP_PRIVATE_KEY` / `GITHUB_APP_WEBHOOK_SECRET` | ripclone-cloud | `src/lib/github.ts`, webhook route | yes | GitHub App credentials. |
| `STRIPE_SECRET_KEY` / `STRIPE_WEBHOOK_SECRET` / `STRIPE_PRICE_ID` / `STRIPE_SPONSOR_PRICE_ID` | ripclone-cloud | `src/lib/stripe.ts`, `src/lib/sponsorship.ts` | yes | Stripe billing + sponsorship. |
| `AUTH_RESEND_KEY` / `EMAIL_FROM` | ripclone-cloud | `src/auth.ts` | no | Resend magic-link provider. |
| `CAPABILITY_SIGNING_KEY` | ripclone-cloud | `.env.example`, CONTENT_AUTH_DESIGN.md | no | **Unused** — future content-plane capability signing. |
| `NODE_ENV`, `PORT` | ripclone-cloud | fly.toml | yes | Standard Next.js/Fly runtime. |

## Cross-repo doc/implementation gaps

| pip install channel | turbogit | README.md | no | Stale: Python package and `pyproject.toml` were removed. |
| `ripclone login` against OSS | turbogit | README.md | no | Cloud-only endpoints absent in OSS server. |
| `ripclone track/untrack/tracked` commands and API | turbogit | docs/WEBHOOKS.md | no | Not implemented. |
| `/start`, `/security`, `/terms`, `/privacy` pages | ripclone-cloud | SCREENS.md | no | Not implemented. |
| Public repo removal from `/public` | ripclone-cloud | SCREENS.md §4 | yes (in action) | Contradicts spec: Remove button exists. |
| Billing/payment-failed, account-deletion, org-invite emails | ripclone-cloud | SCREENS.md / ROADMAP.md | no | Not implemented. |
| Signed content-plane capabilities | ripclone-cloud | CONTENT_AUTH_DESIGN.md | no | Design-only. |
| Multi-provider abstraction beyond GitHub | ripclone-cloud | ROADMAP.md / IDENTITY_DESIGN.md | no | Design-only. |
