# ripclone launch plan — prompt DAG

> Working doc for launch. Each node is a prompt you paste into an executor session
> (Kimi / Codex / Fable), with dependencies, acceptance criteria, and a review step.
> Nodes marked **[USER]** are decisions or actions only Russell can do.
> Spans both repos: `turbogit` (OSS) and `../ripclone-cloud`.

## How to use

**Executors**
- **Kimi** — large, well-specified execution: deletions, refactors with tests as the
  net, doc rewrites, frontend screens.
- **Codex** — small subtle correctness diffs, and ALL diff reviews (`codex review`,
  `codex challenge` for adversarial passes).
- **Fable** — design specs, cross-repo decisions, gate reviews, anything ambiguous.
- **User** — decisions, secrets, Stripe/GitHub App cutover, merges to main.

**Standard review loop (every code node unless noted)**
1. Executor runs `scripts/ci.sh lint test` in `turbogit/rust` (or `pnpm test` in
   ripclone-cloud) before declaring done. Touched e2e files → also `scripts/ci.sh flake`.
2. `codex review` on the diff.
3. Fable final review before merge (batch several nodes per Fable session to save tokens).
4. Track-A (correctness) nodes additionally get `codex challenge` (adversarial).

**Preamble — paste at the top of every executor prompt**
```
Repo: ~/Documents/Github/turbogit (OSS "ripclone", Rust in rust/) and/or
~/Documents/Github/ripclone-cloud (Next.js + drizzle SaaS layer).
ripclone pre-builds git clone artifacts ("clonepacks") on push; clients download
them in parallel from object storage. Read docs/DESIGN.md for context if needed.
Rules: no mocks in tests (real servers/git/binaries — see rust/tests/common/mod.rs);
match existing code style and comment density; no finding-IDs or plan references in
code comments; run fmt + clippy + the tests listed in the node before finishing.
Multiple worktrees are active on this repo — work only in your assigned worktree/branch.
Do exactly the node's scope; if you find adjacent problems, list them in your summary
instead of fixing them.
```

---

## Phase 0 — Decisions [USER]

All decided 2026-07-03.

- **D1 Storage keys.** One scheme for every provider, github included:
  `{provider}/{escaped_path}`. Wipe dev data. Unblocks C1/C2.
- **D2 Access.** Every clone needs a free account. Free signup is GitHub sign-in only —
  that is the sybil defense. Public repos: unlimited clones, no meter. Keep only a high
  abuse-threshold rate limit, plus per-account usage stats with an alert if any account
  reaches fleet scale — then we reprice with data. No anonymous path. The landing page
  must take a new user from signup to first clone in under 60 seconds (H1).
- **D3 Support matrix.** Two layers, don't conflate them.
  *Self-host (OSS):* GitHub, GitLab, and Gitea all supported at launch — the adapters
  exist; E3's e2e tests are what make the claim true, so E3 gates launch.
  *Cloud (ripclone.com):* **GitHub only at launch** — forced by D2 (GitHub-only
  sign-in). The identity/connection schema (H0) is provider-agnostic so GitLab/Gitea
  slot in later without a rewrite, but no GitLab/Gitea login, connection, or
  access-check ships at launch. Bitbucket cut everywhere (C4). SQLite/libsql +
  S3-compatible are the blessed backends; Postgres/MySQL best-effort. Per-provider
  checklist in the Provider readiness matrix below.
- **D4 Builds.** Two distinct verbs (B5). `add` = make a repo available to clone —
  explicit, one-time, does the initial full build with progress in the CLI. `sync` =
  update the clonepack — incremental, what a push triggers, valid only on an already-
  added repo. Cloning a repo nobody added is an error naming the add command, never a
  build. Two on-demand rebuild cases, both on already-added repos: first `--depth 0` on
  a history-deferred repo, and re-clone of a repo whose artifacts were GC'd. Warmth ends
  only via TTL GC (G1).
- **D5 Frontend.** Five screens on the frozen control plane, as simple as possible.
  H0 audits the org/provider/repo model before any UI is built on it.
- **D6 fsync.** Off by default (same as git checkout), documented.
- **D7 Pricing.** Free account = public repos. **$3/seat/month per org** — an org can
  be one person (one seat) or a team. Paid = private repos + agent tokens. Private
  repos never expire while the plan is active; public repos stay warm while used
  (7-day TTL since last clone). No pinning product, no meter, no trial — the free tier
  is the trial. Sponsorship (already built) is how people support the commons.
  Self-host free forever. One breath: "Public repos: free. Your repos and your
  agents: $3. Run it yourself: free."
- **D8 Clone default (decided 2026-07-03, H1 discussion).** `ripclone clone` =
  **full editable clone — git-parity semantics** (today's default is --depth 1).
  Made viable by D4: eager-on-add prebuilds full history, so the default path is the
  warm 10× path. `--depth 1` stays as the explicit speed knob for CI/agents.
  Implementation rides C3 (item 4); docs/benchmark copy follow (F5, S0/S1).

### Provider readiness matrix (per D3)

Rows = what "supported" requires; cells = ✅ done · node = where it lands · ⛔ deferred.
OSS = the self-hostable backend + CLI. Cloud = ripclone.com.

| Capability | GitHub | GitLab | Gitea |
|---|---|---|---|
| OSS clone URL / addressing | ✅ | ✅ | ✅ (host required) |
| OSS auth header | ✅ Basic x-access-token | ✅ Basic oauth2 | ✅ token |
| OSS webhook verify + parse | ✅ | ✅ adapter | ✅ adapter |
| OSS upstream credential (X-Upstream-Token) | ✅ | ✅ | ✅ |
| OSS e2e proof (webhook→build→clone + auth-header) | ✅ today | **E3** | **E3** |
| OSS provider cleanup (keys/config/addressing) | C1–C3 | C1–C3 | C1–C3 |
| Cloud login (identity) | ✅ | ⛔ post-launch | ⛔ post-launch |
| Cloud connection / install model | ✅ GitHub App | ⛔ post-launch | ⛔ post-launch |
| Cloud per-request access check | ✅ (access.ts) | ⛔ post-launch | ⛔ post-launch |
| Cloud schema is provider-agnostic (ready for the above) | **H0** | **H0** | **H0** |

**So the launch gate per provider is:** OSS — E3 + the C-track cleanup (same work for
all three). Cloud — GitHub only; H0 makes the schema ready so adding GitLab/Gitea cloud
support post-launch is additive (new login + connection adapter + access-check impl),
not a migration. Adding cloud GitLab/Gitea also requires revisiting D2 (GitHub-only
signup is the current sybil defense) — an explicit post-launch decision, tracked below.

### The OSS/cloud boundary (who owns what)

One rule, so nodes stop re-deriving it: **OSS owns everything about repos and bytes;
cloud owns everything about people and money.**

- **OSS (turbogit):** build + serve, the added-repos store (source of truth for "is
  this repo available"), sync policy, TTL GC + the per-ref exemption flag, provider
  adapters + webhook receivers, storage/metadata/queue backends, the CLI, server-token
  auth, `/metrics`. Self-host = this alone, fully functional.
- **Cloud (ripclone-cloud):** identity (users/identities), orgs/membership/connections,
  billing + entitlement, the gateway (validate → authorize → meter → forward), product
  policy (tiered add rules, abuse limits, agent-token gate), usage metering + rollups,
  GC-exemption reconcile, the UI, the docs website, legal.
- **The four cross-repo contracts** (the only places they touch):
  1. `POST /add` — cloud policy decides, then calls OSS; OSS registers + builds.
  2. `POST /sync` — cloud webhook filters on ITS added repos, then calls OSS; OSS
     rebuilds. Self-host receives provider webhooks directly and filters on OSS state.
  3. Exemption flag — cloud reconcile writes it; OSS GC honors it.
  4. Clone metrics — CLI posts to whichever server it cloned from; cloud stores +
     rolls up; OSS accepts-and-drops (or the CLI skips when unsupported — see D-3).
- Tie-breaker for new work: if it needs a database row about a *person, org, or
  dollar*, it's cloud. If it needs a byte from a *git repo*, it's OSS.

---

## Track A — Correctness: "everything just actually works"

> Executor: Codex (small subtle diffs). Review: standard + `codex challenge`.

**A1. Phase-2 publish commit guard** — deps: none — turbogit
```
In rust/src/server.rs, build_full_in_background's editable-full publish
(~lines 5261-5308) overwrites packs, clonepack_manifest, full_clonepack and clears
archive_chunks WITHOUT checking that the ref still points at the commit this build
was for. The rebase adoption (~5291) and archive publish (~5373) DO have that guard.
A slow phase 2 for an older commit can clobber a newer completed build and empty
archive_chunks (files-mode clients then 202-poll forever). Add the same
commit-ownership guard to the editable publish path — re-read the ref and skip the
save if info.commit != this build's commit (match the existing guarded saves' style).
Add an e2e in rust/tests/ that races two syncs and asserts the newer commit's ref
survives a delayed older phase-2. Run: scripts/ci.sh test.
```
Accept: guard present on all three phase-2 saves; new e2e passes; flake run clean.

**A2. Per-repo authz on the five ungated content endpoints** — deps: none — turbogit
```
rust/src/server.rs: authorize_repo_read gates refs/sync/status/git-HTTP but NOT
cat_file_inner (~3381), file_sizes_inner (~3423), create_snapshot_inner (~3462),
get_hotfiles_inner (~3564, which also skips validate_git_rev), batch_files_inner
(~3601). Any holder of the shared server token can read private repo bytes via
/cat and /batch. Add the authorize_repo_read gate (and validate_git_rev where
missing) to all five — they already receive the provider param. Extend
rust/tests/e2e_auth.rs: unauthorized repo → 403 on each endpoint. Run: scripts/ci.sh test.
```
Accept: all five gated + rev-validated; e2e proves 403; existing tests green.

**A3. Atomic + locked writes for tokens/config** — deps: none — turbogit
```
Three files write secrets non-atomically:
1. rust/src/auth/token_store.rs:79-115 — open(truncate) in-place write; set/delete are
   unlocked read-modify-write. Crash mid-write bricks tokens.json (load() then hard-fails
   every command); concurrent CLIs drop each other's tokens.
2. rust/src/config.rs:207-224 — std::fs::write at default umask BEFORE chmod 600, chmod
   error swallowed with let _ =.
3. rust/src/provider_config.rs:66-73 — same pattern.
Write one shared helper: write to sibling tmp file created 0600, fsync, atomic rename;
wrap read-modify-write cycles in an advisory flock on a .lock sibling. Use it in all
three. Unit tests: concurrent set() from two threads loses nothing; kill-mid-write
leaves the old file intact. Run: scripts/ci.sh test.
```
Accept: one helper, three call sites, tests prove atomicity + no plaintext-at-0644 window.

**A4. Stuck-202 pair: phase-2 failure stamping + build-status CAS** — deps: A1 — turbogit
```
Two related bugs make clients 202-poll forever:
1. rust/src/server.rs ~5002: a phase-2 error is only logged; build_status stays
   "full history building" permanently (recovery poller defaults off). On phase-2 error,
   stamp build_status = "failed: <reason>" and enqueue one bounded retry.
2. update_build_status (~5853-5889) is a lossy read-modify-write: a concurrent job's
   "building" stamp can overwrite a freshly published ref with a stale copy, and
   ref_store.rs:68-71's `_ => true` fallback lets an empty-commit placeholder beat a real
   ref. Make the status update a targeted conditional write (only touch build_status,
   only if commit matches), and make should_replace_ref reject candidates with an empty
   commit. Also un-swallow the four `let _ = update_build_status` call sites (5425, 5433,
   5497, 5536) — log at error with repo+commit.
Add an e2e: force a phase-2 failure, assert status becomes failed and a subsequent sync
recovers. Run: scripts/ci.sh test && scripts/ci.sh flake.
```
Accept: failed builds visibly fail + recover; ref never regresses in the race test.

**A5. Small-fix batch: config, precedence, poll, swallowed errors, TTL fail-open, CSRF** — deps: none — turbogit
```
Six independent small fixes:
1. rust/src/config.rs:173-181 — a TOML parse error in an EXISTING config file currently
   falls back to Config::default() (so the CLI silently talks to the managed cloud and
   sends the user's token there). Make it a fatal, actionable error.
2. rust/src/bin/cli.rs:1677-1706 — upstream-token precedence comment says override →
   git credential fill → registry; code checks registry first. Make code match the
   comment (live credential helper beats stored registry token) and fix the comment.
3. rust/src/client.rs:1170-1185 — files-mode archive poll uses 40×250ms (10s) vs 40×2s
   elsewhere, then fails later with a misleading "artifact URLs expired" error. Use the
   2s interval and bail early with "archive still building for <repo>" when
   !info.archive_ready.
4. Un-swallow: server.rs:4321 (`let _ = save_branch` in reuse_existing_build — propagate),
   cli.rs:778 (`let _ = config::save` after login — propagate), auth/broker.rs:295-301
   (App-token mint failure degrades to anonymous — return the error instead).
5. AU3 (never verified fixed): signed-URL TTL for private repos is chosen from a
   client-trusted visibility header and FAILS OPEN to the long public TTL
   (server.rs ~1674-1680 in the June review; find current location). Make it fail
   closed (unknown visibility → short/private TTL) and add a test.
6. CSRF state in the auth login flow is SHA256(nanos+pid) — predictable. Use random
   bytes (rand is already a dep). One-line class fix + test that two states differ
   and are non-derivable.
Run: scripts/ci.sh lint test.
```
Accept: each has a test or is covered by existing e2e; error messages actionable.

**A6a. Cloud: `unpaid` entitlement fix** — deps: none — ripclone-cloud — SHIP FIRST, it's tiny
```
src/lib/stripe.ts:28 maps Stripe status 'unpaid' → 'past_due', and pricing.ts:22
treats past_due as entitled — an org whose dunning permanently failed keeps private
access forever. Map unpaid → 'canceled'. Add a test that pins this decision
(none exists today). Run: pnpm test.
```

**A6b. Cloud: webhook lifecycle handlers** — deps: none — ripclone-cloud
```
/api/github/webhook acts only on push: handle installation deleted/suspended/
unsuspended (stop sync, mark install), installation_repositories removed (untrack),
membership changed (invalidate permission cache). See ROADMAP.md "Webhook lifecycle".
ALSO (boundary rule): the push handler must filter on the CLOUD's added_repos before
calling the backend `/sync` — explicit-add-only, enforced cloud-side; do not
ensureTracked on push, and do not rely on the OSS 404 as the filter. (The OSS webhook
receivers filter on OSS added-state for self-hosters; each side filters on its own
store.) Test each with fixture payloads + seeded rows like the existing webhook tests.
Run: pnpm test.
```
Accept: unpaid test red-then-green; lifecycle events handled + tested.

**A-R. Track-A adversarial gate** — deps: A1-A6 — Codex then Fable
```
codex challenge on the combined Track-A diffs: try to construct interleavings where a
stale ref wins, an unauthorized read succeeds, or a token file is corrupted. Then Fable
reviews survivors and merges.
```

---

## Track B — Simplify + speed the sync/build path (top priority)

**B1. Delete-list PR** — deps: none — Kimi — turbogit
```
Delete dead weight (~2,500 lines). All items verified unreachable or test-only:
1. Blob-pack pipeline: mode.rs:26-30 hardcodes needs_blob_pack()=false, so blob_pack.rs
   (all 619 lines) + its plumbing in extract.rs (~1272-1310) is test-only. Delete both
   + their tests.
2. Dead entry points: extract_archive_streaming (extract.rs ~1888-1960, zero callers),
   materialize_worktree_from_pack (~1457-1710, test-only), split_and_store_pack
   (server.rs ~2545), enum DepthBuild (server.rs ~3888), RefInfo.head_buckets (only ever
   written empty), clone --hot-files flag (cli.rs ~84, bound to _hot_files and ignored).
3. Deprecated io_uring scheduler: worktree_writer.rs ~2255-2650 (+ its ~7 RIPCLONE_ env
   knobs) — the code logs itself as "deprecated and slated for removal".
4. The `ripclone backend` subcommand (cli.rs ~278-321, 960-1115) — server settings do
   not belong in the client; note removal in docs/CHANGELOG.md.
Do NOT touch the second extraction pipeline yet (separate node). After deleting, run
scripts/ci.sh lint test flake. Everything must stay green with zero test edits except
deleting tests of deleted code.
```
Accept: green CI, net-negative diff ≥2,000 lines, CHANGELOG note.

**B2. Dead-code flag + compiler sweep** — deps: B1 — Kimi — turbogit
```
rust/src/lib.rs:1-8 has crate-level #![allow(dead_code, deprecated)]. Remove it, build,
and delete (or #[cfg(test)]-scope) everything the compiler now flags. Judgment rule:
if it's referenced only by tests, keep only if the test covers live behavior; otherwise
delete test + code. List anything you were unsure about in your summary instead of
guessing. Run scripts/ci.sh lint test flake.
```

**B3. Collapse the duplicate extraction pipeline** — deps: B1 — Kimi, review Codex+Fable — turbogit
```
rust/src/extract.rs contains two parallel ~500-line implementations:
extract_archive_with_chunk_fetcher (~196-712) and extract_archive_from_chunk_receiver
(~941-1441). Fixes must land twice and have already diverged (the checked-math
frame-bounds fix differs between copies). Keep the receiver variant (it's the one the
main clone uses), and feed the legacy callers through a thin adapter (~30 lines) that
wraps a fetcher into a receiver channel. Byte-identical behavior: the full e2e suite
plus scripts/e2e_local.sh must pass unchanged. Run scripts/ci.sh test e2e flake.
```

**B4. Profile phase-1 sync latency** — deps: none — Kimi executes, Fable analyzes — turbogit
```
Goal: know where push→clonable time goes. Instrument the phase-1 build path
(do_sync through the phase-1 publish in rust/src/server.rs) with per-stage timing
(mirror fetch, HEAD-closure pack build, index/skeleton build, uploads, ref publish) —
reuse the existing tracing spans / --bench report style. Run syncs against 3 repos
(small fixture, oven-sh/bun, pandas) cold and incremental; produce a table. No behavior
changes. Deliverable: the numbers + the instrumentation behind RIPCLONE_BENCH.
Also measure STORAGE AMPLIFICATION while you're there: for each test repo, report
bytes-in-object-storage / repo-size, split by artifact class (head pack, history packs,
archive chunks, metadata) — this is the COGS multiplier for the cloud.
DECISION TRIPWIRE: also measure incremental push→clonable latency (push lands →
depth-1 ref serves the new commit). If p50 exceeds ~5s on the small/medium repos, the
"hybrid top-up clone" idea (post-launch list) gets PROMOTED into launch scope — the
agent story can't ship on a slow freshness path. This measurement decides it, not
optimism.
```
Accept: stage-level table for cold + incremental sync + amplification table; Fable turns
it into B6 targets.

**B5. Added-repos model: `add` (make available) vs `sync` (update)** — deps: D4 ✅, A4, C1, E1 — exec Kimi, review Codex `challenge` + Fable — turbogit

RISK NOTE: this is the highest-blast-radius node in the plan — it changes the semantic
every existing e2e assumes (today an unknown repo builds on demand; after B5 it 404s).
Budget real time for updating the existing test suite (harness setup gains an
`add` step; tests that deliberately clone un-added repos flip to asserting the 404).
Land AFTER E1 so the equivalence oracle guards the change, and give it the Track-A
adversarial treatment (codex challenge) at merge, not just standard review.
```
Introduce the concept the backend is missing today: an ADDED REPO. Today a repo is only
"built" or "not built"; there is no persistent state meaning "this repo is available to
clone, keep it fresh." Add it, and split the current overloaded `sync` verb into two:

VOCABULARY (exact, don't drift):
- ADDED REPO: a persistent record in the metadata store that this repo is available to
  clone. Distinct from BUILT (artifacts exist) and WARM (artifacts in object storage
  right now). Owned by the OSS backend — it is the single source of truth for the
  build/serve decision.
- `add`  = "make this repo available to clone." Registers it as added + does the initial
  build. One-time, explicit, human/cloud-UI driven.
- `sync` = "update the clonepack." Incremental rebuild of an already-added repo. This is
  what a push (webhook / Action / poll) triggers. NOT an alias for add.

1. ADDED-REPOS STORE. New table/record in the metadata store (works across file + SQL
   backends), keyed by the unified storage key (C1). Fields: repo id, added_at,
   history_enabled (bool), source (cli|cloud|api). Cross-process visible (a worker and
   the server must agree). This REPLACES the webhook allowlist as the build filter.

2. `ripclone add <repo>` → `POST …/add`:
   - Validate reachability + credential up front (add builds, so a repo it can't fetch
     or isn't authorized for fails fast here, not mid-build).
   - Register as added, then enqueue the initial build: phase 1 (skeleton/HEAD/index) +
     archive + full history. Full history is DEFAULT ON (that's the 10× full clone);
     `--no-history` (and/or a server size threshold) defers it → sets history_enabled=false.
   - Stream progress in the CLI: "depth-1 ready… archive ready… history building… done."
     Returns success once depth-1 + archive are ready (repo is usable); full history
     continues in the BACKGROUND and shows in status. So `add bun` ≈ wait ~20s; `add
     linux` returns usable in ~40s with history still building.
   - NOTE, deliberate order change: today phase 2 builds history THEN archive
     (docs/DESIGN.md). Reorder to archive-before-history — files mode gets ready
     sooner, and history may be deferred entirely (--no-history). Update DESIGN.md's
     two-phase section to match.
   - `add` on an already-added repo is idempotent (behaves like a `sync`).

3. `ripclone sync <repo>` → `POST …/sync`:
   - Requires the repo to be ADDED. CLI `sync` of a non-added repo → error "not added;
     run `ripclone add <repo>`". The webhook/Action/poll path checks added-ness and
     just IGNORES pushes for non-added repos (no error — webhooks fire for everything).
   - Incremental over the existing base (LSM delta / CDC frame reuse — never a full
     rebuild). Full-rebuild fallback only if there is no base (added but never-built or
     fully GC'd). Updates every built class; if history_enabled=false, leaves history
     deferred.

4. CLONE DECISION TREE (ref resolve), 4-way:
   - added + warm      → 200 serve
   - added + building  → 202 + poll
   - added + cold (GC'd) → enqueue a sync-rebuild → 202 + poll
   - NOT added         → 404 with machine-readable code `repo_not_added`;
     CLI prints "run `ripclone add <repo>`". NO build is enqueued.
   Two on-demand build cases exist (both are "added → rebuild", neither is
   "not-added → build"): (a) first `--depth 0` on a history-deferred added repo;
   (b) re-clone of an added repo whose artifacts were GC'd.

5. SELF-HOST = explicit add, universal. No auto-add mode. A clone of a non-added repo
   errors everywhere the same way.

5b. BRANCH SCOPE (was unspecified): `add` builds the DEFAULT branch. Pushes to other
   branches of an added repo follow the existing policy — default branch always syncs;
   other branches sync only once first built (via `add <repo> --branch X` or an
   explicit sync); RIPCLONE_WEBHOOK_WARM_ALL=1 keeps its warm-every-pushed-branch
   meaning for self-hosters. Warmth and GC are per-ref.

5c. WARMTH INVARIANT (restored — dropped in a rewrite): TTL GC (G1) is the ONLY thing
   that ends warmth — never a push, never a sync. Entitled private repos are exempt
   (the G2 reconcile drives the backend exemption flag). Being added survives GC:
   eviction makes an added repo cold, not gone (decision-tree case 3).

6. AUTH: the `add`/`sync` endpoints require the server token like other build endpoints;
   on cloud the gateway gates account/entitlement before forwarding (unchanged).

7. STATUS: reuse the A4 build-status model; the status endpoint reports per-class
   readiness (depth1_ready, archive_ready, history: ready|building|deferred|failed) so
   both `add` and `clone` render progress from it.

8. MIGRATION: seed the added-repos store on upgrade — every repo that currently has
   artifacts (has a ref) becomes added; every entry in the old webhook allowlist becomes
   added. After migration the allowlist env is deprecated (document it).

CLOUD CONTRACT (note for the ripclone-cloud side, not this node): cloud "Add repo" calls
`POST …/add`; OSS owns added-state; cloud's repos table becomes the product record
(who/when/billing) that mirrors it. Rename cloud `tracked_repos` → `added_repos`
(H0/G-track). CONSISTENCY ORDER for the two tables: cloud runs its policy checks, calls
OSS `/add`, and writes its own row ONLY after OSS succeeds — OSS is the source of
truth, the cloud row is derived. The G2 reconcile job also compares the two sets and
alerts on drift (a cloud row with no OSS added-state, or vice versa). Cloud webhook
calls `POST …/sync` only for repos in ITS added_repos (see A6b) — it never relies on
OSS's 404 as the filter.

Tests: clone of not-added repo → repo_not_added, NO enqueue; add registers + builds all
classes + streams progress + repo becomes cloneable; sync on added repo updates
incrementally; CLI sync on non-added → error; webhook push for added repo → sync
enqueued, for non-added → ignored; added repo GC'd cold → clone triggers rebuild (202)
not error; `add --no-history` → depth-1+archive only, first `--depth 0` then 202s and
builds; migration marks pre-existing built repos as added; entitled private repo
survives GC while an idle un-exempt ref is evicted (per-ref); pushes to a non-default,
never-built branch of an added repo do NOT build unless warm_all. Run scripts/ci.sh
test e2e flake.
Docs: quick start uses `add` to make a repo available and explains `sync` = update
(push-driven); the GitHub Action keeps an added repo synced, it does not add it.
```

**B6. Sync-path efficiency + safety-critical dedup** — deps: B4 for item 1 ONLY;
items 2–3 are data-independent and can run anytime — spec Fable, exec Kimi — turbogit
```
1. Whatever B4's profile shows (bitmap write on the editable path is a known candidate,
   docs/ROADMAP.md §9).
2. CachingRefStore holds the global write lock across S3 CAS retries
   (ref_store.rs:645-674) — one contended ref write stalls every /refs on the node.
   Drop the guard before inner I/O; double-checked read path.
3. SAFETY-CRITICAL dedup: manifest_chunk_refs / collect_manifest_hashes exist
   byte-similar in server.rs (~2378/2406) and remote_gc.rs (~484/469). If a new manifest
   field lands in one and not the other, GC deletes live chunks. Move to one shared home
   (clonepack.rs or similar); both callers use it. Same for the path-validation helpers:
   path_from_bytes / validate_relative_path / safe_create_dir_all are pub in
   worktree_writer.rs (~72-155) and duplicated privately in extract.rs (~33-46, 713-782)
   — single fsutil module, delete the copies. Run full CI.
```

**B7. Env-knob cut** — deps: B1, B2 — Kimi — turbogit
```
128 distinct RIPCLONE_* vars exist in rust/src. Produce the inventory (grep), then:
keep ~10-15 user/operator-facing (server/token/storage/metadata/queue/fetch-retry/mode/
cache/fsync/metrics/webhook/GC), demote the rest to private constants at their current
defaults, and write docs/CONFIG.md as the single reference (user vs operator vs the
handful of expert knobs that survive). Delete deprecated aliases RIPCLONE_TOKEN,
RIPCLONE_TOKEN_HASH, RIPCLONE_URL entirely (pre-1.0, no install base).
Run scripts/ci.sh lint test.
```

**B8. Extract the build pipeline from server.rs** — deps: ALL other OSS code nodes merged — Kimi, review Codex+Fable — turbogit
```
server.rs is 8,450 lines with ~10 responsibilities. Move ONLY the build pipeline
(do_sync → two-phase build → publish, ~1,200 lines — it has no HTTP dependency) into
rust/src/build/ (pipeline.rs, two_phase.rs). Pure code motion: no logic changes, no
renames beyond module paths. The diff should be reviewable as moves. Run full CI.
```
Run this LAST, after every other OSS node merges — everything touches server.rs and
parallel runs mean merge hell. Safe to slip post-launch if needed: it's organization,
not behavior.

---

## Track C — Provider/config redesign (gated on D1, D3)

**C1. Storage-key unification** — deps: D1 — Codex or Fable-spec + Kimi — turbogit
```
Make the storage key uniform: {provider_id}/{escaped_path} for ALL providers including
the github default (rust/src/provider.rs storage_key/mirror_dir_name, ~455-493), and
delete the bare-key back-compat branch + the guess-based ambiguity in parse_storage_key
(~580). Mirror dirs become {provider}_{escaped_path}.git uniformly. Update every test
that bakes in bare owner/repo keys. Breaking change is intentional (pre-launch, dev data
wiped). Grep for split_once('/') on storage keys across rust/src to catch stragglers.
BOUNDARY: this is STORAGE-INTERNAL only — HTTP routes, the gateway contract, and CLI
addressing are unchanged (routes are already provider-scoped). If you find yourself
editing a route or anything in ripclone-cloud, stop — out of scope.
Run full CI incl. e2e.
```

**C2. Provider config/token source cut** — deps: C1 — Kimi — turbogit
```
1. Config sources five → two: keep RIPCLONE_PROVIDERS env JSON + config.toml. Delete
   providers.json reading/writing (provider_config.rs load/save/add/remove, the legacy
   array format, RIPCLONE_PROVIDERS_CONFIG).
2. Token chain: config-declared token or per-request X-Upstream-Token only. Delete
   RIPCLONE_PROVIDER_<ID>_TOKEN; fold RIPCLONE_GITHUB_TOKEN into the github default's
   token resolution rather than a special case in ProviderRegistry::new().
Run full CI; update docs/BACKENDS.md provider section.
```
STRETCH (only if C1-C4 land early): collapse ProviderKind's match arms into preset
data rows. ~40 lines of design purity; skip without guilt.

**C3. CLI addressing + surface cut** — deps: C2 — Kimi — turbogit
```
1. One addressing form: `provider:path` where the prefix MUST be a registered instance
   id — remove the raw-hostname fallback (cli.rs ~347-363) that makes any ':' ambiguous.
   Keep --provider as an alias; delete the RIPCLONE_PROVIDER env var.
2. Hide internal subcommands with #[command(hide = true)]: sidecar, cat, snapshot,
   prefetch, build-archive, extract-archive, train-dictionary.
3. Unify login UX: `ripclone login` against a non-cloud --server should route to the
   self-host auth flow (or error with the exact right next command); make the 401 hint
   say `ripclone login` for the cloud, and drop cloud-specific plan/token hints
   (client.rs:68-104) when the server != the cloud default.
4. D8: flip the `clone` default from --depth 1 to full history (git-parity). --depth 1
   remains the documented speed knob. Update the default-mode env/docs and any test
   that assumed the depth-1 default; the /start page and README examples then read
   `ripclone clone owner/repo` with no flags, honestly.
Run scripts/ci.sh test; paste `ripclone --help` output in the summary.
```

**C4. Bitbucket cut** — deps: D3 — Kimi — turbogit
```
Remove ProviderKind::Bitbucket / its preset (it has no webhook adapter and no tests —
a half-promise). Remove from README/docs. Open a GitHub issue titled "Bitbucket
provider" capturing what re-adding takes: one preset row + one webhook adapter
(X-Event-Key, often unsigned) + one e2e. Run CI.
```

---

## Track D — Trust

**D-1. `--verify-upstream`** — deps: none — spec Fable, exec Codex — turbogit
```
(Fable spec, sketch:) For editable clones, cross-check the tip: client does one
ls-remote against the UPSTREAM host (not the ripclone server) for the requested ref,
and verifies the installed objects chain to that sha (fsck-level connectivity from the
pinned tip). Result: ripclone drops out of the trust base — it cannot tamper with an
editable clone. Flag --verify-upstream (env RIPCLONE_VERIFY_UPSTREAM). DEFAULT
SCOPING (constraint): agents cloning private repos with an rc_live_ token often hold
NO upstream credentials — they cannot ls-remote GitHub at all. So: default ON for
public repos and whenever an upstream credential is available; default OFF (with a
warn-level note) for credential-less private/agent flows, docs explaining the residual
trust. Clear error when the upstream is unreachable (fall back to warn or fail per
flag). Files mode: not verifiable this way — docs must say so plainly.
```

**D-2. Security model page** — deps: D-1 — Kimi draft, Fable review — ripclone-cloud site
```
Write the user-facing security page: what ripclone stores (mirrors + derived artifacts),
the trust boundary (shared server token + per-repo authz; gateway delegates to GitHub),
what --verify-upstream guarantees for editable clones and what files mode does not
guarantee, credential flow (X-Upstream-Token is per-request, never stored in the queue),
what happens on plan lapse / uninstall (mirror + artifact deletion policy — state one),
and the telemetry story (see D-3). Plain calm language, no marketing.
```

**D-3. Telemetry disclosure** — deps: none — Kimi — turbogit
```
The CLI fire-and-forgets clone metrics to the configured server after every clone
(cli.rs ~1370, report_clone_metrics). RIPCLONE_NO_METRICS exists but is undocumented.
Document exactly what is sent (read the code, list the fields) in README + docs/CONFIG.md
+ the security page; make sure --no-metrics / env works against any server; add a test.
BOUNDARY: the metrics-receiving route is CLOUD-only today. Decide + implement one of:
the CLI only posts when the server advertises metrics support (via /v1/version or the
ref response), or the OSS server grows a cheap accept-and-drop route. Never let a
self-host CLI spam 404s at its own server.
```

---

## Track E — Testing gate ("comprehensively tested and trusted")

**E1. Byte-for-byte equivalence oracle** — deps: none, start now — Kimi — turbogit
```
The product's core claim has no direct test. Add rust/tests/e2e_equivalence.rs +
a mode in scripts/e2e_local.sh: build a fixture repo containing symlinks (incl. one
non-UTF-8 target), exec bits, empty files, unicode filenames, deeply nested dirs, an
empty dir via .gitkeep, a >8MB binary file (crosses chunk boundaries), and a gitlink
(submodule entry). git clone it and ripclone-clone it (editable depth 1, depth 0, and
files mode); compare with `diff -r --no-dereference` on worktrees, plus refs, HEAD, and
tags for editable. Also assert git fsck + git status --porcelain empty. On Linux CI,
run the whole oracle twice: RIPCLONE_IO_URING=1 and =0, so the io_uring and POSIX
writers are proven equivalent (they have no parity test today). Include an LFS fixture:
a repo with .gitattributes filter=lfs + pointer files. ripclone's policy is
pass-through (pointers on disk, LFS blobs never stored/served — client runs `git lfs
pull` against the provider); the test pins that an editable clone of an LFS repo still
reports `git status` clean (set the repo config so the smudge filter doesn't mark
pointer-materialized files modified — native git-lfs clones smudge at checkout, we
don't). Wire into scripts/ci.sh test. Use the existing in-process harness
(rust/tests/common/mod.rs).
```

**E2. GC race + MinIO in CI** — deps: none — Kimi — turbogit
```
1. New e2e: start a clone, stall it mid-chunk using the existing fault hook
   (start_server_faulting), run RemoteGc with grace=0, assert the clone either completes
   or fails cleanly with a retryable error — never a corrupt tree.
2. e2e_remote_gc_s3.rs (4 tests) never runs in CI (env-gated on S3 creds). Add a MinIO
   service container to .github/workflows/ci.yml and a ci.sh target so the S3 GC suite
   runs on every PR touching rust/. Replace the fixed 2s sleeps with bounded polls.
```

**E3. Provider webhook e2es — LAUNCH GATE (D3: OSS GitLab/Gitea support)** — deps: D3 ✅ — Kimi — turbogit
```
Extend rust/tests/e2e_webhook.rs to GitLab (X-Gitlab-Token verification + payload) and
Gitea (HMAC + payload incl. branch delete) end-to-end: webhook → build → clone. Add
gitlab-shaped AND gitea-shaped HTTP origins to e2e_multi_provider.rs exercising
auth-header injection on the fetch (GitLab = Basic base64(oauth2:token), Gitea =
token header). This is what makes "GitHub + GitLab + Gitea supported" an honest claim —
it gates G2.
```

**E4. Expiry mid-clone** — deps: none — Kimi — turbogit
```
Two tests: (1) mint a server session token with expiry shorter than a fault-slowed
clone; assert refresh-or-clean-failure, never a partial tree. (2) S3 path: signed URL
TTL shorter than a slowed chunk download → assert the retry path re-resolves URLs or
fails with an actionable error (whichever the code intends — read it and pin behavior).
```

**E5. De-flake e2e_freshness.rs** — deps: none — Kimi — turbogit
```
rust/tests/e2e_freshness.rs:88-101 races real sleeps (800ms push inside a 2500ms
injected delay, then asserts exactly 2 builds). Replace wall-clock races with an
explicit hook (channel/barrier via the existing injected-delay test seam) and replace
sleep-then-count with wait-for-completion-signal-then-count. Must survive
scripts/ci.sh flake 10 times consecutively.
```

**E6. Feature inventory → the G2 gate list** — deps: none, start now — Kimi scan, Fable+User classify
```
Produce a table of EVERY user-visible surface in BOTH repos. turbogit: CLI subcommands
+ flags, RIPCLONE_* env vars, HTTP endpoints, webhook providers, install channels, doc
claims (README + docs/). ripclone-cloud: pages, server actions, API + webhook routes,
gateway behaviors, emails, doc claims. (The G2 gate covers the product, not one repo.)
Columns: surface | repo | documented? | e2e-tested? | works? (best evidence).
No judgments — just the inventory. Fable + Russell then mark each keep/flag/cut, which
becomes the tracked launch checklist for gate G2.
CLASSIFICATION RUBRIC (pre-agreed, so the keep/flag/cut session is fast — Fable
applies it, Russell reviews only the contested rows):
- documented + tested + works → KEEP.
- works but untested → add the test if it's cheap (one e2e), else FLAG experimental.
- half-built or broken → CUT + issue, unless it's on the launch path → fix-node.
- internal/dev tooling → HIDE (not user surface, not cut).
- when unsure → FLAG experimental. Flagged = works-as-is, labeled, no support promise.
Two classifications are pre-flagged for the decision step (don't skip them):
- `ripclone worktree` add: writes in place, no staging, no chunk-fetch retry — an
  interrupt leaves an unrecoverable half-repo (client.rs ~2313-2472). Fix (temp-dir
  staging + route through fetch_artifact_with_retry) or mark experimental + issue.
- Empty-repo clone: unsupported (404 at resolve). If kept unsupported, the error must
  say "repository has no commits" — not a bare 404. Issue exists in docs/ROADMAP.md §11.
```

---

## Track F — Self-host + release

**F1. Quick-start truth** — deps: none — Kimi — turbogit
(Known churn, accepted: this fixes the quick start with today's `sync` verb; B5 later
renames the flow to `add` and touches it again. Fixing the broken first-run now is
worth writing the section twice.)
```
README Quick start fails as written: ripclone-server refuses to start without
RIPCLONE_SERVER_TOKEN (server.rs:5893) and the docs never mention it. Fix the README
server/sync/clone examples to include the token; also change the server default dirs
from /data/cache & /data/repos (bin/server.rs:12-16) to ~/.local/share/ripclone/ so a
bare binary works on a laptop; keep /data via flags in the Docker docs. Add a startup
validation that warns loudly when RIPCLONE_QUEUE is SQL but metadata/ref store is
file-backed (the silent state-split footgun, BACKENDS.md:160). Test: the exact README
commands run on a clean machine.
```

**F2. Static builds or preflight** — deps: none — Kimi, Fable reviews CI — turbogit
```
Prebuilt binaries dynamically link libgit2/openssl/zstd and fail cryptically on minimal
images; install.sh:71 hides it with `|| true`. Preferred: build release binaries with
vendored/static C deps (feature flags exist for vendored openssl/libgit2 — check
Cargo.toml) or musl targets. Fallback if that's a rabbit hole: install.sh preflight
that detects missing shared libs and prints the exact apt/brew line, and remove the
`|| true`. Also: add ripclone-worker to the release tarball (release.yml:50 copies only
3 binaries; README claims 4). Verify by running install.sh output on a clean
ubuntu:24.04 container.
```

**F3. Release dry-run** — deps: F2 — User + Kimi
```
FIRST, reality-check the release identity: README + install.sh point at
github.com/russellromney/ripclone/releases, but the local repo dir is "turbogit" —
confirm the GitHub remote is actually named ripclone (rename if not) or fix every URL.
Also delete ripclone-bench-key-3.pem (an EC2 private key) from the repo root.
Then cut a real pre-release tag. Verify: tarball installs + runs on clean Ubuntu
container and clean macOS; pip wheel installs and runs (manylinux audit passes with the
vendored-C build); `ripclone version` compatibility check works against the dev server;
uninstall documented. Fix whatever breaks; repeat until boring.
```

**F4. git-remote-ripclone decision** — deps: none — Kimi — turbogit
```
Either document it (a docs page: ripclone:// URL syntax, server resolution via
git config remote.<name>.ripcloneServer, the push story / pushInsteadOf workaround,
depth limits) or remove it from the tarball + README and open an issue. Recommend:
document — the code is careful and it's a real differentiator. Add its e2e to ci.sh
if not already run.
```

**F5. OSS docs cleanup** — deps: B7, C-track, D6 — Kimi — turbogit
```
1. README restructure: pitch + ONE benchmark table + quick start + links. Move full
   benchmarks to docs/BENCHMARKS.md (kill the duplication), providers/build-options to
   docs/. Fix stale claims: DESIGN.md "15-32×" vs README numbers; ROADMAP.md's old
   full|fast|hybrid|skeleton mode names → editable|files.
2. Move internal artifacts (ADVERSARIAL_REVIEW*, WRITER_SCHEDULER_EXPERIMENT,
   ARCHIVE_AB_RESULTS, DISPATCHER, GITHUB_INTEGRATION, ROADMAP) to docs/internal/.
3. Add: TROUBLESHOOTING.md (missing libgit2, 202 warming, 401 vs 403, config drift),
   uninstall section, fsync durability note (D6), telemetry note, and the LFS policy
   statement: "LFS objects come from your git host, not ripclone — run `git lfs pull`
   after cloning." (Pass-through by design; blobs never stored.)
Per goal (e): user docs live on the ripclone-cloud site (H4); the repo keeps quick
start + dev notes + the self-host operator reference — a DELIBERATE divergence from
"all docs on the website": operator docs version with the code they configure, and
self-hosters read the repo, not the SaaS site. The website links to them.
```

---

## Track G — Money safety + cloud ops

**G1. OSS TTL GC + pin flag** — deps: none — spec Fable, exec Kimi — turbogit
```
(WARMTH.md's OSS half.) Per-ref last_accessed_at (persisted), a periodic sweep evicting
clonepack artifacts for refs idle past RIPCLONE_WARM_TTL (default 7d), a warm_pinned
flag exempting a ref, GET /status reporting warm/last_accessed_at/pinned. Eviction =
artifact deletion; next clone rebuilds via the existing 202 path. Turn remote GC
default ON with the ledger grace, and give the recovery poller a sane non-zero default
(RIPCLONE_POLL_INTERVAL_SECS is 0 today — webhook-less deploys never self-heal missed
or stuck builds). E2e: idle ref evicted, pinned ref survives, clone after eviction
rebuilds cleanly.
```

**G2. Cloud: access fences + guardrails (implements D2+D7)** — deps: G1, D2 ✅, D7 ✅ — Kimi — ripclone-cloud
```
1. Signup: free accounts are GitHub sign-in ONLY. Email magic-link (if kept) is for
   paid accounts / additional login methods on existing accounts, never free signup.
   This replaces captcha/per-domain signup defenses — delete that scope.
2. NO product meter on public clones (D2 amendment). Instead: (a) a high-threshold
   per-account abuse rate limit (Turso-backed — must survive Fly suspend and coordinate
   across machines; threshold generous enough that no human ever sees it), and (b)
   per-account daily clone-count instrumentation (usage_events already has the data —
   add the rollup + a tripwire alert via G3 when any account sustains fleet-scale
   public cloning, so pricing gets revisited with evidence, not vibes).
3. GC-exemption reconcile: hourly job marks a repo's refs exempt on the backend iff
   the repo is private and its org is entitled; clears it on lapse. (Uses the OSS pin
   flag from G1 as the mechanism; there is no user-facing pinning.)
4. Build guardrails — tiered add policy (decided 2026-07-03). At add-preflight, one
   GitHub API call returns size + stars; enforce BEFORE any fetch:
   - size ≤ SIZE_FREE (default 2 GB), public → any account may add.
   - size > SIZE_FREE → allowed only if stars ≥ STAR_MIN (default 500) OR the adder
     has push access to the repo (permission check we already have) OR the add is by
     a paid org. Clear rejection message naming the rule.
   - size > SIZE_HARD_CAP (default 10 GB) → rejected for everyone ("contact us").
   All thresholds env-configurable. Plus: per-account add rate limit, global
   concurrent-build ceiling. Log every rejection. (OSS side keeps only a dumb
   RIPCLONE_MAX_REPO_SIZE env — the tiered policy is cloud product logic.)
5. Agent-token entitlement gate (implements D7's "agent tokens are paid"): CREATING an
   agent token requires the owning org to be entitled (active/past_due plan); on plan
   lapse, existing agent tokens stop resolving (same grace as private repos) rather
   than being deleted — they work again on resubscribe. Personal tokens stay free.
   UI: the agent-token create button shows the upgrade path when un-entitled.
6. Tests: abuse limit trips at threshold + 429 shape; unpaid org's pin lapses; add-size
   rejection; tripwire rollup counts correctly; agent-token creation blocked when
   un-entitled; existing agent tokens fail closed on lapse; personal tokens unaffected.
```

**G3. Observability + alert** — deps: none — Kimi — both repos
```
Cloud: expose queue depth, build failure rate, sync-task age, and worker liveness
(the OSS server already has /metrics — surface the backend's numbers plus syncTasks
state). One alert path that actually reaches Russell (email or webhook) on: queue depth
> N for 10m, build-failure rate spike, /readyz failing. A wedged worker must be visible
within minutes, not via user complaints.
Also: instrument AGENT-TOKEN usage separately (clones + builds per agent token per
install, rolled up monthly). Agent tokens are free today; this is the data that decides
whether/how to price them later. Metrics only — no billing change.
```

**G4. Backups** — deps: none — User + Kimi
```
Define the Turso backup story (Turso has PITR on paid plans — confirm tier; else
scheduled dumps to Tigris). Script the restore; DRILL it once against a scratch DB;
write RESTORE.md. Cover the OSS metadata store for the cloud backend (Turso) the same way.
```

**G5. Cloud doc-truth batch** — deps: none — Kimi — ripclone-cloud
```
README.md: "$2/seat" → $3, remove workspace language (matches PRICING.md/pricing.ts).
Retire SYNC_QUEUE_DESIGN.md (superseded banner). Resolve the SYNC_LATENCY_DESIGN B1
("auto-sync every installed repo") vs UX.md ("explicit add only — LOCKED") contradiction
in favor of explicit-add; fix the doc. Mark CONTENT_AUTH_DESIGN.md superseded-by-
implementation and WARMTH.md implementation status. Update IDENTITY_DESIGN.md to
shipped-tense. Fix permCache/visCache unbounded growth (access.ts:64,91 — add eviction)
and the authz inconsistency where repo management requires connector but billing
requires membership (repos.ts:52 vs userManagesInstallation — unify on membership).
Phase-0 leftovers from ROADMAP.md: surface `ripclone version` / min-version in the UI,
and sweep any copy still showing the old /v1/repos/{owner}/{repo} path (pre-provider).
Rewrite PRICING.md to the D2/D7 model: free account (GitHub sign-in only) = unlimited
public clones + adds; $3/seat/mo per org (a personal org is one seat) = private repos
+ agent tokens; private repos never expire while the plan is active; public repos stay
warm while used (7-day TTL); no pinning, no meter, no anonymous tier, no trial;
sponsorship = the support-the-commons channel; self-host free. UX.md flow 1
(anonymous aha) is superseded — update it.
```

**G7. Cloud side of the add/sync contract (implements B5's cloud contract)** — deps: B5, G2 — Kimi — ripclone-cloud
```
B5 defines the OSS half; this node is the cloud half — previously unowned:
1. Gateway: forward `POST /add` (tiered policy from G2.4 runs FIRST, then forward with
   the internal token + installation token for private); pass through B5's statuses
   untouched — 404 repo_not_added, 202 building + status polling — so the CLI renders
   the same experience through the cloud as against self-host.
2. "Add repo" UI flow: picker (from the connection's grant for private; owner/repo
   entry for public) → policy check → OSS /add → write the cloud added_repos row
   AFTER OSS succeeds (consistency order per B5) → show live build progress from the
   status endpoint.
3. Update the sync path: webhook handler + any manual "sync now" action call
   `POST /sync` (never /add), filtered on cloud added_repos (A6b).
4. Deploy lockstep: B5 changes the backend API; deploy the backend and this cloud
   change together (all-dev, no compat shim needed — but do it as one cutover, and
   bump the protocol version so an old CLI gets a clear error, not a mystery 404).
5. Tests: policy-rejected add never reaches OSS; successful add creates both records;
   OSS-failed add creates neither; 404/202 passthrough shapes; sync-not-add on push.
```

**G6. Prod cutover** — deps: everything above — **[USER]**, checklist in ROADMAP.md:421-461
Rotate Stripe key FIRST. Then live prices/webhook/portal, prod GitHub App, prod
Turso + Fly, fresh AUTH_SECRET, DNS, e2e + sandbox smoke.
PLUS: **seed the commons before opening the doors** — add + fully build the showcase
list from Russell's account (bun, pandas, react, linux, + ~10-20 popular repos across
languages). The landing page's 60-second flow (H1) and every demo depend on these
being warm on day one; nobody else will have added anything yet. Verify each with a
real timed clone from an external machine.
PLUS: **launch-spike smoke** — the cloud runs on Fly with auto-suspend; an HN-shaped
burst (hundreds of signups + clones of seeded repos in minutes) is the realistic
worst case. One k6/hey run against signup, ref-resolve, and device-flow on the prod
setup; fix cold-start pain (min_machines_running=1) before, not during, the thread.

---

## Track H — Frontend rebuild (gated on D5)

**H0. Connection-model redesign to the researched target** — deps: none — Fable + User
— ✅ SIGNED OFF 2026-07-03 (new IDENTITY_DESIGN.md in ripclone-cloud). H1 unblocked.
```
The current identity/connection model is confusing. We researched how Vercel, Netlify,
CircleCI (both eras), Buildkite, Codecov, Sentry, Depot, and Graphite model this
(2026-07-03; full report in the session record). The consensus target model:

- ORG: the only tenant, standalone, never derived from a provider org. Personal use =
  a 1-member org auto-created at signup. Billing ($3/seat, active-seat true-up — keep
  ours) and agent tokens hang here.
- MEMBERSHIP (org_id, user_id, role): our own roles, owner/member. Never mirror
  provider ACLs into roles.
- USER_IDENTITY (user_id, provider, provider_user_id, login_cache): proves "this user
  is GitHub user #N". Keyed on the numeric id; login is display-only. THIS TABLE IS
  MISSING TODAY — identity is smeared across users.github_id and the email-accounts
  work, which is the root of the confusion.
- PROVIDER_CONNECTION (org_id, provider, installation_id, provider_account_id,
  login_cache, status active|suspended|deleted): the GitHub App install IS the
  connection — a visible, deletable object in org settings. Generalize today's
  `installations` table. Unique index on (provider, installation_id): one provider
  org belongs to exactly one ripclone org (GitHub enforces this anyway; sharing
  across orgs is deferred until someone pays for it).
- REPO (org_id, connection_id, provider_repo_id, name_cache, private): explicit add,
  picked from what the connection grants, verified live against the provider.
  Numeric-id keyed so renames never break anything.
- AUTHZ stays as built: live provider check per request, cached short-TTL, webhook-
  invalidated, plus a user-facing "re-sync" button. Correct today — don't touch.
- LIFECYCLE: installation.deleted → connection status=deleted, private serving stops,
  rows kept for grace-period reconnect; suspend/unsuspend toggle status;
  installation_repositories.removed deactivates repos; renames are free (numeric ids).
  Provider events NEVER auto-remove org members or seats.

SEAM REQUIREMENT (load-bearing — this is why we can punt cloud GitLab safely): GitHub
is ONE implementation of a `GitProvider` interface (verifyReadAccess / connectionForOwner
/ token / members / mintInstallationToken), and the login flow sits behind a provider-
login seam. GitHub is the only impl at launch, but NO GitHub-specific assumption may
leak into the gateway, billing, seats, or UI — those read the interface and the
provider column, never `github_*`. Test the seam by writing a stub second provider in
tests and asserting the gateway/billing paths compile and route against it. Done right,
adding GitLab.com later = implement the interface + add a login button + a connection
screen. NOT a migration. (Billing is already provider-agnostic — keep it that way.)

Known pitfalls this avoids (all with public scar tissue): tenant-mirrors-provider-org
(CircleCI legacy — no migration path out), login-string keys (rename 404s — Codecov),
single-user OAuth connections that rot when that human leaves (Codecov team bot),
namespace-exclusivity complaints (early Vercel/Depot — both lifted).

Task: audit today's schema (drizzle/, src/lib/orgs|repos|access|seats,
IDENTITY_DESIGN.md) against this target; write the delta as a one-page doc replacing
IDENTITY_DESIGN.md — what renames (installations→provider_connections,
tracked_repos→added_repos per B5's vocabulary), what's added (user_identities),
what's deleted/merged, and the migration steps. Russell signs off;
H1 builds on it.
```

**H1. IA + screen spec** — deps: D2 ✅, D5 ✅, D7 ✅, H0 — Fable + User
```
Fable drafts the five-screen spec from UX.md's locked decisions + D2/D7: (1) landing:
signup→first-clone in UNDER 60 SECONDS is the hard requirement — GitHub OAuth (one
click) → `ripclone login` device flow → copyable clone command. The command MUST target
an ALREADY-WARM showcase repo (the G6 seed list) — signup→add→wait-for-build→clone
blows the 60s budget; the user's own repos come second. Show the speed proof (real
benchmark numbers / demo repo) above the fold since there's no anonymous path;
(2) org page as home: connect → subscribe → share-with-team block, seat NAMES not
counts, repo list with warm state + "cold in N days" (public) / "warm while your plan
is active" (private); (3) tokens (personal + agent, shown-once); (4) usage (clones,
top repos, value-visible time-saved metric from clone_metrics); (5) settings. Sidebar:
pinned org(s), Tokens, Usage, Settings — Public list folded into the add-flow.
The free→paid upgrade moment gets explicit design attention: it's the private-repo /
agent-token gate (403 with the exact upgrade path), NOT a clone meter — no meter
exists. Sponsorship wall stays as the support-the-commons surface.
Russell approves before H2.
```

**H2. Implement the five screens** — deps: H1 — Kimi — ripclone-cloud
```
Rebuild src/app pages per the approved spec. HARD CONSTRAINT: do not modify src/lib,
src/app/api, src/app/v1, or drizzle/ — the control plane is frozen; if a screen needs
data the lib doesn't expose, list it in the summary for a reviewed follow-up. Reuse the
existing design system components where they exist. Delete pages the spec drops.
pnpm test must stay green; include screenshots of each screen in the summary.
```

**H3. Design + QA pass** — deps: H2 — Kimi (browse/QA), Fable review
```
Run the site locally, walk flows 1-11 from UX.md, screenshot each, fix visual/copy
issues, verify empty states point at the next action. Then the onboarding copy: the
share-with-team block post-checkout, the seat explainer ("5 seats = 5 people cloned
private repos in 30 days"), offboarding consequences note.
```

**H5. Legal + support basics** — deps: D-3 — Kimi draft, User review — ripclone-cloud
```
Required for a paid product; currently assigned to nobody:
1. Terms of Service (subscription, acceptable use incl. the abuse thresholds, service
   changes, self-host license unaffected).
2. Privacy policy — the legally-load-bearing home of the telemetry disclosure (D-3
   documents it technically; this is the policy): what's collected (clone metrics,
   usage events, GitHub profile basics), retention, processor list (Stripe, Turso/Fly,
   Tigris, GitHub), contact. Plain language, no boilerplate wall.
3. A support channel, chosen and linked in the footer + error pages: support@ email +
   GitHub issues for OSS. Set expectations (solo founder, best effort).
Use a template service or plain-language generator as the base; Russell reviews.
```

**H4. Website docs** — deps: F5, D-2 — Kimi — ripclone-cloud
```
The /docs section becomes the canonical user docs (goal e): install → login → clone;
why it's fast (one honest page: prebuilt clonepacks, parallel chunk downloads, direct-
from-storage URLs — with the real numbers incl. the 2-4× depth-1 framing); private
repos + billing; security page (D-2); self-host pointer to the repo docs. Humanizer
pass on all prose.
```

---

## Node sizing (for session budgeting)

S = one focused session · M = 1-3 sessions · L = several sessions + review cycles ·
XL = the big ones; expect iteration. Sizes assume the node's spec as written.

| Size | Nodes |
|---|---|
| S | A6a, C4, D-3, F1, F4, H5, A-R (review session) |
| M | A1, A2, A3, A4, A5, A6b, B2, B4, B6, C2, C3, D-1, D-2, E2, E3, E4, E5, E6 (scan), G3, G4, G5, G6 (execution), H1 (spec), H3 |
| L | B1, B3, B7, B8, C1, E1, F2, F3, F5, G1, G2, G7, H4 |
| XL | **B5** (highest blast radius + test-suite update), **H2** (five screens) |

Practical read: Track A is a week of Codex sessions; B5 and H2 are the two long poles;
C1 and G2 are the sneaky-big ones (C1 touches every key-baking test, G2 is many small
fences that each need a test).

## Waves

Waves are pacing, not law: any node with deps:none can start whenever an executor
session is free. The hard ordering is only: D-decisions → C-track; B1→B2→B3;
B4→B6; A4+C1→B5; A-nodes → A-R; B5+G2→G7; everything → B8 → wave-4 review.

- **Wave 1 (now):** D1-D6 decisions [USER] · A1-A5, A6a · B1, B4 · E1, E6 · F1 · G5
  (pull-ahead candidates if sessions are free: E2, E4, E5, A6b, G3, G4)
- **Wave 2:** A-R gate · B2, B3, B5, B7 · C1-C4 · E2-E5 · F2, F4 · G1, G3 · H0, H1
- **Wave 3:** B6 · D-1..D-3 · F3, F5 · G2, G4, G7 · H2-H5
- **Wave 4 (gate):** B8 (last code, after all merges) · feature-inventory closeout
  (G2 gate) · fresh adversarial review (Codex, whole diff since 9a1e129) · benchmark
  refresh incl. p95/cold rows + amplification · G6 cutover.

## Launch gates (sign-off checklist)

- [ ] **G1 Correctness:** A1-A6 + A-R merged; E1 oracle green in CI; fresh adversarial
      review finds nothing High+.
- [ ] **G2 Everything works:** E6 inventory fully classified keep-tested / flagged / cut;
      zero unclassified surfaces.
- [ ] **G3 Money:** G1-G4 live; Stripe key rotated; alert tested end-to-end.
- [ ] **G4 Trust:** D-1 shipped (default on vs cloud), D-2 + D-3 published.
- [ ] **G5 Self-host+docs:** README commands pass verbatim on clean Ubuntu; F2/F3 done;
      docs on site + slim README.
- [ ] **G6 Release+cloud:** tag dry-run boring; five screens live; ToS + privacy +
      support channel published (H5); commons seeded and verified warm; cutover done.

## What's true when this plan is done

**For a user.** A developer on a clean machine runs the install one-liner and it works —
static binaries, no missing libraries, worker included. The README quick start runs
verbatim. A cloud user goes from landing page to first clone in under a minute: GitHub
sign-in, `ripclone login` once, and from then on the whole product is two verbs —
`ripclone add` (interactive: builds, streams progress, ends warm) and
`ripclone clone`. Free accounts clone public repos without limits; $3 per seat buys an
org private repos and agent tokens. Errors tell them the next command, not an env var.

**For correctness.** Every ripclone clone is byte-identical to `git clone` — proven in CI
on every PR, both modes, both writers, on a fixture full of edge cases. An editable clone
is cryptographically verified against the upstream host, so ripclone is structurally
incapable of tampering with code. A failed build fails visibly and self-heals; a stale
build can never clobber a newer one; no token holder can read another tenant's repos;
a crash can't brick the CLI's auth state.

**For the codebase.** ~3,000 fewer lines and zero dead code (compiler-enforced). One
extraction pipeline, one shared home for GC-critical and path-safety logic, the build
pipeline out of server.rs, providers as data + one webhook trait, two config sources,
uniform storage keys, ~12 documented env vars. Every user-visible surface is in exactly
one state: tested, flagged experimental, or cut with an issue.

**For the business.** Revenue is $3 per active seat per org (a personal org is one
seat). Every clone has an account behind it; every build has someone who asked for it.
The paid fence is only things that can't be free-ridden: private repos and agent
tokens. Costs are bounded: TTL GC ages out idle repos, entitled private repos never
expire, GitHub-gated signup blocks throwaway accounts, build guardrails cap what any
one add can cost, and the storage multiplier is a measured number. Per-account usage
stats with a fleet-scale alert mean heavy free usage gets repriced with data, not
guesses. A wedged worker or growing queue pages Russell within minutes. The DB restore
has been drilled, the Stripe key rotated, prod cut over cleanly.

**For trust and story.** The website carries the docs: install → clone → why it's fast,
with honest numbers (10× where true, 2–4× where true, p95 and cold-start published).
A security page states what's stored, what's verified, and what telemetry is sent (with
its opt-out). The five-screen app makes the value visible — who the seats are, what
warmth means, time saved.

**What's deliberately not true yet** — the section below: no hybrid top-up clone (202
still exists for cold repos), no Bitbucket, no device-code login, Postgres/MySQL
best-effort, files mode not upstream-verifiable. All parked as issues, none blocking.

## Explicitly post-launch (open issues now, don't build)

- Hybrid top-up clone (serve stale clonepack + client git-fetches the tiny delta —
  kills the 202 wait for incremental pushes; the biggest post-launch design win).
- Client-side pack synthesis (store content once; halves storage cost). Rejected for
  now because local pack/index writing measured slow — that's why the pack/archive
  separation exists (ARCHIVE_AB_RESULTS.md). Revisit only if storage cost bites, and
  re-run the A/B first.
- Cross-branch head-pack dedup (matters when all-branch warming is real).
- Unify ref/metadata/queue into one state store (kills the config-drift class).
- Device-code `ripclone login` (the `gh`-feel flow; simple login ships first).
- OSS hygiene batch: legacy /v1/packs //v1/objects endpoints, rate-limiter retain cost,
  auth-cache eviction, ?token= in query string (session JWT lands in browser history),
  token-store keyspace collision (provider vs login keys), stale tmp-dir sweep on clone
  start. (Predictable CSRF state moved into A5 — it ships at launch.)
- Range/resume + spill-to-disk artifact fetch: today a whole pack is fetched into RAM
  with no resume (adversarial S4) — a big full clone can OOM a small box, and a signed
  URL expiring mid-download restarts from zero. E4 pins current behavior; the real fix
  (ranged, resumable, disk-spilled fetch for large artifacts) is an issue now, built
  when big-repo full clones are a marketed path.
- Recurring benchmark job (goal-a guard): the wave-4 benchmark refresh is point-in-time;
  after launch, a scheduled run (weekly, fixed repos, fixed client) that compares clone +
  sync times against the last run and alerts on >20% regression. Cheap insurance that
  "as performant as possible" stays true after B6's work is done.
- Cross-process re-check cap + adaptive polling (existing deferred memory items).
- Bitbucket provider; Postgres/MySQL back to "supported" with a real support story.
- **Cloud GitLab.com support — the named #1 fast-follow** (huge OSS/GitLab-native
  crowd; the obvious next market after GitHub). Because H0 built the additive seam, this
  is: implement `GitProvider` for GitLab (group access token for the connection, GitLab
  members/permission API for the access check), add "Sign in with GitLab," add a
  connection screen, wire GitLab webhook registration (OSS parse adapter already
  exists). Revisit D2's GitHub-only-signup sybil defense. Target: ~1–2 weeks, not a
  rewrite. Ship when a paying GitLab.com team asks (or proactively soon after launch).
- **Cloud Gitea/Codeberg + other reachable public hosts** — same interface, lower
  priority (niche). NOTE: firewalled self-hosted hosts are NOT a cloud target ever —
  the cloud can't reach them; those users self-host ripclone (already supported at
  launch). "Any self-hosted provider" is delivered by the OSS self-host path, not cloud.

## Coverage map (review finding → node)

| Review finding | Node |
|---|---|
| Phase-2 clobber (B1) | A1 |
| Ungated content endpoints (B2) | A2 |
| Non-atomic token/config writes (B3) | A3 |
| worktree-add half-repo (B4) | E6 pre-flagged decision: fix or mark experimental |
| Stuck-202 (S1/S2) | A4 |
| Config-parse fallback, precedence, archive poll, swallowed errors (S3/S4/S7/S8) | A5 |
| Visibility-TTL fails open (AU3, unverified) | A5.5 |
| Predictable CSRF state | A5.6 |
| GC walker + path-validation duplication (S5) | B6.3 |
| CachingRefStore lock (S6) | B6.2 |
| Whole-pack in-RAM fetch, no resume (adversarial S4) | E4 pins behavior; post-launch issue |
| io_uring vs POSIX writer parity | E1 (oracle runs under both writers) |
| Recovery poller default-off | G1 |
| Storage amplification unknown | B4 |
| Agent-token usage data (future repricing) | G3 |
| Free-account sybil abuse | G2.1 (GitHub-only signup) |
| Release identity (repo name vs URLs) + stray .pem | F3 |
| Cloud Phase-0 leftovers (version UI, old path copy) | G5 |
| Empty-repo clone UX | E6 pre-flagged decision |
| Delete list / dead code / knobs | B1, B2, B3, B7 |
| server.rs god module | B8 |
| Provider config sprawl / token chains / addressing / Bitbucket | C1-C4 |
| Storage-key ambiguity | C1 |
| Equivalence oracle / GC race / provider e2e / expiry / flake | E1-E5 |
| Quick start / static builds / tarball worker / wheel / default dirs / drift warning | F1-F3 |
| git-remote-ripclone undocumented | F4 |
| README/docs restructure, stale claims, internal docs | F5, G5, H4 |
| Telemetry undocumented | D-3 |
| Trust story / security page | D-1, D-2 |
| unpaid entitlement / webhook lifecycle | A6 |
| Free-tier abuse / GC / warmth | G1, G2 |
| Observability / backups / rate limiting | G3, G4, G2.3 |
| Cloud doc drift ($2/$3, contradictions) | G5 |
| Frontend rebuild + onboarding + value-visible | H1-H4 |
| Anonymous-clone contradiction | D2 + H1 + G2 |
| Benchmark honesty (p95/cold, claims) | Wave-4 benchmark refresh + F5/H4 |
| fsync durability documentation | D6 + F5 |
| Sync latency as the real metric | B4, B5, B6 |
