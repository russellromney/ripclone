# Ripclone rebuild: keep, cut, redo

Status: steps 1–5 are done; step 6 is in progress. See [Order of
work](#order-of-work). The diagnosis below describes origin/main `85cf0e0`,
before the rebuild.

## Diagnosis

The product contract and the artifact design are sound. The control plane
around them is not. Each slice added a mechanism without removing the one it
replaced, so several half-models now run at once:

- **Work and result live in two places.** `results` holds an untyped JSON blob
  with no status. `jobs` holds status, attempts, size class, a credential, and a
  config snapshot. `workers` holds a second liveness clock that nothing reads.
  "Is commit B ready, pending, or failed?" is answered in four places by
  combining them (pinned GET, repo status, job ack, `exact_result_*`).
- **Reads write.** `GET /refs/{branch}` may run `ls-remote`, run a mirror `git
  fetch` under the repo lock, enqueue a build, or read a manifest from storage
  to build a top-up answer. Only the pinned GET is a pure read.
- **Commit resolution has three mechanisms** (ls-remote, trusted SHA, in-request
  mirror fetch), with different locks, caps, and error codes.
- **Sequencing escapes its caps.** A build slot frees after Head, so Full and
  Files work has no limit. Retry backoff sleeps while holding the claim. Crash
  recovery waits 30 minutes. Head is republished after Full compaction, so
  published results are not immutable.
- **Top-up is narrower than its goal.** The goal is right: install the last
  ready Full and fetch the newest commits from upstream, instead of waiting for
  a server build plus orchestration. But today it only works after the server
  has built Head(B), so the client still waits on the server; it fails at once
  before that. It also requires B to be a single-parent child of the base, and
  a local provider config for every clone.
- **Tests pin the implementation, not the contract.** At least 95 of 219
  integration tests drive internal seams; production carries ~750 lines of test
  hooks and 27 `RIPCLONE_TEST_*` variables; everything runs single-threaded
  because tests mutate process env. CI takes 17-22 minutes; 24 of the last 100
  runs failed, the most common cause being one claim-loss test that needed a new
  hook to stabilize.

## Target model (the spirit, written down)

1. **Only the server builds; a client may top up.** Clients download artifacts
   from object storage. When the Full they asked for is not ready, the server
   offers the newest ready Full for the same repo and ref. The client installs
   it and runs one `git fetch` of the target commit from upstream, using the
   upstream URL the server returns and the user's normal git credentials. The
   client never waits on server build orchestration for history it can fetch
   itself, and needs no provider config.
2. **Resolve once, in one function.** `resolve(repo, selector) -> (commit,
   checkout_name)` using `ls-remote`, or a trusted SHA (full SHA or signed
   webhook). No request-path mirror fetch.
3. **Writes and reads are separate calls.**
   - `POST /v1/repos/{repo}/builds {selector}` resolves, admits, and returns
     `{commit, checkout_name, state}`.
   - `GET /v1/repos/{repo}/commits/{sha}?kind=head|files|full` is a pure read:
     `ready` (manifest plus signed URLs), `pending`, or `failed`. The client
     uses the same call to poll and to refresh URLs. A `pending` Full also
     carries `top_up: {base_commit, base}` when a ready Full
     exists for that ref (one indexed read, no storage read, no Head(B) needed).
   - Webhook, poller, and `sync` all call the same `admit(repo, commit)`.
4. **One row per (repo, commit) is both the job and the result.**
   ```
   builds(repo, commit, seq, parent, ref_name, created_at,   -- ref_name: the ref that admitted it, for top-up
          head_state, head_manifest, files_state, files_manifest,
          full_state, full_manifest,          -- state: pending | ready | failed
          attempts, error, lease_owner, lease_until, lease_kind, retry_after)
   ```
   - admit: `INSERT OR IGNORE` with `seq`, a strictly increasing admission
     number; an explicit retry resets failed outputs to pending.
   - claim: one `UPDATE ... RETURNING` on the lowest-`seq` row with pending
     outputs whose lease is empty or expired and whose `retry_after` has
     passed, skipping repos that already hold a live lease of that kind.
   - renew: extend `lease_until` (short lease, for example 60 s). An expired
     lease is lost: renew, publish and fail refuse it, and the worker cancels
     the build.
   - publish(kind): set that output's state and manifest `WHERE lease_owner = me`
     and the lease is live. A ready output is never overwritten.
   - fail(kind): mark it failed, increment attempts, set `retry_after`. No sleep
     while holding the lease.
   - status: read the row. One place.
   No jobs table, no workers table, no size classes, no key strings, no stored
   credentials.
5. **Bounded sequencing.** Each commit's build runs Head, then Files, then
   Full. Three process-wide caps: Head, Files and Full slots, one pool each, so
   a long Full never holds back another commit's Files. Each pool claims in
   admission order, and a repo holds at most one lease per pool.
6. **Published outputs are immutable.** Head compaction, if kept, applies to the
   next commit's Head, never by republishing this one.
7. **Credentials come from the server's broker at build time** (GitHub App,
   configured token). They are never copied into the database or sent to
   workers in job payloads.
8. **One mirror shape, one fetch function, mirror writes under the mirror
   lock** (fetch, commit-graph, compaction). Builds read the mirror under a
   shared lock and fetch only a commit it lacks; compaction deletes packs, so
   it runs only while no build reads. No reachability bitmap is written: a
   cold Full packs history with a plain `pack-objects --revs` walk. The
   mirror's `attr.tree` names the empty tree, so that walk does not resolve
   the mirror's missing `HEAD` once per object.

## Keep (reuse with little change)

| Area | Why |
|---|---|
| Clonepack format, Head/Files/Full builders, LSM history, incremental archive (`pack.rs`, `archive.rs`, history code) | The product's value. Format is content-addressed and not tied to writer internals. |
| CAS, local and S3 storage, signed URLs, local cache retention | Clean boundary, five small backend branches. |
| Client staging plus rename publish, hash verification of every artifact | Correct and simple. |
| Extract and worktree writer (POSIX path) | Core install path. The io_uring writer was deleted (decision 7). |
| Provider registry | Data only; all providers share one git transport. |
| Webhook verification (GitHub, GitLab, Gitea) and the poller | Isolated; both become callers of `admit`. |
| GitHub App credential broker | Isolated behind a trait; the server-side credential source. |
| Parent reuse rule (use the parent's output only when it is ready) | Gives incremental builds. |

## Cut

| Item | Size | Reason |
|---|---|---|
| Smart-HTTP git fallback `/v1/git/...` | ~310 | Second clone path, own auth form, request-path mirror fetch. |
| OIDC `/v1/build` | ~320 | Undocumented, GitHub-only, ignores its commit, duplicates `sync`. |
| `RIPCLONE_TRUST_GATEWAY` | ~30 plus 7 branches | One authz path. |
| Workers registry, live counts, `pending_by_class` | ~150 | No production reader. |
| Size classes and `--max-size-class` | ~190 plus claim filter | A scheduler the design rules out. |
| Second enqueue (`SqlJobQueue::enqueue`), unclaimed publish paths, trait fallback to unclaimed publish | ~400 | Replaced by the single row model. |
| Retry sleep inside ack | small | Use `retry_after`. |
| `RIPCLONE_LSM` and `RIPCLONE_ARCHIVE_BOUNDED` toggles | keeps 2 paths | Keep one path each. |
| zstd dictionary and `train-dictionary` | ~100 plus plumbing | Server always passes `None`. |
| `--temp` overlay | ~190 plus client threading | Needs mount privileges; niche. |
| Non-refresh client fetch API, `fetch_chunk_refs`, ~525 lines of zero-reference functions | ~800 | Test-only or unused. |
| `ripclone-proxy`, `writer_bench` as shipped binaries | 633 | Move under `benchmark/` or tests. `writer_bench` was deleted with the io_uring writer. |
| Test hooks in production code (`TestStage`, `RIPCLONE_TEST_*`) | ~750 plus ~76 call sites | Replaced by external fault injection in contract tests. |
| Head republish after Full compaction | ~80 | Breaks immutability. |
| Dead CI pieces (`ci.sh databases`, never-run Turso test, unused staged binaries) | small | Dead. |
| Release image libgit2 and fuse packages | n/a | Nothing uses them. |

## Redo

| Area | Today | Target |
|---|---|---|
| Control state | `results` JSON + `jobs` + `workers` + `repository_configs`; DDL in 3 places; `meta/` and `queue/` adapter layers | One `builds` table, `added_repos`, `repo_config`. One module with admit, claim, renew, publish, fail, status. |
| Ref API | `GET /refs` that resolves, fetches, admits, and reports; two 202 shapes for sync | `POST builds` plus pure `GET commits/{sha}`. One response shape per state. |
| Commit resolution | 3 mechanisms in 6 entry points | One `resolve` function. |
| Embedded worker loop and sequencing | Foreground release, unbounded background work, PID worker IDs, 30-minute recovery | Three capped pools (Head, Files, Full), short leases, claim order by admission. |
| Remote worker protocol (if kept) | ~2,200 lines, 6 endpoints, credential shipping, local ref cache | 4 endpoints mapping 1:1 to claim, renew, publish, fail. Credentials from the broker. |
| Client resolver | Polling loop with pin, top-up branch, mixed 503 handling | POST once, poll the pure GET, same GET for URL refresh. |
| Top-up | Needs Head(B) built first (reads its parent from storage), B must be a single-parent child of the base, local provider config on every clone, test env reads in production | Base is the newest ready Full for the ref, known as soon as B is admitted. Client installs it, runs one `git fetch <upstream> B` with the user's git credentials, then checks out B. The upstream URL comes from the client's own provider config, never from the server, so a server cannot point the user's git at another host. `git fetch` guarantees B is complete; if the base is not an ancestor (force-push), the fetch just sends more. Full(B) still builds in the background for later clones. |
| Client download layer | Two refresh-aware loops plus a test-only third; duplicated cache lookup | One `download(artifact, sink)` with Range resume and URL refresh; sinks: memory or temp file. |
| Client install function | One 530-line function steered by 4 flags | `resolve -> plan {Files, Head, Full} -> materialize -> publish`. |
| `server.rs` (11k lines) | Routes, auth, workers, builder, webhooks, test hooks in one file | `api`, `builder`, `worker`, `auth`, `webhooks` modules. |
| Control database library | libsql pinned to a git revision of a prerelease, which blocks `cargo publish` | `sqlx` SQLite behind the `ControlStore` trait. |
| Test suite | 219 integration tests, ~45% through internal seams, serial, 17-22 minute CI | ~50 contract tests against real binaries, run in parallel. Faults injected outside the product: a gated upstream git server and a fault-injecting artifact proxy. Unit tests stay for pure logic (pack parsing, manifest, control SQL). |
| Config and docs | 74 env vars, 18 undocumented, README says git is pure Rust | Config reduced with the cuts; `CONFIG.md` generated or checked against the code; README matches the runtime. |

## Decisions (settled 2026-10-13)

1. **Top-up.** Decided: keep and redo as above. It is the default for a full
   clone when Full(B) is pending; `--wait` forces the exact artifact. Depth-1
   clones do not top up in v1.
2. **Remote API worker.** Decided: cut now. Re-add a 4-endpoint version
   (claim, renew, publish, fail) when a second build machine is needed.
3. **Turso.** Decided: cut. The control store is SQLite behind a small
   `ControlStore` trait (see "Control store" below), so Postgres can be added
   later without touching callers.
4. **Per-request upstream tokens.** Decided: server-side credentials only
   (GitHub App or a token configured at `repo add`). Top-up is unaffected: the
   client uses the user's own git credentials.
5. **`--at` selectors.** Decided: full SHA or ref name only.
6. **Login session.** Decided: one token scheme for v1; cut `/login`, the
   session JWT, and `auth login`.
7. **io_uring writer.** Decided: one Linux-kernel A/B run; delete it if the gain
   is within noise. Done: deleted; A/B within noise, 2026-10-14.
8. **Slice 12 PRs.** Decided: merge #171, carry #174's blocker fixes
   (`0cee9f5` staged clone marker and memory permit, `6223af5` + `b88cc25`
   strict storage preflight) as a small PR, close #170, #172, #173, #174.

## Control store

One trait, one SQLite implementation now, room for Postgres later.

```rust
trait ControlStore: Send + Sync {
    async fn admit(&self, repo: &RepoId, commit: &Oid, parent: Option<&Oid>) -> Result<BuildRow>;
    async fn retry(&self, repo: &RepoId, commit: &Oid) -> Result<BuildRow>;
    async fn claim(&self, owner: &str, lease: Duration, pool: Pool) -> Result<Option<Claim>>;
    async fn renew(&self, claim: &Claim, lease: Duration) -> Result<bool>;
    async fn publish(&self, claim: &Claim, kind: Kind, manifest: &Manifest) -> Result<bool>;
    async fn fail(&self, claim: &Claim, kind: Kind, error: &str, retry_after: Duration) -> Result<()>;
    async fn status(&self, repo: &RepoId, commit: &Oid) -> Result<Option<BuildRow>>;
    async fn latest_ready_full(&self, repo: &RepoId, ref_name: &str) -> Result<Option<BuildRow>>;
    // repos: add, remove, list, get_config, set_config
}
```

Rules that keep a second backend cheap:

- The trait speaks domain types, never SQL rows or connections. Callers never
  see a transaction.
- Each method is one statement or one short transaction. No locks held across
  `await` points outside the store.
- Portable SQL only: `INSERT ... ON CONFLICT DO NOTHING`, `UPDATE ...
  RETURNING`, integer epoch seconds, manifests as text. No `INSERT OR IGNORE`,
  no SQLite JSON functions, no SQLite-only date functions.
- `claim` is the only method whose SQL differs: SQLite relies on its single
  writer; Postgres adds `FOR UPDATE SKIP LOCKED` in the subquery.
- Schema lives in one file per backend with a `schema_version` row. No
  migration framework until there are users.
- One conformance test suite runs against any `ControlStore`. Adding Postgres
  means passing that suite, nothing else.
- Library: `sqlx` with runtime queries (no compile-time macros, so no database
  needed to build). It supports SQLite and Postgres from crates.io.

Not built now: the Postgres implementation, connection pooling beyond what the
library gives, and replication.

## Docs

Rules: one doc per concept; docs describe what the code does now; there is one
plan (this file) and no other roadmap; a PR that changes behavior updates its
doc; `CONFIG.md` is checked against the env vars the code reads. Git history is
the archive, so superseded docs are deleted, not moved.

| Doc | Action |
|---|---|
| `docs/superpowers/`, both roadmaps, dated reports (`ADVERSARIAL_REVIEW`, `ARCHIVE_AB_RESULTS`, `WRITER_SCHEDULER_EXPERIMENT`, `WRITER_PERF_ANALYSIS`, `giant-repo-gate`) | Deleted (PR #182). |
| `AGENTS.md`, `AUTH.md`, `SYNC.md`, `FRESHNESS.md`, `SYNC_LATENCY_PROFILE.md`, `SCALING_WORKERS.md`, `BUILD_OPTIONS.md`, `TROUBLESHOOTING.md`, `CHANGELOG.md`, `internal/GITHUB_INTEGRATION.md`, `internal/RELEASE_DRYRUN.md` | Deleted (PR #182). They described cut features, the admission machinery being replaced, nonexistent install paths, or pre-release history. Agent guidance and build requirements moved to the README; GitHub App settings moved to `CONFIG.md`. |
| `README.md`, `CONFIG.md`, `BACKENDS.md`, `PROVIDERS.md`, `WEBHOOKS.md` | Rewritten in PR #182 to the surface that stays. They omit features being cut. |
| `DESIGN.md` | Kept. Becomes the target model as the control plane lands (step 3), including top-up. |
| `ARTIFACT_LIFECYCLE.md`, `BENCHMARKS.md`, `benchmark/README.md`, `SECURITY.md`, `rust/README.md` | Kept. Benchmarks rerun at step 6. `docs/SUPPORT.md` (added by #171) folded into the README build section. |
| `docs/internal/rebuild/map-*.md` | Deleted at step 5. |

`FRESHNESS.md` is the user doc for how a push becomes clonable, top-up, and
timings.

## Order of work

Each step is a separate PR with green CI. No step mixes a cut with a redo.

1. **Contract tests first** ([CONTRACT_TESTS.md](CONTRACT_TESTS.md)). Write the ~50 contract tests against the CLI and
   HTTP behavior that must survive, with external fault injection. They should
   pass on today's main where the contract is unchanged. Include top-up: a full
   clone right after a push must not wait for any server build step.
   **Done:** #186, #188 (suite and faults), #189 (CLI exit codes and Ctrl-C),
   #190 (port races), #191 (integrations), #192 (poll backoff), #193 (legacy
   suites deleted).
2. **Pure cuts.** Delete the Cut list, one area per PR, deleting the tests that
   only covered the removed code.
   **Done:** #194 (test hooks), #195 (remote worker, Turso, size classes,
   workers registry), #196 (one auth path), #197 (build toggles), #198 (dead
   code), #199 (S3 emulator), #200 (unit tests), #201 (rustls).
3. **Control plane.** Introduce the `builds` row model, the split API, one
   resolve function, leases, and capped sequencing. Delete `jobs`, `workers`,
   `meta/`, `queue/` adapters, and the hook suites that pinned them. No data
   migration (there are no users). The Head republish after Full compaction
   is replaced here by an immutable scheme: Full(A) records a compacted Head
   base that the next Head(B) builds its delta against, because cutting the
   republish alone lets the Head delta grow without bound.
   **Done:** #202 (`ControlStore` and `builds`), #203 (`POST builds`, `GET
   commits`), #207 (bitmap writes off the request lock).
4. **Client.** Resolver, download layer, install plan.
   **Done:** #204 (resolve, plan, materialize, publish), #205 (top-up with
   the user's git, `clone --wait`, `FRESHNESS.md`).
5. **Module split and docs.** Split `server.rs`; make README, `CONFIG.md`, and
   `ARTIFACT_LIFECYCLE.md` match the code.
   **Done:** #206 (`server.rs` split), #208 (contract gaps), #209 (follow-ups),
   and the `docs/rebuild` branch (docs pass, unread `RepoConfig` keys removed).
6. **Release work.** Benchmarks rerun on the final binaries, then the OSS gate.
   **Benchmarks done:** Hetzner runs on 2026-10-14 against `ddae2ab`. Clone
   times matched. The runs found three server regressions, fixed in #211
   (scratch under the cache), #212 (`posix_spawn`) and #213 (separate Files
   and Full pools). `BENCHMARKS.md` has the results. **Next:** the OSS gate.

Rough effect: `server.rs` from ~11k lines to ~4-5k across modules; production
Rust from ~47k to somewhere near 35-38k; integration tests from 219 to ~50.
These were estimates, not measurements.
