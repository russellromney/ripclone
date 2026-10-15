# Contract test suite

Status: every row below has a passing test. This file was step 1 of
[PLAN.md](PLAN.md); see [Current state](#current-state).

## Goal

About 50 tests that state what Ripclone promises, run against the real
binaries, in parallel, in a few minutes. They replace the ~219 integration
tests. Unit tests stay for pure logic (pack parsing, manifest decode, control
store SQL, webhook signature checks, the client's pin rule).

A test belongs here only if a user or operator could observe the behavior. If
a test needs to reach inside the server to set up or observe its state, it is
a unit test or it is not needed.

## Rules

1. **Binaries only.** Each test starts its own `ripclone-server` and runs the
   `ripclone` CLI as child processes. Configuration goes through the child's
   env and config file. The test process never calls `set_var`.
2. **Observe public surfaces only:** CLI exit code and output, the checked-out
   tree compared with the origin, HTTP API responses, `/metrics` counters, and
   the storage directory or bucket listing.
3. **Faults come from outside the product.** No `TestStage`, no
   `RIPCLONE_TEST_*`, no probes. Tools:
   - **Gated origin.** A git smart-HTTP server in the test harness (Rust,
     wrapping `git http-backend`; replaces the Python origin). It can hold or
     fail `upload-pack` per caller credential, check auth headers, and move
     refs between steps.
   - **Fault proxy.** A reverse proxy in front of artifact URLs and, for S3,
     in front of the S3 emulator. Rules: cut after N bytes, 503 once, flip a byte, 404,
     drop Range support, expire a signed URL, fail PUTs.
   - **Process control.** SIGKILL and restart the server; restart it with
     different config.
   - **Real config.** `files_slots = 0` and `full_slots = 0` pause Files and Full builds.
     This is an operator setting (Head-only server), not a test seam, and it
     makes "Full is pending" deterministic.
4. **Parallel.** Each test gets its own tempdir, server process, and port (the
   server binds `:0` and prints the bound address). No shared statics, no
   `--test-threads=1`, no `lsm_on_`/`lsm_off_` passes.
5. **No 2 s floor.** One documented setting, `RIPCLONE_POLL_MS`, shortens client
   and server polling for every test.
6. **No proving a negative by sleeping.** Wait on an observable event (a
   counter, a status, an origin request log) with a timeout of 10 s or less.

## The tests

"Added" names the PR that added the row's test. Each row's test is named
after the row (`a1_...`, `s3_c2_...`).

### A. Resolve and admit (7)

| # | Test | Added |
|---|---|---|
| A1 | Clone a cold repo by branch: the tree equals origin at B and the CLI reports B. | #186 |
| A2 | Sync an unchanged branch twice: no new build (`builds_queued_total` flat), storage listing unchanged. | #186 |
| A3 | Push C, sync: exactly one new build; clone gets C. | #186 |
| A4 | Clone at a full SHA of an older commit: detached checkout at that SHA. | #186 |
| A5 | `--at HEAD~1` (a rev expression) is rejected with a clear error and no build. | #203 |
| A6 | Table: repo not added (clone and sync), unknown ref, missing upstream repo. Each gives its own error code and no build. | #186 |
| A7 | After `add`, the origin is deleted (`repo_not_found`, exit 5) or accepts and never answers (`RIPCLONE_LS_REMOTE_TIMEOUT_SECS=1`: `upstream_unavailable`, exit 6). Clone, sync and the API fail with no build; the built commit still clones by SHA. | #208 |

### B. Coalescing (3)

| # | Test | Added |
|---|---|---|
| B1 | With the origin holding the server's fetch: 5 concurrent clones plus a sync for B produce one build. All installs are B. (Webhooks are I4.) | #188 |
| B2 | Branch name, full SHA, and default branch for the same B: one build; each clone keeps its own checkout name. | #188 |
| B3 | Origin moves B to C while B builds: B and C build independently; a clone pinned to B gets B's tree. | #188 |

### C. Outputs are independent and immutable (6)

| # | Test | Added |
|---|---|---|
| C1 | `files_slots = 0` and `full_slots = 0`: depth-1 clone of B succeeds while Full and Files stay pending (API status shows it). | #203 |
| C2 | Head ready, then restart with the storage proxy failing PUTs and background builds on: Full and Files fail, Head stays cloneable, status shows `failed` per output. | #203 (S3 job) |
| C3 | Restart without the fault and retry: only Full and Files rebuild (Head build counter flat). | #203 (S3 job) |
| C4 | SIGKILL the server while the origin holds its fetch; restart; release: the build completes once, every output ready. | #202 |
| C5 | After B is fully ready, a later build of C never changes B's published manifests (byte-compare B's API responses before and after). Over the three builds, every server log line is the server's own: no git output. | #202 |
| C7 | S3 only: the bucket holds Full(B)'s first write while push C arrives. Head(C) and Files(C) become ready while Full(B) is still pending; after release, Full(B) publishes before Full(C). | Hetzner benchmark |

A SIGKILLed server's `git` children run until their command ends, on every
OS. #209 killed them on Linux with a parent-death signal (row C6), but
setting it needs a `pre_exec` hook, and any `pre_exec` makes std `fork()` the
whole server for every git spawn. C6 is retired. The unit test
`git::tests::server_git_spawns_do_not_fork` keeps server spawns on
`posix_spawn`.

### D. Clone pins B (6)

| # | Test | Added |
|---|---|---|
| D1 | Branch moves B to C during the clone: the clone installs B. | #188 |
| D2 | Proxy rewrites a later poll to name a different commit: integrity error, no target directory. | #188 |
| D3 | Pinned commit removed upstream before it is built: clone fails and does not fall back to the branch. | #188 |
| D4 | `--wait` with Full(B) pending and `files_slots = 0`, `full_slots = 0`: the CLI waits, reports pending, and exits at its timeout without installing anything else. | #205 |
| D5 | 401, 403, 404, 5xx, and an unreachable server give distinct CLI errors and exit codes (3, 4, 5, 6, 6; see the README). | #189 |
| D6 | Client-side failures: target exists and is not empty (exit 1, its files untouched), server unreachable (exit 6), no `git` on PATH for an editable clone (exit 1, before any request). Nothing left behind; a Files clone works without git. An existing empty directory is a valid target, as for `git clone`: the clone lands in it and keeps its mode; an unreachable server, no git, or artifacts failing mid-clone leave it in place, empty, with its mode. | #208 |

### E. Top-up (4)

| # | Test | Added |
|---|---|---|
| E1 | Full(A) ready; restart with `files_slots = 0` and `full_slots = 0`; push B; full clone: installs B by top-up without waiting on any server build. Tree and `rev-list --count` equal origin at B, `git fsck` clean. | #205 |
| E2 | Same with a push of several commits including a merge (A is an ancestor, not the parent). | #205 |
| E3 | Same after a force-push (A is not an ancestor): clone still equals origin at B. | #205 |
| E4 | No ready Full exists for the ref: full clone waits for Full(B) instead of failing. | #205 |

Top-up uses the user's git credentials. E1-E3 run against an auth-checking
origin to prove the clone works with only git's own credential and that no
Ripclone token reaches the origin.

### F. Downloads (9)

| # | Test | Added |
|---|---|---|
| F1 | One flipped byte: hash failure, no target directory, no cache object. | #188 |
| F2 | Truncated body (wrong length): failure, no URL refresh. | #188 |
| F3 | 404 artifact: immediate failure. | #188 |
| F4 | One 503: retried, clone succeeds. | #188 |
| F5 | Connection cut after N bytes: resumes with Range from the saved offset (proxy counts Range requests). | #188 |
| F6 | Proxy without Range support: restarts only that artifact. | #188 |
| F7 | S3 emulator: signed URL expires mid-download: the CLI refreshes URLs for the same B and completes. | #191 (S3 job) |
| F8 | SIGINT during the download: exit 130, no target, staging directory, or marker left; the next clone succeeds. | #188 |
| F9 | An artifact that answers 503 every time: exactly 3 attempts, then exit 6; no target, no staging. | #208 |

### G. Clone shapes produce correct trees (10)

| # | Test | Added |
|---|---|---|
| G1 | Depth 1 editable: shallow, `git status` clean, files equal origin. | #186 |
| G2 | Full editable: `rev-list --count` equals origin, `git fsck` clean. | #186 |
| G3 | Files mode: tree equals `git archive` of B, including symlinks and exec bits. | #186 |
| G4 | Push, rewind force-push, and non-ancestor force-push in sequence: each new full clone equals the new tip. | #186 |
| G5 | Enough pushes to seal and compact history levels: full clone stays complete. | #186 |
| G6 | Small push after a 12 MiB tree: B's Files clone reuses chunks A's Files clone downloaded, the store grows by under half the tree, and B's tree is exact. | #208 |
| G7 | Delete a file, then re-add the same bytes (same blob id): depth-1, full and Files clones of the tip all have it. | #208 |
| G8 | Empty trees: a root commit with no files, and a later commit that deletes every file. Every shape installs an empty tree. | #208 |
| G9 | Two branches admitted while the origin holds the fetch, then built together: each branch's clones equal its own tip in every shape. | #208 |
| G10 | Paths with spaces and non-ASCII characters, a symlink to one, an executable; a branch named `feature/new-ui`; an annotated tag via `--at`. Every shape equals its commit; the tag is detached, the branch checked out by name. | #208 |

### H. Storage and server state (9)

| # | Test | Added |
|---|---|---|
| H1 | Local disk: delete the client cache and restart the server: published artifacts still clone. | #186 |
| H2 | S3 emulator: same lifecycle; artifacts are in the bucket. | #191 (S3 job) |
| H3 | A second server on the same control database is rejected before it binds. | #186 |
| H4 | Table: invalid storage, control, rate-limit and worker config (file or env, malformed file, removed `[queue]` section) is rejected before anything is created in the state dir. So is every numeric server variable that is set and does not parse (poll and retention intervals, retention age and size, signed-URL TTL, ls-remote timeout, LSM levels, Head rebase size, S3 timeout, attempts and retry delay), with an error naming it. | #208 |
| H5 | Local disk with retention every second and a 1-day limit: the server reports retention off; month-old artifacts stay and clone. | #208 |
| H6 | S3 emulator, same settings: retention trims month-old local cache objects, the bucket is unchanged, published artifacts clone without a rebuild, and the next push builds. | #208 (S3 job) |
| H7 | A repository's config (`/v1/admin/config`) survives a server restart. | #208 |
| H8 | Table: the control database file is deleted, or replaced by another file, under a running server: `/readyz` gives 503 and the log names the case. | #209 |
| H9 | Build scratch goes under `<cache>/tmp`, which startup empties: with a read-only `TMPDIR`, a stale scratch entry is removed and a full build and clone succeed. | #211 |

### I. Auth, integrations, CLI (12)

| # | Test | Added |
|---|---|---|
| I1 | Missing or wrong server token: 401; a valid token works. | #186 |
| I2 | Private repo with a token on the server's provider instance: the server builds it; the CLI holds no upstream token. | #202 |
| I3 | Table across GitHub, GitLab, Gitea, generic: the provider's auth header reaches the origin. | #191 |
| I4 | Table across GitHub, GitLab, Gitea webhooks: a signed push builds before any clone; a bad signature gets 401 and no build. | #191 |
| I5 | Poller: with no webhook, a push is built within one poll interval. | #186 |
| I6 | Every documented CLI verb exists (keep `docs_cli_surface`). | #193 |
| I7 | `--verify-upstream` auto, always, never (table). A clone that fails verification is removed; a target that was an empty directory is left empty. | #191 |
| I8 | Only `/healthz`, `/readyz`, `/metrics` and `/v1/version` answer without the token; unsigned webhooks fail closed (503 without a secret); every other route, `/v1/artifacts/{hash}` included, gives 401 to a missing or wrong token. | #208 |
| I9 | A client declaring another protocol gets 426; the CLI names the mismatch and says to run `ripclone version`; nothing builds. | #208 |
| I10 | `--verify-upstream` in Files mode: `never` and `auto` clone without contacting the upstream; `always` fails before any download. | #208 |
| I11 | Config discovery: CLI and server read `~/.config/ripclone/config.toml` and `RIPCLONE_CONFIG`; a flag beats env, env beats the file (CLI server URL, server control path). | #208 |
| I12 | Removed environment variables (queue, test, JWT families) set for the server and the CLI: each warns once and still runs. | #209 |

### J. Builds start from the newest ready ancestor (5)

Each output of C builds on the same output of its base: the newest built
commit that is an ancestor of C. Tests read the published Head and Full
manifests: "built on A" means Head(C) keeps every Head pack of A and adds
under 64 KiB, and Full(C) keeps A's history packs and adds one small level.
"Cold" means C shares no Head or history pack with the other commit. Every
case clones C in depth-1, full and Files shapes, compares each with the
origin, and runs `git fsck` and `rev-list --count` on the editable clones.

| # | Test | Added |
|---|---|---|
| J1 | Origin moves A to C through several commits; only C is admitted (the commits between have no build). C is built on A, and the store grows by under 1 MiB though history holds 2 MiB of noise. | this PR |
| J2 | Same when C is a merge of a side branch. | this PR |
| J3 | A force push to a sibling X of the built A builds cold, and X's full clone lacks A. A force push of two commits on A then builds on A, not on the newer X, and lacks X. | this PR |
| J4 | The mirror directory is deleted between restarts: a push of two commits on A fetches A again and builds on it. After the origin force-pushes unrelated history and prunes A and C, the mirror is deleted again and the next build is cold. | this PR |
| J5 | A branch with no history in common with the built main builds cold, and its full clone lacks main's commit. | this PR |

Total: 71 tests. Several are table-driven.

## What it replaced

The suite replaced every older integration test (deleted in #193): the
`local_*` suites, `suites/contracts`, `control_startup`, the `e2e_*` suites
and `tests/common`. Pure-logic tests moved next to their code as unit tests.
#194 then removed the production test hooks (`TestStage`, `test_hook`,
`AdmissionTestProbe`, every `RIPCLONE_TEST_*` read).

## CI shape

- `test`: unit tests plus the contract suite, parallel. Target under 5 minutes
  after compile. The one S3 unit test (multipart upload) runs here against
  `scripts/s3_emulator.sh`, reusing the unit-test binary.
- `s3`: C2, C3, C7, F7, H2 and H6 against the S3 emulator (`scripts/s3_emulator.sh`, versitygw).
- `gitea`: the live Gitea provider row.
- `lint`, `musl`, `docker`, `cargo-deny`.
- `e2e`: `scripts/e2e_local.sh` against prebuilt binaries.
- `benchmark`: a smoke run of `benchmark/shaped_benchmark.sh` when benchmark
  files change.

## Current state

- The suite lives in `rust/tests/contract/` and runs with `cargo test --test
  contract`, in parallel. The tools are in `net.rs` (gated origin, fault
  proxy, silent endpoint) and `harness.rs`; wire details are in one module,
  `harness::api`.
- The six `s3_` rows start their own versitygw and are `#[ignore]`d; the
  `s3` CI job runs them with `--ignored s3_`. The live Gitea row runs in the
  `gitea` job with `--ignored gitea_`.
- The CLI polls at most 200 ms apart in every test, so `--wait` lasts about
  8 s, inside the 10 s wait. D4 sets 50 ms to run out its budget.
- No nightly stress or benchmark job exists.
