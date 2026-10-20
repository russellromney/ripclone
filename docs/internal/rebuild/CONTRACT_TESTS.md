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
   - **Test settings.** `RIPCLONE_FILES_SLOTS=0` and `RIPCLONE_FULL_SLOTS=0`
     pause Files and Full builds, which makes "Full is pending"
     deterministic. See [Test settings](#test-settings).
4. **Parallel.** Each test gets its own tempdir, server process, and port (the
   server binds `:0` and prints the bound address). No shared statics, no
   `--test-threads=1`, no `lsm_on_`/`lsm_off_` passes.
   Every temp dir a test process makes is under `$TMPDIR/ripclone-test-<pid>`
   (`rust/src/test_tmp.rs`). The next test run removes it if the process was
   killed mid-run. A World dir that cannot be removed fails its test, and
   `scripts/ci.sh test` fails if the tests leave anything in their `TMPDIR`.
5. **No 2 s floor.** `RIPCLONE_POLL_MS` shortens client polling for every
   test.
6. **No proving a negative by sleeping.** Wait on an observable event (a
   counter, a status, an origin request log) with a timeout of 10 s or less.

## The tests

"Added" names the PR that added the row's test. Each row's test is named
after the row (`a1_...`, `s3_c2_...`).

### A. Resolve and admit (8)

| # | Test | Added |
|---|---|---|
| A1 | Clone a cold repo by branch: the tree equals origin at B and the CLI reports B. | #186 |
| A2 | Sync an unchanged branch twice: no new build (`builds_queued_total` flat), storage listing unchanged. | #186 |
| A3 | Push C, sync: exactly one new build; clone gets C. | #186 |
| A4 | Clone at a full SHA of an older commit: detached checkout at that SHA. | #186 |
| A5 | `--at HEAD~1` (a rev expression) is rejected with a clear error and no build. | #203 |
| A6 | Table: repo not added (clone and sync), unknown ref, missing upstream repo. Each gives its own error code and no build. | #186 |
| A7 | After `add`, the origin is deleted (`repo_not_found`, exit 5) or accepts and never answers (`RIPCLONE_LS_REMOTE_TIMEOUT_SECS=1`: `upstream_unavailable`, exit 6). Clone, sync and the API fail with no build; the built commit still clones by SHA. | #208 |
| A8 | An upstream with no commits: `add` fails with `ref_not_found` (exit 5), `sync` and `clone` say the repo isn't added, and nothing builds or is installed. After the first push, `add` builds it and a clone is exact. | #234 |

### B. Coalescing (3)

| # | Test | Added |
|---|---|---|
| B1 | With the origin holding the server's fetch: 5 concurrent clones plus a sync for B produce one build. All installs are B. (Webhooks are I4.) | #188 |
| B2 | Branch name, full SHA, and default branch for the same B: one build; each clone keeps its own checkout name. | #188 |
| B3 | Origin moves B to C while B builds: B and C build independently; a clone pinned to B gets B's tree. | #188 |

### C. Outputs are independent and immutable (10)

| # | Test | Added |
|---|---|---|
| C1 | `files_slots = 0` and `full_slots = 0`: depth-1 clone of B succeeds while Full and Files stay pending (API status shows it). A files clone also succeeds without waiting: B's tree, exec bit and symlink included, from Head(B), with no `.git`. With `--wait` the files clone reports Files(B) pending and installs nothing. | #203, #221 |
| C2 | Head ready, then restart with the storage proxy failing PUTs and background builds on: Full and Files fail, Head stays cloneable, status shows `failed` per output. | #203 (S3 job) |
| C3 | Restart without the fault and retry: only Full and Files rebuild (Head build counter flat). | #203 (S3 job) |
| C4 | SIGKILL the server while the origin holds its fetch; restart; release: the build completes once, every output ready. Then SIGKILL as soon as a push's first artifact is in the store: every stored object hashes to its name, and after a restart the push builds and its full and Files clones are exact. | #202, #220 |
| C5 | After B is fully ready, a later build of C never changes B's published manifests (byte-compare B's API responses before and after). Over the three builds, every server log line is the server's own: no git output. | #202 |
| C7 | S3 only: the bucket holds Full(B)'s first write while push C arrives. Head(C) and Files(C) become ready while Full(B) is still pending; after release, Full(B) publishes before Full(C). | Hetzner benchmark |
| C8 | S3 only: the bucket holds one of Head's writes and refuses the next. Head fails with the storage error and publishes nothing; a depth-1 clone fails and leaves no target; the write that landed is deleted, since no output names it. With the fault gone, `sync --at` builds every output and each shape clones exactly. | #220 (S3 job) |
| C9 | Every output of A is ready; the origin holds B's fetch, so nothing of B is. A files clone and a depth-1 clone of B each report B pending, download nothing, and install nothing: neither falls back to A. After release both install B. | #221 |
| C10 | The mirror holds B's objects but no refs, as a fetch stopped between storing its pack and writing refs leaves it. After a restart, B builds with no failures, and its full and Files clones are exact. | #234 |
| C11 | The mirror holds B's commit but not its tree, as a fetch that failed after writing part of B leaves it. B is fetched again, builds with no failures, and its full and Files clones are exact. | #234 |

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

### E. Top-up (5)

| # | Test | Added |
|---|---|---|
| E1 | Full(A), with a tagged parent P, ready; restart with `files_slots = 0` and `full_slots = 0`; push B; full clone: installs B by top-up without waiting on any server build. Tree and `rev-list --count` equal origin at B, `git fsck` clean. The reflog starts with `clone: from <url>` at B and there is no ORIG_HEAD. Its tags, remote-tracking branches and `git describe` equal a `git clone`, and the user's git fetched once, for the top-up: the other refs are the server's. | #205 |
| E2 | Same with a push of several commits including a merge (A is an ancestor, not the parent). | #205 |
| E3 | Same after a force-push onto P (A is not an ancestor): clone still equals origin at B, and has P's and A's tags, as git's does. | #205 |
| E4 | No ready Full exists for the ref: full clone waits for Full(B) instead of failing. | #205 |
| E5 | Head(B) is built while Full(B) has no slot; a tag pushed with B is in the top-up clone of B, although the top-up fetch takes no tags. | |

Top-up uses the user's git credentials. E1-E3 run against an auth-checking
origin to prove the clone works with only git's own credential and that no
Ripclone token reaches the origin.

### F. Downloads (12)

| # | Test | Added |
|---|---|---|
| F1 | One flipped byte: hash failure, no target directory, no cache object. | #188 |
| F2 | Truncated body (wrong length): failure, no URL refresh. | #188 |
| F3 | 404 artifact: immediate failure. | #188 |
| F4 | One 503: retried, clone succeeds. | #188 |
| F5 | Connection cut after N bytes: resumes with Range from the saved offset (proxy counts Range requests). | #188 |
| F6 | Proxy without Range support: restarts only that artifact. | #188 |
| F7 | S3 emulator: signed URL expires mid-download: the CLI refreshes URLs for the same B and completes, with `/v1/artifacts/` refused. | #191 (S3 job) |
| F8 | SIGINT during the download: exit 130, no target, staging directory, or marker left; the next clone succeeds. | #188 |
| F9 | An artifact that answers 503 every time: exactly 3 attempts, then exit 6; no target, no staging. | #208 |
| F10 | With 256 KiB parts, the 2 MiB history pack downloads as parallel ranges whose 206 answers tile it exactly once; one 503 retries only that range; the clone is exact. | |
| F11 | A proxy that ignores Range: the ranged download falls back to one whole GET of that artifact, and the clone is exact. | |
| F12 | A flipped byte in one range fails the clone on the hash check and leaves nothing. | |

### G. Clone shapes produce correct trees (28)

| # | Test | Added |
|---|---|---|
| G1 | Depth 1 editable: shallow, `git status` clean, files equal origin. | #186 |
| G2 | Full editable: `rev-list --count` equals origin, `git fsck` clean. | #186 |
| G3 | Files mode: tree equals `git archive` of B, including symlinks and exec bits. | #186 |
| G4 | Push, rewind force-push, and non-ancestor force-push in sequence: each new full clone equals the new tip. | #186 |
| G5 | Enough pushes to seal and compact history levels: full clone stays complete. | #186 |
| G6 | Small push after a 12 MiB tree: B's Files clone reuses chunks A's Files clone downloaded, the store grows by under half the tree, and B's tree is exact. | #208, #221 |
| G6b | Two pushes after the same 12 MiB tree, each Files built on the last (an edit inside a large file and a new file beside it; then a delete, a move, an exec bit, a symlink and 50 small files): each Files clone equals the origin's tree. Split from G6 so the two run in parallel. | #221 |
| G7 | Delete a file, then re-add the same bytes (same blob id): depth-1, full and Files clones of the tip all have it. | #208 |
| G8 | Empty trees: a root commit with no files, and a later commit that deletes every file. Every shape installs an empty tree. | #208 |
| G9 | Two branches admitted while the origin holds the fetch, then built together: each branch's clones equal its own tip in every shape. | #208 |
| G10 | Paths with spaces and non-ASCII characters, a symlink to one, an executable; a branch named `feature/new-ui`; an annotated tag via `--at`. Every shape equals its commit; the tag is detached, the branch checked out by name. | #208 |
| G11 | Pushes add tails to the Files archive: a small push stores only its new files and keeps the bundles a clone of its parent downloaded, until a build 25% past the compact size compacts the archive to about the tree's size. Files clones of the tip and an older commit are exact. | #233 |
| G12 | Editable clones work with the user's later git: the branch tracks its upstream, `origin` is the configured upstream, `git pull` and `git push` work, a tag or `--at` commit tracks nothing, and a branch named with `#`, `;` and `"` tracks correctly. | #234 |
| G13 | With a global git config naming a template dir and an identity, full and depth-1 clones have the local config, reflog (`clone: from <url>`) and template hooks of `git clone` and `git clone --depth 1`, apart from `core.checkstat`. The hook runs on the user's commit; `HEAD@{1}` is the cloned commit. A depth-1 clone's `git fetch` takes only its branch, a full clone's every branch. A SHA-256 default in the environment still gives a SHA-1 clone. With `user.useConfigOnly` and no identity, the clone has the reflogs `git clone` writes under git's fallback committer. | #234 |
| G14 | A full clone has every branch and tag of a plain `git clone`, from the server's refs (the CLI's upstream does not exist, so a fallback fetch fails the test): `refs/remotes/origin/*` with `origin/HEAD`, annotated, nested and lightweight tags, a side branch never built and its tags, both `git describe` forms; `fsck --full --strict` clean; `git switch side` checks out the side branch tracking `origin/side`. `--at v1` has them too, from the server's refs. `--at v1 --depth 1` fetches `+refs/tags/v1:refs/tags/v1` as git's does. After webhooks and no build: a tag on an old commit, a new branch, a deleted branch and tag and a moved tag each reach the next clone of the same commit. A depth-1 clone has only its branch and its tip's tags, equal to `git clone --depth 1`: none for an untagged tip; lightweight, annotated and a tag of a tag on a tagged one, `fsck --strict` clean. | |
| G15 | Tags on trees and blobs, lightweight and annotated, and remote-tracking branches equal `git clone`'s in full and depth-1 clones: tags on the tip's tree, a subtree and blobs in both; one on an older tree and one on a blob no commit holds only in the full clone, which gets them from the server's refs. | |
| G16 | Full clones of other commits, `--at v1`, `--branch side` and `--at` a built commit on no ref, make no upstream fetch (counted at the HTTP origin) and equal `git clone --branch` in checkout, remote refs, tags, `git describe`, reachable object count and `fsck --full --strict`, with their own tree checked out; the orphan has its own history too, and the side clone downloads none of the side commit's own history packs, nor its `own_history`. The user then switches to `main` and `side` and pulls a later push fast-forward. A default-branch commit newer than the published refs, built by branch or by its sha alone, gets no `refs_base` and its clone downloads no other Full; later reads of it ask the mirror no more ancestry questions (counted in the server log). | |
| G17 | After D and tag v2 are pushed with no Full slots, the Full of A carries the refs published before the push (tags and branches artifacts unchanged) with `refs_base` C. `--at v1`, `--branch side` and `--at C` clones make no upstream fetch and equal `git clone` of the origin as it was before the push (refs, tags, reachable objects, `fsck`); the user's `git fetch` then brings D and v2. | |
| G18 | A server that names a refs base its manifest does not match, or serves a corrupt side pack, fails the `--at v1` clone with an integrity error, leaving no target and no staging; the same clone straight from the server succeeds. | |
| G19 | `--single-branch` clones of the default branch, `--branch side` and `--at v1` equal `git clone --single-branch [--branch]` in checkout, local and remote-tracking refs, tags, fetch refspec, branch upstream, `git describe` and reachable objects, hold no unreachable object, make no upstream fetch and download no branches artifact or side pack. After pushes to main, side and a new branch with tags, `git pull` and `git fetch` in each give what they give in a git single-branch clone made at the same time; `git switch side` fails in the main clone. | |
| G20 | With the branch's Full not ready (no Full slots), a `--single-branch --branch side` clone tops up from the upstream and equals `git clone --single-branch --branch side`, downloading no branches artifact or side pack. | |
| G21 | Tags of tags up to 32 tag objects deep come as `git clone` gets them; one 33 deep is left out of the server's refs with a log line, and the full clone has every other tag from the server's refs, `fsck --strict` clean. | |
| G22 | A tags artifact (served through a proxy) that lists one name twice fails full and depth-1 clones, which leave no target or staging; the same artifact with distinct names clones. | |
| G23 | Head(X2) built on Head(X1), which held tagged X1, its tree and a blob X2 drops: the depth-1 clone of X2 holds X1 but has only the tags on X2 and its tree's objects, equal to `git clone --depth 1`. With the tree and blob tags deleted, a clone whose `git ls-tree` fails still drops X1's tags: commit tags never need the tree listed. | |
| G24 | A default-branch push publishes new branches and builds no side pack (the server's `pack-objects` of it is counted). A side-branch push adds a layer of only its new objects, and each new annotated tag a layer of its tag object, up to eight; the next change builds the side pack whole as one layer, and so does a force-push of the default branch that leaves a tag on its old commit. Every full clone, one of eight layers too, equals `git clone` from the server's refs. | |
| G25 | Without the server's branches (stripped by a proxy) a full clone fetches the refs with the user's git: a full disk (a `git` wrapper) fails the clone and leaves nothing; an unreachable upstream warns and keeps the clone's own branch; a reachable one gives `git clone`'s refs. | |
| G26 | When the first tip's Full fails (a server `git` wrapper fails its history pack), the Full of A carries no `refs_base`: an `--at v1` clone fetches the refs from the upstream and equals `git clone --branch v1`, `origin/main` at C. With no upstream either, it succeeds at A with the tags whose targets it holds, no `origin/side`, a warning, and `fsck` clean. | |
| G27 | A pull-request commit X1–X on no ref, whose Full and the tip C's both build on A's Full: X's `own_history` holds exactly X1's commit and tree, and names X's Head level packs. The `--at X` clone asks for it while a pack only C's Full has is held, downloads none of X's Full history packs C's lacks, and each shared pack, own pack and manifest once (counted at a proxy), makes no upstream fetch, and has X's history, `git clone`'s remote refs and tags, `fsck` clean. | |
| G28 | After main is force-pushed from A to B off A0, a pull-request commit X built on A gets no `own_history` with `refs_base` B: the `--at X` clone asks for X's Full history packs B's lacks while B's are held, makes no upstream fetch, and has X checked out, `git clone`'s remote refs, `fsck` clean. | |

### H. Storage and server state (14)

| # | Test | Added |
|---|---|---|
| H1 | Local disk: delete the client cache and restart the server: published artifacts still clone. | #186 |
| H2 | S3 emulator: same lifecycle; artifacts are in the bucket. | #191 (S3 job) |
| H3 | A second server on the same control database is rejected before it binds. | #186 |
| H4 | Table: invalid storage, credential and worker config (an S3 bucket without an endpoint, a malformed config file, a provider `token` in the file, an app key without an `app_id`) is rejected before anything is created in the state dir. So is every numeric server variable that is set and does not parse (poll and retention intervals, retention size, signed-URL TTL, ls-remote and fetch timeouts, LSM levels, Head rebase size), with an error naming it. | #208 |
| H5 | Local disk with retention every second and a 1 GB limit: the server reports retention off; month-old artifacts, past the 7-day limit, stay and clone. | #208 |
| H6 | S3 emulator, same settings: retention trims month-old local cache objects, the bucket is unchanged, published artifacts clone without a rebuild, and the next push builds. | #208 (S3 job) |
| H8 | Table: the control database file is deleted, or replaced by another file, under a running server: `/readyz` gives 503 and the log names the case. | #209 |
| H9 | Build scratch goes under `<cache>/tmp`, which startup empties: with a read-only `TMPDIR`, a stale scratch entry is removed and a full build and clone succeed. | #211 |
| H10 | S3 emulator: after a one-line push, B uploads no object of 1 MiB or more that the bucket already holds (its Files reuses A's bundles). A second repository with the same files builds the same large objects: the bucket proxy sees a HEAD and no PUT for each. A bucket copy cut short is uploaded again when a third repository with the same files makes it; that repository's clones and the Files clones of B and the second repository are exact. | #220 (S3 job), #221 |
| H11 | A scratch leftover the server cannot remove, as when a killed server's git child is still writing it, doesn't stop the next start: the server logs it, builds and clones in its own scratch dir, and once the leftover can go, the next start removes it. | #234 |
| H12 | S3 emulator, through a proxy that refuses `/v1/artifacts/`: depth-1, full, files, `--at` an older commit, `--branch` and top-up clones are exact, and none asks for the hash route. |  |
| H13 | Local disk, signed-URL TTL 3 s, deletion every second. A fork builds the same commit A as the repo, which also builds B and has a tag the fork lacks: the same output artifacts for A, and the repo's own tags artifact. A clone of B is held at its first download while `rm --purge` runs: B's commit reads `404`, `list` drops the repo, a new clone (plain or `--at B`) exits 5 with `repo not added` and leaves no target, and 2 s of sweeps delete nothing. Released, the clone is exact. Then the artifacts only the repo named, its refs' included, are deleted, the fork's stay, only the fork's mirror is left, and its depth-1, full and Files clones are exact. A second purge succeeds, and the repo added again builds and clones. |  |
| H14 | S3 emulator, same settings, the bucket's downloads held while the purge runs: the clone holding signed URLs from before it is exact; then the purged repo's own artifacts leave the bucket and the local cache, the fork's stay, and its full and Files clones are exact. | (S3 job) |
| H15 | The refs table is renamed under a running server: a ready Full still answers, without refs, the log names the read, and a full clone fetches the refs from the upstream, equal to `git clone`'s. | |

### I. Auth, integrations, CLI (15)

| # | Test | Added |
|---|---|---|
| I1 | Missing or wrong server token: 401; a valid token works. | #186 |
| I2 | Private repo with a token for the provider in the server's environment: the server builds it; the CLI holds no upstream token. | #202 |
| I3 | Table across GitHub, GitLab, Gitea, generic: the provider's auth header reaches the origin. | #191 |
| I4 | Table across GitHub, GitLab, Gitea webhooks: a signed push builds before any clone; a bad signature gets 401 and no build. (Tag webhooks are G14.) | #191 |
| I5 | Poller: with no webhook, a push is built within one poll interval, and a tag pushed with no commit reaches clones of the same commit with no build. | #186 |
| I6 | Every documented CLI verb exists (keep `docs_cli_surface`). | #193 |
| I7 | `--verify-upstream` auto, always, never (table). A clone that fails verification is removed; a target that was an empty directory is left empty. The upstream check runs while the clone downloads: with the upstream listing held, an artifact download still starts. A clone installs the tip the CLI read: when the server is behind, or a push lands after the server read the branch, full and depth-1 clones fetch that tip from the upstream. After a force-push during a single-branch clone, it keeps only the tags in the new tip's history. | #191 |
| I8 | Only `/healthz`, `/readyz`, `/metrics` and `/v1/version` answer without the token; unsigned webhooks fail closed (503 without a secret); every other route, `/v1/artifacts/{hash}` included, gives 401 to a missing or wrong token. | #208 |
| I9 | A client declaring another protocol gets 426; the CLI names the mismatch and says to run `ripclone version`; nothing builds. | #208 |
| I10 | `--verify-upstream` in Files mode: `never` and `auto` clone without contacting the upstream; `always` fails before any download. | #208 |
| I11 | Config discovery: the CLI reads `~/.config/ripclone/config.toml` and `RIPCLONE_CONFIG`; `RIPCLONE_SERVER` beats the file. | #208 |
| I13 | A clone with stderr on a pipe prints no progress line: depth-1, full, Files and `--quiet` clones leave stderr empty. A `--wait` clone of a pending build prints the "warming" note, and with `--quiet` only the pending error. | — |
| I14 | Stderr on a pseudo-terminal: a clone held mid-download draws one line with its phase, bytes of the total and percent, blanks it with spaces (no escape codes) before stdout reports the install. A top-up held at the origin shows `fetching <sha> from upstream (1s)`, with no byte figures. `add --wait` with the build held shows each output's state, every line narrower than the 60-column terminal. `--progress` draws the line on a pipe; with `--quiet` it is rejected. | — |
| I15 | Poller, with the upstream's `HEAD` detached so its listing names no default branch: a pushed branch still reaches a full clone, whose `origin/HEAD` stays on `main`, and while nothing changes the poller lists again and again without refreshing. | |
| I16 | `--verify-upstream=always` walks the clone from its commit: a proxy answering a full clone with the Head called a Full gives a clone without the tip's parent and no shallow file, which `always` fails and removes; `auto` keeps it. | |
| I17 | An upstream read over HTTP is one protocol v2 `ls-refs` POST, for the server's resolve and the CLI's `--verify-upstream`; an upstream that speaks only v0 gets `git ls-remote` and the same clone. | — |

### J. Builds start from the newest ready ancestor (7)

Each output of C builds on the same output of its base: the newest built
commit that is an ancestor of C. Tests read the published Head and Full
manifests: "built on A" means Head(C) keeps every Head pack of A and adds
under 64 KiB, and Full(C) keeps A's history packs and adds one small level.
"Cold" means C shares no Head or history pack with the other commit. Every
case clones C in depth-1, full and Files shapes, compares each with the
origin, and runs `git fsck` and `rev-list --count` on the editable clones.

| # | Test | Added |
|---|---|---|
| J1 | Origin moves A to C through several commits; only C is admitted (the commits between have no build). C is built on A, and the store grows by under 1 MiB though history holds 2 MiB of noise. | #217 |
| J2 | Same when C is a merge of a side branch. | #217 |
| J3 | A force push to a sibling X of the built A builds cold, and X's full clone lacks A. A force push of two commits on A then builds on A, not on the newer X, and lacks X. | #217 |
| J4 | The mirror directory is deleted between restarts: a push of two commits on A fetches A again and builds on it. After the origin force-pushes unrelated history and prunes A and C, the mirror is deleted again and the next build is cold. | #217 |
| J5 | A branch with no history in common with the built main builds cold, and its full clone lacks main's commit. | #217 |
| J6 | Each push adds one Head level that holds only objects no earlier pack has. The idx bundle holds only the base's idx files, so every Head on a base shares it, and each level's idx is fetched on its own. Past a quarter of the base, Full packs a new base. A lost parent idx also packs a new base. | #233, #257 |
| J7 | A rebase keeps the base packs the new commit still uses and repacks the rest. A pack that holds a replaced large file is dropped. | #233 |
| J8 | A push's Head and Files store their files table as a delta on the cold build's table, at most a quarter of it; Full shares Head's. Rewriting every file stores a whole table again. Response and manifest agree, and every clone shape is exact. | #263 |

### K. Scheduling and the mirror (7)

A repository runs at most one build of each kind at a time, and its Fulls
build in admission order. One repository's slow upstream never holds
another's builds or the API, and the mirror stays compact.

| # | Test | Added |
|---|---|---|
| K1 | The origin holds every fetch of X, and a second push of X is admitted, with two Head slots. X fetches once: its second build waits for the first. Meanwhile Y is added, built and cloned at depth 1, and `/add`, `POST /builds` and `/status` each answer within 2 s. After the release both X commits clone. | #218 |
| K2 | `RIPCLONE_FETCH_TIMEOUT_SECS=1`. The origin holds the fetch until the build retries, then answers: the build completes and clones. With the origin held throughout, with 2 attempts, Head and Files each fail after both (4 fetches) with a `timed out after` error, and nothing publishes. | #218 |
| K3 | The tip of a repo is admitted, then three older commits by SHA in one burst, out of history order, with four Full slots. Fulls publish in admission order (server log), and each commit's full clone equals the origin, with `rev-list --count` and `git fsck` checked. | #218 |
| K4 | While the server is down, its mirror gains 20 packs and 70 loose refs from outside. The next commit's Files and Full run together, and the later of them to finish compacts it after publishing: the log reports a repack and packed refs, under 16 packs remain, and full clones of the built commit and of a later push are exact. | #218 |
| K5 | `RIPCLONE_BUILD_LEASE_SECS=3`. The origin holds the first fetch while the server is stopped (SIGSTOP) for 4 s, then resumed. No lease is lost, Head starts once, the origin sees one fetch, nothing fails, and full and Files clones are exact. | #234 |
| K6 | While a server `git` wrapper holds the side pack's `pack-objects`, a default-branch push with a tag builds Head, and its depth-1 clone has the tag; once released, a full clone has the side branch and the tag. | |
| K7 | The first refresh's fetch is held, a side branch is pushed with its webhook, and the fetch then fails: the log shows the failure once, the server refreshes again, and a full clone has the side branch from the server's refs. | |

### L. Any tree git allows (4)

Every tree git can commit builds all three outputs and clones like `git
clone`. Trees are written with plumbing, so names a filesystem refuses and
gitlinks to commits the origin lacks work on any OS. macOS refuses
non-UTF-8 names (`git clone` there fails the same way) and folds NFD to NFC,
so L1 checks clones only on Linux and the builds everywhere.

| # | Test | Added |
|---|---|---|
| L1 | A tree of odd names (spaces at either end, leading dashes, newline, tab, quotes and backslash, glob characters, NFC and NFD forms, Latin-1 bytes in a file and a directory name, a symlink to a non-UTF-8 name, an executable) builds, and every clone shape equals the commit. A push renaming the non-UTF-8 file builds incrementally and clones exactly. | #219 |
| L2 | A submodule gitlink, never in the mirror: Head, Files and Full build. Each clone shape matches `git clone` without `--recurse-submodules`: `ls-files --stage` and `-v`, `status`, `submodule status`, and an empty directory for the submodule. Bumping the submodule repeats it. | #219 |
| L3 | A cold Full leaves no `.bitmap` in the mirror and its full clone equals the origin; with Full paused, a full clone of a push tops up from it. Then three pushes (one commit, three commits, a merge) each build Full from the tail since the last Full: a new level each, never a cold rebuild, and one `build-phases kind=full` log line per Full with `history_ms`. Each full clone equals the origin (`rev-list --count`, `fsck --strict`). With Full paused, a full clone of a new push tops up from the last one and is exact. | #219, #221 |
| L4 | A blob new in B is corrupted in the mirror after Head(B) and Files(B) were built. Full(B) fails naming the blob and publishes nothing; a full clone of B fails and leaves no target or staging; Full(A) and depth-1 B clone exactly. | #219 |

Total: 115 tests. Several are table-driven.

## Test settings

The code reads these variables so tests and benchmarks can shorten waits,
shrink thresholds or pause outputs. They are not in the user docs and are not
a stable interface. Every one is used by a contract test.

| Variable | Default | Effect |
|---|---|---|
| `RIPCLONE_POLL_MS` | 2000 | Longest wait between CLI polls for a pending output. Each wait is a quarter of the time waited, from 100 ms; a command stops after 50. |
| `RIPCLONE_DOWNLOAD_PART_BYTES` | 16 MiB | Range size when a large artifact downloads as 16 parallel ranges. |
| `RIPCLONE_ORIGIN_BASE` | unset | Fetch, top up and verify built-in `github` repos from `<base>/<owner>/<repo>.git`. |
| `RIPCLONE_LS_REMOTE_TIMEOUT_SECS` | 30 | Timeout of each upstream `ls-remote`. |
| `RIPCLONE_FETCH_TIMEOUT_SECS` | 3600 | Longest one mirror fetch may run. A fetch under 1000 B/s for 60 s stops sooner. |
| `RIPCLONE_BUILD_RETRY_BASE_MS` | 2000 | First retry delay after a retryable build failure; doubles up to 300 s. |
| `RIPCLONE_BUILD_MAX_ATTEMPTS` | 5 | Attempts per output before it fails. At least 1. |
| `RIPCLONE_BUILD_LEASE_SECS` | 60 | Build lease, renewed every third. At least 3. |
| `RIPCLONE_HEAD_SLOTS`, `RIPCLONE_FILES_SLOTS`, `RIPCLONE_FULL_SLOTS` | cores/4, at least 2 | Builds of each kind at once. Head needs at least 1; `0` pauses Files or Full. |
| `RIPCLONE_LSM_MAX_LEVELS` | 16 | History levels a Full keeps before merging the smallest neighbors. |
| `RIPCLONE_HEAD_REBASE_BYTES` | 128 MiB | Head levels past their base at which Full rebases them. |
| `RIPCLONE_SIGNED_URL_TTL_SECS` | 1200 | Lifetime of signed artifact URLs. A purge deletes artifacts after twice this. |
| `RIPCLONE_RETENTION_INTERVAL_SECS` | 300 | How often S3-mode retention trims the local cache and purged artifacts are deleted; `0` turns both off. |

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
- `s3`: C2, C3, C7, C8, F7, H2, H6 and H10 against the S3 emulator (`scripts/s3_emulator.sh`, versitygw).
- `gitea`: the live Gitea provider row, then `scripts/trial_gitea.sh`.
- `lint`, `musl`, `docker`, `cargo-deny`.
- `e2e`: `scripts/e2e_local.sh` against prebuilt binaries.
- `benchmark`: a smoke run of `benchmark/shaped_benchmark.sh` when benchmark
  files change.

## User trials

Two scripts run the release binaries the way a user would, against private
repos, and exit non-zero on any failed check. Both start their own servers on
OS-picked ports on local storage (even under soup, whose S3 settings would
send private content to a real bucket), keep the machine's git config and
keychain out, never print a token, and delete their work directory
(`KEEP_TRIAL=1` keeps it).
`RIPCLONE_BIN_DIR` names the binaries, default `rust/target/release`.

- `scripts/trial_gitea.sh`: a digest-pinned Gitea in Docker, with this
  checkout's history pushed as a private repo. Covers `provider add` and
  `provider test`, add by URL, a full clone into the default directory, depth
  1, files and `--branch`, push from a clone and pull in another, and a clone
  right after a push. Refusals: a missing repo, a repo the server's token cannot
  read, a non-empty target, a wrong server token, a server without an upstream
  token. Runs in the `gitea` CI job.
- `scripts/trial_github.sh owner/repo...`: read-only, against private GitHub
  repos. The server reads with `RIPCLONE_TOKEN_GITHUB`, or as a GitHub App
  with `RIPCLONE_APP_KEY_GITHUB` and `RIPCLONE_TRIAL_APP_ID`; the user's git
  uses their gh login, or `RIPCLONE_TRIAL_GIT_TOKEN`. Full, depth-1 and files clones
  are compared with `git clone` (history, tree, local config, reflog), then
  pulled and fetched. Refusals: a missing repo and a server without a token.
  Manual: it needs a token and private repos. For this repo's owner: `soup run
  -p ripclone -e development -- sh -c 'RIPCLONE_TOKEN_GITHUB="$RIPCLONE_GITHUB_TOKEN"
  scripts/trial_github.sh <private repos>'`.

## Current state

- The suite lives in `rust/tests/contract/` and runs with `cargo test --test
  contract`, in parallel. The tools are in `net.rs` (gated origin, fault
  proxy, silent endpoint) and `harness.rs`; wire details are in one module,
  `harness::api`.
- The eight `s3_` rows start their own versitygw and are `#[ignore]`d; the
  `s3` CI job runs them with `--ignored s3_`. The live Gitea row runs in the
  `gitea` job with `--ignored gitea_`.
- The CLI polls at most 200 ms apart in every test, so `--wait` lasts about
  8 s, inside the 10 s wait. D4 and I13 set 50 ms to run out its budget.
- No nightly stress or benchmark job exists.
