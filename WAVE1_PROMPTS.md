# Wave 1 — session prompts (bundled: 5 sessions)

> One session = one worktree = one branch = SEVERAL plan nodes, executed in the
> listed order with ONE COMMIT PER NODE (commit message starts with the node id,
> e.g. "A1: ..."). Exception: micro-fixes (~<20 lines, e.g. a review-requested
> change) ride the existing PR branch in any session — no new worktree, no ceremony. Nodes are bundled by file locality, so what used to be
> cross-session merge rules is now just "do this one next." The plan
> (LAUNCH_PLAN.md) stays the source of truth for every node's spec.
>
> Session discipline (applies to all):
> - Finish a node completely (checks green) → commit → then start the next.
> - TEST ECONOMY (OSS sessions): per node, run fmt + clippy + only the test files
>   your node touches, in DEBUG: `cargo test --test <file>` — never --release
>   locally (the dev profile optimizes deps, so heavy paths run near release
>   speed while your changes compile in seconds). NO local full-suite or flake
>   runs at all — GitHub CI runs the full release suite + flake guard on every PR
>   push; that's the gate. A test that only passes in release is a test bug:
>   report it, don't escalate the profile.
> - BUILD CACHE: `export RUSTC_WRAPPER=sccache` at session start (worktrees keep
>   their own target dirs; sccache shares the compiled artifacts). If the machine
>   is saturated, also `export CARGO_BUILD_JOBS=4`.
> - If a node goes sideways, commit NOTHING for it, note it in your summary, and
>   move on only if the next node doesn't depend on it.
> - End with a per-node summary: done/committed, skipped(+why), things noticed
>   but not touched.

## Worktree setup

**Base commit — this is the part that matters.** All wave-1 branches cut from LOCAL
`main`, which must be at `0c47cf8` ("docs: launch plan + wave-1 session prompts")
or later. That commit is the first one containing BOTH the plan files and the
token-store rework — every Track-A file:line reference assumes it. Verify before
cutting anything:

```sh
git -C ~/Documents/Github/turbogit log --oneline -2 main
# must show:
#   0c47cf8 docs: launch plan + wave-1 session prompts
#   a033502 auth: store CLI tokens in the ripclone token file, drop the OS keyring
```

(origin/main is current — base on origin/main or local main interchangeably.)

Worktrees are cut from a commit of the shared repo — it does not matter which
existing checkout you run these from; the existing ~15 feature worktrees are
irrelevant to wave 1. Run from the primary checkouts; `../` paths are siblings
of the repo directory:

```sh
cd ~/Documents/Github/turbogit
git worktree add ../wt-server -b wave1/server-correctness main
git worktree add ../wt-config -b wave1/config-auth main
git worktree add ../wt-cleanup -b wave1/cleanup-profile main
git worktree add ../wt-tests  -b wave1/tests-docs main

cd ~/Documents/Github/ripclone-cloud    # main here is already pushed (2167136+)
git worktree add ../rc-wave1 -b wave1/cloud-batch main
```

## Merge order

1. **wave1/server-correctness** and **wave1/config-auth** merge first (small
   surgical diffs, different files from each other).
2. **wave1/cloud-batch** merges anytime (separate repo).
3. **wave1/tests-docs** merges next (mostly new files).
4. **wave1/cleanup-profile** merges LAST — its session rebases onto latest main
   and reruns fmt + clippy before finalizing (bulk deletion = the conflict
   magnet). PR CI runs the full suite.

## Shared preamble — paste at the TOP of every session

```
You are executing several nodes of a launch plan, in order, one commit per node
(commit message starts with the node id). The plan is at
~/Documents/Github/turbogit/LAUNCH_PLAN.md — read the "How to use" preamble and
each of YOUR nodes (named below) before touching anything. Rules that override
defaults: no mocks in tests (real servers/git/binaries — see rust/tests/common/mod.rs
for the harness); match existing code style and comment density; no plan/finding
references in code comments (node ids go in COMMIT MESSAGES only); do exactly each
node's scope — list adjacent problems in your final summary instead of fixing them.
Work ONLY in your assigned worktree and branch. Finish each node (its named checks
green) and commit before starting the next. End with a per-node status summary.
```

---

## Session 1 · Codex · ../wt-server · branch wave1/server-correctness

server.rs correctness batch — all three nodes live in the same file, and A4
depends on A1, so they belong to one ordered session.

```
[shared preamble]
Your nodes, in this order (LAUNCH_PLAN.md → Track A):
1. "A1. Phase-2 publish commit guard"
2. "A2. Per-repo authz on the five ungated content endpoints"
3. "A4. Stuck-202 pair" (it builds directly on your A1 change)
Repo: this worktree (turbogit).
```

## Session 2 · Codex · ../wt-config · branch wave1/config-auth

config/auth/CLI correctness batch — A3 owns config.rs's write paths, A5 its load
path; in one session there is nothing to coordinate.

```
[shared preamble]
Your nodes, in this order (LAUNCH_PLAN.md → Track A):
1. "A3. Atomic + locked writes for tokens/config"
2. "A5. Small-fix batch" (all six items; item 1 touches the same config.rs you
   just hardened — keep both behaviors)
Repo: this worktree (turbogit).
```

## Session 3 · Kimi · ../wt-cleanup · branch wave1/cleanup-profile

The bulk session: delete, then let the compiler sweep, then instrument. B2
depends on B1; B4 touches the region Session 1 is editing, hence the gate below.

```
[shared preamble]
Your nodes, in this order (LAUNCH_PLAN.md → Track B):
1. "B1. Delete-list PR"
2. "B2. Dead-code flag + compiler sweep"
3. GATE before this one: rebase onto latest origin/main. If the branch
   wave1/server-correctness has NOT merged yet, STOP here and report — do not
   start B4 (it touches the same server.rs region).
   "B4. Profile phase-1 sync latency" (incl. the storage-amplification table and
   the decision-tripwire measurement)
Repo: this worktree (turbogit).
FINAL STEP regardless of how far you got: rebase onto latest origin/main, resolve
(your diff is mostly deletions — keep their changes, keep your deletions), rerun
fmt + clippy. PR CI runs the full suite + flake guard.
```

## Session 4 · Kimi · ../wt-tests · branch wave1/tests-docs

Test + docs batch — E1 is the priority and leads while context is fresh; the
rest are small isolated riders. All new files or doc edits; near-zero conflict.

```
[shared preamble]
Your nodes, in this order:
1. LAUNCH_PLAN.md → Track E → "E1. Byte-for-byte equivalence oracle" (incl. the
   LFS fixture and dual io_uring/POSIX runs — make the io_uring legs
   Linux-CI-conditional; develop on this macOS machine against the POSIX writer).
2. Track E → "E5. De-flake e2e_freshness.rs"
3. Track F → "F1. Quick-start truth" (README token fix, default dirs off /data,
   config-drift startup warning; the sync→add docs churn is accepted).
4. Track E → "E6. Feature inventory" — BOTH repos (turbogit +
   ~/Documents/Github/ripclone-cloud). READ-ONLY scan: write the table to
   INVENTORY.md at the turbogit repo root (commit it as the E6 commit); include
   the two pre-flagged decisions named in the node. Change nothing else.
Repo: this worktree (turbogit).
```

## Session 5 · Codex · ../rc-wave1 · branch wave1/cloud-batch (ripclone-cloud)

Cloud batch — separate repo, so it conflicts with nothing; three nodes touching
different areas (billing, webhooks, docs).

```
[shared preamble]
Your nodes, in this order (LAUNCH_PLAN.md in the TURBOGIT repo at
~/Documents/Github/turbogit/LAUNCH_PLAN.md):
1. Track A → "A6a. Cloud: unpaid entitlement fix" (red test first, then the fix)
2. Track A → "A6b. Cloud: webhook lifecycle handlers" (incl. the explicit-add
   filter rule)
3. Track G → "G5. Cloud doc-truth batch"
Repo: this worktree (ripclone-cloud). Checks per node: pnpm test.
```

---

## Queued behind wave 1 (start as sessions free up)

- **A-R** (adversarial gate): after Sessions 1, 2, and 5 merge — `codex challenge`
  on the combined Track-A diff, then Fable review.
- **E2** (GC race + MinIO CI) + **E4** (expiry mid-clone): a natural Session-6
  bundle in a fresh worktree; isolated tests.
- **G3** (observability + alert) + **G4** (backups): a cloud Session-7 bundle.
- **B3** (extraction-pipeline collapse): after Session 3 merges.
- **H1 spec**: Fable, in the main conversation, parallel to everything.

## If you can only run a few sessions at once, start order

Session 1 → Session 5 → Session 3 → Session 4 → Session 2
(1 and 5 are the correctness/revenue fixes; 3 is the long pole to start early;
2 is small and can slot in anywhere.)


---

# Wave 2 — session prompts

Same rules as wave 1 (preamble, one commit per node, test economy, micro-fix
allowance). Cut worktrees from origin/main (current). Only B5/B6.1/G7 wait on the
B4 measurement — everything below is unblocked.

## Session 6 · Codex · ../wt-providers · branch wave2/provider-track
The critical path (B5 needs C1). Sequential, one worktree.
```
[shared preamble]
Your nodes, in order: Track C → "C1. Storage-key unification" → "C2. Provider
config/token source cut" → "C3. CLI addressing + surface cut" (incl. item 4, the
D8 full-clone default flip) → "C4. Bitbucket cut".
Repo: this worktree (turbogit).
```

## Session 7 · Kimi · ../wt-simplify · branch wave2/simplify
```
[shared preamble]
Your nodes, in order: Track B → "B3. Collapse the duplicate extraction pipeline"
→ "B7. Env-knob cut".
Repo: this worktree (turbogit).
```

## Session 8 · Kimi · ../wt-gc · branch wave2/gc-warmth
GC/ref-store locality bundle: G1 + the two data-independent B6 items.
```
[shared preamble]
Your nodes, in order: Track G → "G1. OSS TTL GC + pin flag" (incl. GC + recovery-
poller defaults ON), then Track B → "B6. Sync-path efficiency + safety-critical
dedup" ITEMS 2 AND 3 ONLY (the CachingRefStore lock and the GC/path-validation
dedup — item 1 waits on the B4 measurement; skip it).
Repo: this worktree (turbogit).
```

## Session 9 · Kimi · ../wt-tests2 · branch wave2/tests
```
[shared preamble]
Your nodes, in order: Track E → "E2. GC race + MinIO in CI" → "E4. Expiry
mid-clone" → "E3. Provider webhook e2es" (a launch gate — GitLab/Gitea).
Repo: this worktree (turbogit).
```

## Session 10 · Kimi · ../wt-release · branch wave2/release
```
[shared preamble]
Your nodes, in order: Track F → "F2. Static builds or preflight" → "F4.
git-remote-ripclone decision" (recommendation in node: document it).
Repo: this worktree (turbogit). Note: F2 legitimately builds release binaries —
that IS the deliverable; the no-release rule is about test loops.
```

## Session 11 · Codex · ../rc-wave2 · branch wave2/cloud-ops (ripclone-cloud)
```
[shared preamble]
Your nodes, in order (plan in the turbogit repo): Track G → "G3. Observability +
alert" → "G4. Backups" (the restore DRILL needs Russell — script it, drill on a
scratch DB, leave the prod drill flagged) → "G8. Account deletion + Resend
transactional basics".
Repo: this worktree (ripclone-cloud).
```

## Session 12 · Kimi · ../rc-screens · branch wave2/screens (ripclone-cloud)
The long pole. SCREENS.md v4 + DESIGN.md are the complete spec.
```
[shared preamble]
Your node: Track H → "H2. Implement the screens". Read SCREENS.md and DESIGN.md
in ripclone-cloud FIRST — they are the spec; the plan node just points at them.
HARD scope fence: src/app pages/components only.
Repo: this worktree (ripclone-cloud).
```

## Session 13 · Codex · ../wt-trust · branch wave2/trust
```
[shared preamble]
Your nodes, in order: Track D → "D-1. --verify-upstream" (the node contains the
full spec incl. credential scoping) → "D-3. Telemetry disclosure". (D-2, the
security page, is written after D-1 lands — it goes in Session 11's repo later.)
Repo: this worktree (turbogit).
```

## Also runnable now (not sessions)
- **B4 measurement** — prompt already delivered; worktree ../wt-b4m.
- **A-R adversarial gate** — codex challenge, state-scoped, prompt from Fable.
- **#91 micro-fixes** — on its existing branch.

## Start order if capacity-limited
S6 (critical path) → S12 (long pole) → B4-measurement → S8 → S9 → S11 → S7 → S10 → S13.

## Merge notes
S6 and S7 both touch provider/extract-adjacent code lightly — merge S6 first.
S8 touches server.rs/remote_gc — merge before or after S6, either is clean.
H2 (S12) conflicts with nothing (frozen plane).
