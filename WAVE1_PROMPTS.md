# Wave 1 — session prompts

> One session = one worktree = one branch = one plan node. Every prompt tells the
> executor to read its node in LAUNCH_PLAN.md — the plan stays the single source of
> truth; these blocks only add session wiring.
>
> **Before firing anything:**
> 1. Resolve the uncommitted changes in the main turbogit checkout (another session's?).
> 2. `git add LAUNCH_PLAN.md WAVE1_PROMPTS.md && git commit` on main so worktrees see them.
> 3. Cut worktrees per session (commands below).

## Merge rules (read once, enforce at review)

- Merge small Track-A diffs as they finish: **A1 → A2 → A3 → A5** (all touch server.rs
  or config.rs in different regions; merging small-first keeps conflicts trivial).
- **B1 merges LAST of the OSS batch**: its session must rebase onto latest main and
  rerun full CI before the PR is final.
- **B4 rebases after A1 merges** (both touch the do_sync region; B4 is additive spans).
- **A4 starts only after A1 merges** (same code region, A4 deps A1).
- Every PR: `scripts/ci.sh lint test` green (+ `flake` if e2e touched) → `codex review`
  → Fable batch-review → merge.

## Worktree setup

```sh
cd ~/Documents/Github/turbogit
git worktree add ../wt-a1 -b wave1/a1-phase2-guard main
git worktree add ../wt-a2 -b wave1/a2-authz-endpoints main
git worktree add ../wt-a3 -b wave1/a3-atomic-writes main
git worktree add ../wt-a5 -b wave1/a5-small-fixes main
git worktree add ../wt-b1 -b wave1/b1-delete-list main
git worktree add ../wt-b4 -b wave1/b4-sync-profile main
git worktree add ../wt-e1 -b wave1/e1-equivalence-oracle main
git worktree add ../wt-e5 -b wave1/e5-deflake-freshness main
git worktree add ../wt-f1 -b wave1/f1-quickstart main
# E6 is read-only — run it in any worktree or the main checkout, no branch needed.

cd ~/Documents/Github/ripclone-cloud
git worktree add ../rc-a6a -b wave1/a6a-unpaid-fix main
git worktree add ../rc-a6b -b wave1/a6b-webhook-lifecycle main
git worktree add ../rc-g5 -b wave1/g5-doc-truth main
```

## Shared preamble — paste at the TOP of every session

```
You are executing one node of a launch plan. The full plan is at
~/Documents/Github/turbogit/LAUNCH_PLAN.md — read the "How to use" preamble section
and YOUR node (named below) before touching anything. Rules that override defaults:
no mocks in tests (real servers/git/binaries — see rust/tests/common/mod.rs for the
harness); match existing code style and comment density; no plan/finding references in
code comments; do exactly your node's scope — list adjacent problems in your summary
instead of fixing them. Work ONLY in your assigned worktree and branch. When done:
run the checks your node names, then write a summary: what changed, test evidence,
anything you noticed but didn't touch.
```

---

## Batch 1 — fire all of these in parallel

### S1 · Codex · worktree ../wt-a1
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track A → "A1. Phase-2 publish commit guard".
Repo: this worktree (turbogit), branch wave1/a1-phase2-guard.
```

### S2 · Codex · worktree ../wt-a2
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track A → "A2. Per-repo authz on the five ungated content
endpoints". Repo: this worktree (turbogit), branch wave1/a2-authz-endpoints.
```

### S3 · Codex · worktree ../wt-a3
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track A → "A3. Atomic + locked writes for tokens/config".
Repo: this worktree (turbogit), branch wave1/a3-atomic-writes.
Note: another session is editing config.rs's LOAD path (parse errors). You own the
WRITE paths. Different functions — expect a trivial rebase, don't coordinate further.
```

### S4 · Codex · worktree ../wt-a5
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track A → "A5. Small-fix batch" (all six items).
Repo: this worktree (turbogit), branch wave1/a5-small-fixes.
Note: another session owns config.rs's WRITE paths (atomic writes). You own the LOAD
path (item 1). Different functions — expect a trivial rebase.
```

### S5 · Kimi · worktree ../wt-b1
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track B → "B1. Delete-list PR".
Repo: this worktree (turbogit), branch wave1/b1-delete-list.
MERGE RULE: several small correctness PRs are landing in parallel. Before declaring
done, rebase onto latest origin/main, resolve (your diff is deletions — keep their
changes, keep your deletions), and rerun scripts/ci.sh lint test flake.
```

### S6 · Kimi · worktree ../wt-b4
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track B → "B4. Profile phase-1 sync latency" (including
the storage-amplification table and the decision-tripwire measurement).
Repo: this worktree (turbogit), branch wave1/b4-sync-profile.
MERGE RULE: rebase after the phase-2-guard PR (wave1/a1) merges — you touch the same
region with additive tracing spans. No behavior changes.
```

### S7 · Kimi · worktree ../wt-e1
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track E → "E1. Byte-for-byte equivalence oracle"
(including the LFS fixture and the dual io_uring/POSIX runs).
Repo: this worktree (turbogit), branch wave1/e1-equivalence-oracle.
Note: io_uring runs only on Linux — make the dual-writer legs Linux-CI-conditional;
develop the rest on this machine (macOS) against the POSIX writer.
```

### S8 · Kimi · worktree ../wt-e5
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track E → "E5. De-flake e2e_freshness.rs".
Repo: this worktree (turbogit), branch wave1/e5-deflake-freshness.
```

### S9 · Kimi · worktree ../wt-f1
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track F → "F1. Quick-start truth" (README token fix,
default dirs off /data, config-drift startup warning; note the accepted sync→add
docs churn).
Repo: this worktree (turbogit), branch wave1/f1-quickstart.
```

### S10 · Kimi · any checkout, read-only, no branch
```
[shared preamble]
Your node: LAUNCH_PLAN.md → Track E → "E6. Feature inventory" — BOTH repos
(turbogit at ~/Documents/Github/turbogit, ripclone-cloud at
~/Documents/Github/ripclone-cloud). READ-ONLY: produce the table as a markdown file
at ~/Documents/Github/turbogit/INVENTORY.md; change no other file. Include the two
pre-flagged decisions named in the node.
```

### S11 · Codex · worktree ../rc-a6a (ripclone-cloud)
```
[shared preamble]
Your node: LAUNCH_PLAN.md (in the turbogit repo) → Track A → "A6a. Cloud: unpaid
entitlement fix". Repo: this worktree (ripclone-cloud), branch wave1/a6a-unpaid-fix.
Tiny node — red test first, then the fix.
```

### S12 · Codex or Kimi · worktree ../rc-a6b (ripclone-cloud)
```
[shared preamble]
Your node: LAUNCH_PLAN.md (in the turbogit repo) → Track A → "A6b. Cloud: webhook
lifecycle handlers" (including the explicit-add filter rule).
Repo: this worktree (ripclone-cloud), branch wave1/a6b-webhook-lifecycle.
```

### S13 · Kimi · worktree ../rc-g5 (ripclone-cloud)
```
[shared preamble]
Your node: LAUNCH_PLAN.md (in the turbogit repo) → Track G → "G5. Cloud doc-truth
batch". Repo: this worktree (ripclone-cloud), branch wave1/g5-doc-truth.
```

---

## Queued behind batch 1 (start as slots free up)

- **A4** (stuck-202 pair) — after A1 merges. Worktree ../wt-a4, Codex.
- **A-R** (adversarial gate) — after A1-A6 all merged: `codex challenge` on the
  combined Track-A diff, then Fable review.
- **E2** (GC race + MinIO CI), **E4** (expiry mid-clone) — anytime, isolated tests.
- **G3** (observability + alert), **G4** (backups) — anytime, cloud.
- **B2** (dead-code sweep) — after B1 merges.
- **H1 spec** — Fable, in the main conversation, parallel to all of the above.

## If you run fewer sessions, priority order

A1 → A2 → A3 → A6a → B1 → E1 → E6 → F1 → G5 → A5 → B4 → E5 → A6b
