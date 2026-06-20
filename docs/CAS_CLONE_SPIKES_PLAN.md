# CAS-Based Clone Spikes Plan

## Scope

Ripclone is **clone-only**. No commit/push proxy. The goal is to find a clone architecture that is meaningfully faster than a standard shallow git clone for coding-agent workflows.

## Core hypothesis

Agents start with **no blobs**. They receive only the commit + tree skeleton and fetch blob contents lazily as they read files. By duplicating tree/commit writes across commits (space-for-time), we can make the initial clone a single content-addressed fetch with no negotiation.

## Target architecture (to validate)

```
Server side:
  - Mirror repos from GitHub.
  - For each tracked commit, build a "skeleton pack": a git packfile
    containing the commit object and every reachable tree object (no blobs).
  - Store skeleton packs and individual blobs in a content-addressed store.
  - Expose: ref resolution (branch -> commit + skeleton hash) and object fetch.

Client side:
  - Ask server for branch -> commit + skeleton CAS hash.
  - Fetch skeleton pack from CAS, unpack into .git/objects, set HEAD.
  - When agent reads a file, look up its blob hash from the tree, fetch blob
    from CAS, write to .git/objects and working tree.
```

## Spikes

### Spike 1: Skeleton size and dedup

**Goal:** Determine if storing one full skeleton pack per commit is cheap enough to be practical.

**Build:**
- `spikes/make_skeleton_pack.py <bare-repo> <commit>`
  - Outputs `skeleton-<commit>.pack` containing commit + all reachable trees.
- `spikes/analyze_skeletons.py <bare-repo> <n-commits>`
  - Walks the last N commits of the default branch.
  - Generates a skeleton pack for each.
  - Reports per-commit size, object count, and sharing across commits.

**Measure:**
- Skeleton pack size per commit.
- Number of unique objects across all N skeletons.
- Total storage for N full skeletons vs perfect sharing.

**Decision:** If skeletons are single-digit MBs and duplication overhead is <5×, full-per-commit skeletons are acceptable. Otherwise, delta skeletons or a shared object store are needed.

---

### Spike 2: Lazy-blob clone simulation

**Goal:** Measure end-to-end clone time and bytes transferred for a realistic agent session.

**Build:**
- `spikes/cas.py` — minimal local CAS (directory of `<hash>` files + HTTP interface).
- `spikes/cas_server.py` — HTTP server wrapping the local CAS.
- `spikes/simulate_clone.py <repo-url> <commit-or-branch> <file-list>`
  - Resolves commit via server.
  - Fetches skeleton pack.
  - Unpacks into a temp `.git/objects`.
  - Sets HEAD and refs.
  - For each file in the list, fetches the blob from CAS and writes it to the working tree.

**Measure:**
- Time and bytes for skeleton fetch.
- Time and bytes for blob fetches (sequential vs parallel).
- Total time and bytes vs `git clone --depth=1 --filter=blob:none`.
- Use realistic file lists captured from actual agent-like exploration.

**Decision:** If lazy-blob fetch transfers fewer bytes and finishes faster for realistic sessions, the architecture is promising.

---

### Spike 3: Working tree materialization

**Goal:** Measure how fast we can create files from fetched blobs compared to git checkout.

**Build:**
- `spikes/materialize_tree.py <skeleton-pack> <cas-dir> <file-list>`
  - Unpacks skeleton, fetches required blobs, creates working tree files.

**Measure:**
- Time to materialize N files from CAS blobs.
- Time for equivalent `git checkout` from a shallow clone.
- Sparse materialization: only create files in the file-list.

**Decision:** Identify whether file creation is a bottleneck and whether sparse materialization helps.

---

### Spike 4: CAS write amplification

**Goal:** Quantify storage overhead of duplicated skeletons vs shared/delta storage.

**Build:**
- `spikes/analyze_storage.py <bare-repo> <n-commits>`
  - Computes four storage models:
    1. Perfect sharing: sum of unique raw objects across N commits.
    2. Full skeleton per commit: sum of all skeleton packs.
    3. Delta skeletons: for each commit, pack only objects not in its first parent.
    4. Full working-tree pack per commit: commit + trees + all blobs for HEAD.

**Measure:**
- Size for each model.
- Overhead of (2) and (3) relative to (1).
- Cost of (4) as a sanity check.

**Decision:** Choose the storage model that balances simplicity and overhead.

## Spike tooling layout

```
spikes/
  cas.py                 # CAS primitives
  make_skeleton_pack.py  # build a skeleton pack for one commit
  analyze_skeletons.py   # Spike 1
  simulate_clone.py      # Spike 2
  materialize_tree.py    # Spike 3
  analyze_storage.py     # Spike 4
  common.py              # shared helpers
```

## Suggested execution order

1. **Spike 1** — cheap, high information value. Kills the approach if skeletons are huge.
2. **Spike 2** — validates the core user-facing win.
3. **Spike 4** — decides the storage model once 1 and 2 look good.
4. **Spike 3** — optimization detail.

## Success criteria

- Skeleton packs are small enough to duplicate per commit (<10 MB for bun, <5× overhead).
- Lazy-blob clone transfers meaningfully fewer bytes than shallow git clone for realistic agent sessions.
- End-to-end time is competitive with or faster than `git clone --depth=1 --filter=blob:none`.
- Merkle tree is preserved (correct object hashes, clean git status for materialized files).
