# CAS-Based Clone Spike Findings

## What we tested

We ran four spikes against `oven-sh/bun` using a content-addressed object-store approach:

- Agent starts with **only the skeleton** (commit + all tree objects). No blobs.
- Blobs are fetched **lazily** as the agent reads files.
- Objects are stored in a CAS and transported as git packfiles.

All tests were run on macOS against a local bare mirror of `oven-sh/bun` (last 500 commits).

## Spike 1: Skeleton size and dedup

### Results

| metric | value |
|---|---|
| Skeleton pack per commit (bun HEAD) | **2.73 MB** |
| Objects per skeleton | ~8,181 commit/tree objects |
| Unique skeleton objects across 50 commits | **8,181** |
| Perfectly-shared skeleton pack (all 50 commits) | **2.60 MB** |
| Sum of 50 full per-commit skeleton packs | **125.42 MB** |
| Duplication overhead of full per-commit packs | **48×** |

### Takeaway

A single-commit skeleton is small (~2.5 MB) and highly shareable with neighboring commits. Duplicating the full skeleton per commit wastes a lot of storage, but the per-commit pack itself is cheap to fetch.

## Spike 2: Lazy-blob clone simulation

### Scenario

A realistic agent session reads **50 files** (source files, docs, build configs) from `oven-sh/bun@HEAD`.

### Results

| step | time | bytes |
|---|---|---|
| Fetch skeleton pack | 0.007 s | 2.73 MB |
| `git index-pack` skeleton | 0.11 s | — |
| Fetch 47 needed blobs | 1.16 s | 1.43 MB |
| **Total lazy clone** | **1.28 s** | **3.97 MB** |
| Baseline `git clone --depth=1` (local file://) | **7.29 s** | 289 MB working tree |

### Takeaway

For a partial read pattern, the lazy-blob approach transfers **~15× fewer bytes** and finishes **~5–6× faster** than a full shallow clone in this local test. The win should be larger over a real network because we avoid the full packfile and checkout.

## Spike 3: Working tree materialization

### Results

- Materializing **47 files** (1.36 MB) from CAS blobs: **1.50 s**
- Full `git clone --depth=1` (14,695 files): **6.62 s**

### Takeaway

Sparse materialization is faster than full checkout when the agent only touches a subset of files.

## Spike 4: CAS write amplification

### Results

| storage model | size (50 commits) | overhead vs perfect sharing |
|---|---|---|
| (a) Perfect sharing of unique skeleton objects | 2.60 MB | 1.0× |
| (b) Full skeleton pack per commit | 125.41 MB | **48.09×** |
| (c) **Delta skeleton pack per commit** | **3.08 MB** | **1.18×** |
| (d) Full working-tree pack per commit | 3,639.33 MB | 1,395× |

### Takeaway

**Delta skeleton packs are the right storage model.** They add only 18% overhead vs perfect sharing while keeping each update a single small packfile fetch. Full per-commit skeletons are wasteful; full working-tree packs are absurd for this use case.

## Critical implementation detail: packfiles vs loose objects

We discovered a major performance trap on the client side:

| operation | time for bun skeleton |
|---|---|
| `git unpack-objects` (writes 8k loose object files) | **31 s** |
| `git index-pack` (keeps objects in one pack + .idx) | **0.1 s** |

**Objects must stay in packfiles on the client.** Unpacking into `.git/objects/xx/sha` loose objects is catastrophically slow on macOS (and likely any filesystem) due to many small file writes. The client should place packfiles in `.git/objects/pack/` and run `git index-pack`.

## Recommended architecture

Based on these findings, the next version of ripclone should be:

1. **Server maintains bare mirrors** of tracked repos from GitHub.
2. **Server builds and stores in CAS:**
   - **Delta skeleton packs** per commit (commit + new/changed trees since parent).
   - **Blob packs** or individual blobs, content-addressed by SHA-1.
   - A ref map: `branch → {commit, parent_commit, skeleton_pack_hash}`.
3. **Client clone flow:**
   - Resolve branch to commit + skeleton pack hash.
   - Download skeleton pack, place in `.git/objects/pack/skeleton-<commit>.pack`, run `git index-pack`.
   - Set `HEAD`. Working tree is initially empty.
   - On file read, look up blob SHA from tree, fetch blob from CAS, write to working tree and `.git/objects` as a loose object.
4. **Client update flow (new commit on branch):**
   - Server returns parent commit and delta skeleton pack.
   - Client downloads delta pack and index-pack it.
   - Git now sees the new tree structure without re-fetching unchanged objects.

## Open questions for next iteration

- **Blob transport format:** For agents reading many files, should we batch blobs into small packfiles by directory or by predicted access pattern?
- **Ref freshness:** How often should the server refresh branch tips from GitHub? Webhooks + polling fallback.
- **Cross-repo dedup:** Does the CAS give meaningful savings across different repos, or is cross-commit dedup the main win?
- **Real-network numbers:** Re-run Spike 2 against the Fly-deployed service with actual latency/bandwidth.

## Files

- Plan: `docs/CAS_CLONE_SPIKES_PLAN.md`
- Spike scripts: `spikes/scripts/spike{1,2,3,4}.sh`
- Rust spike crate: `spikes/` (partial; shell scripts were used for reliable execution)
