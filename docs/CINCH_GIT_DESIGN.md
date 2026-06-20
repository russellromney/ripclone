# attic-git: fast agent-oriented git storage on a versioned filesystem

This doc focuses specifically on how `attic` — the versioned object filesystem
from `cinch-cloud/attic` — could be used as the storage layer for a fast git
clone/commit service aimed at coding agents.

It ignores the broader legacy cinch stack (control-plane, graph/SQL servers,
etc.) and treats attic as the substrate: chunked immutable files, manifest
snapshots, forks, and S3-backed storage.

---

## 1. Why attic is a natural git backend

`attic` already does most of what git wants:

```
attic                         git
─────────────────────────────────────────────────
chunk (sha-256, immutable)    blob / tree / commit (sha-1, immutable)
metadata.sqlite manifest      tree + index + refs
snapshot                      commit
branch                        branch
fork                          git checkout -b (copy-on-write)
restore / time travel         git checkout <old-commit>
S3-backed chunks              object store / packfiles
local-disk manifest cache     hot refs + index
```

The key properties that matter:

1. **Immutability.** Git objects never change; attic chunks never change.
2. **Content addressing.** Attic chunks are named by sha-256. Git objects are
   named by sha-1. A thin key-mapping layer is all that's needed.
3. **Cheap snapshots/forks.** A commit in git is an immutable snapshot; attic
   snapshots cost ~zero until divergence.
4. **Tiered storage.** Cold chunks live in S3; hot chunks are cached on the
   server's local disk. This is exactly the hot/cold split git repositories
   have.
5. **Working-tree exports.** Attic can export a snapshot as a tarball or mount
   it as a filesystem — the two things an agent needs.

---

## 2. Two ways to map git onto attic

### Mapping A: git objects stored in attic

Keep git's object model. Use attic as the durable object store underneath a
normal `.git` directory.

```
s3://attic-git/objects/
    <sha1-prefix>/<sha1>            # git object bytes
    packs/<repo>/<pack>.pack        # optional packfiles
    packs/<repo>/<pack>.idx
```

The attic layer handles:

- Chunking large objects (unusual for git, but useful for big blobs).
- Deduplicating object bytes across all repos.
- Caching hot objects on local disk.
- Streaming objects from S3 on demand.

A custom `git-remote-attic` lets you do:

```bash
git clone attic://github.com/oven-sh/bun.git
```

Pros:
- Fully git-compatible.
- Global deduplication across every mirrored repo.
- Existing git tools work unchanged.

Cons:
- Still speaks git protocol semantics.
- Packfile generation and delta chains remain git-specific work.

### Mapping B: a git repo *is* an attic volume

Forget `.git/objects`. The repo is just an attic volume. Each commit is a
snapshot; each branch is a fork.

```python
vol = attic.open("oven-sh/bun")
vol.snapshot("df55ab7")
vol.snapshot("main")        # mutable tag, updated by mirror

feature = vol.fork("main", "feature")
feature.put("README.md", b"new readme")
feature.commit()
feature.snapshot("my-commit-sha")
```

A translation layer converts between attic snapshots and git commit objects
when talking to upstream GitHub.

Pros:
- No packfiles, no delta chains, no negotiation.
- `clone` = open/fork volume + export snapshot.
- `commit` = snapshot volume + write git commit object.
- Time travel and branches are native attic operations.

Cons:
- Not git-compatible at the storage layer.
- Need a translation layer to push/pull from upstream git hosts.

### Recommended hybrid

Use **Mapping A for the object pool** (all blobs/trees/commits stored in
attic) and **Mapping B for the fast agent path** (HEAD is exported as an
attic snapshot/tarball so agents can skip packfile negotiation).

---

## 3. Concrete design: `attic-git-gateway`

A small service that mirrors git repos into attic and serves agent requests.

### 3.1 Storage layout

```
attic volume: git-objects-global
    chunks/<sha256>                         # content-addressed bytes
    objects/<sha1-prefix>/<sha1>            # maps sha-1 → chunk list

attic volume: github.com/oven-sh/bun
    snapshots/main/                         # latest HEAD snapshot
    snapshots/<commit-sha>/                 # pinned commits
    branches/main                           # points to latest snapshot
    tarballs/<commit-sha>.tar.gz            # pre-built working tree exports
    packs/<commit-sha>.pack                 # optional git packfile
```

The global `git-objects` volume holds every git object from every repo. The
per-repo volume holds snapshots, branches, and pre-built exports.

### 3.2 Mirror worker

A background process keeps the attic state in sync with upstream git:

1. Poll `git ls-remote` or listen to webhooks.
2. For each new commit:
   - Fetch commit, tree, and any new blobs.
   - Store objects in `git-objects` volume.
   - Build a working-tree snapshot in the per-repo volume.
   - Optionally build a tarball and packfile.
   - Update the `main` branch pointer.

Because attic chunks are content-addressed, the worker only uploads bytes that
aren't already in the global pool.

### 3.3 Clone flow

```
Agent ──POST /clone/oven-sh/bun/main────> Gateway
                                           │
                                           ├── lookup HEAD commit
                                           ├── check tarball cache
                                           └── return signed URL + {commit, tree}
Agent ──GET signed URL──────────────────> S3
Agent <─tarball────────────────────────── S3
Agent ──fetch commit+tree pack──────────> Gateway
Agent <─tiny packfile (commit + tree)
```

On the agent:

```bash
# Fast CDN download of current files
curl -fsSL "$tarball_url" | tar -xz

# Tiny git metadata fetch
git init
git remote add origin https://github.com/oven-sh/bun.git
git fetch --depth=1 --filter=blob:none origin main
git read-tree HEAD
```

Result: full working tree + ~2 MB `.git`, in roughly the time it takes to
download the tarball.

### 3.4 Commit flow

The agent edits files and creates new git objects (but does not pull old
blobs):

```bash
# stage changes
git add README.md

# write new tree without fetching old blob contents
tree=$(git write-tree --missing-ok)
commit=$(git commit-tree "$tree" -p HEAD -m "agent change")
git update-ref HEAD "$commit"
```

Then the agent sends only the new objects to the gateway:

```json
POST /commit/oven-sh/bun
{
  "branch": "main",
  "commit": "13fce4a...",
  "tree":   "6b23ed2...",
  "new_objects": [
    {"sha1": "c1f5573...", "bytes": "base64..."}
  ]
}
```

Gateway:

1. Verifies the parent commit exists in the object pool or upstream.
2. Stores new objects in the `git-objects` volume.
3. Pushes the new commit to upstream GitHub.
4. On success, creates a new snapshot/tarball for the updated branch.

### 3.5 Why this is faster than raw GitHub

| GitHub clone cost | attic-git fix |
|---|---|
| Server builds a packfile per clone | Tarball pre-built once, cached, served by CDN |
| Packfile contains every HEAD blob | Agent never fetches old blobs |
| Single git protocol connection | Stateless HTTP + parallel downloads |
| No dedup across repos | Global `git-objects` pool |
| Cold start on large repos | Hot local cache of popular objects/tarballs |

---

## 4. Filesystem-native integrations

These ideas use attic as a filesystem, not just an object store.

### 4.1 Working tree as an attic volume

Instead of extracting a tarball, give the agent an attic volume mounted at
`/workspace`:

```python
vol = gateway.fork("oven-sh/bun", "main", "agent-123")
# agent reads/writes /workspace backed by vol
```

Files are lazy-loaded from S3 on first read. Unchanged files share chunks with
the base branch. The agent sees the full tree instantly; only touched bytes are
ever downloaded.

### 4.2 Agent workspace = copy-on-write fork

Each agent gets its own fork of the repo volume. Commits are just snapshots:

```python
vol.put("README.md", b"new text")
vol.commit()                         # local attic snapshot
vol.snapshot("agent-work-42")
```

When the agent is done, the gateway promotes the snapshot to a git commit and
pushes it. This is "branch-as-a-volume" — no `.git` directory, no index, no
staging area unless you want one.

### 4.3 9P / FUSE mount with git metadata

The `attic/spike/ninep/` work in cinch-cloud already explored 9P mounts. An
attic-git volume could be mounted as a POSIX filesystem:

```bash
mount -t 9p attic-git:/oven-sh/bun/main /workspace
```

The mount shows the working tree. A small userspace daemon keeps a `.git`
metadata overlay so normal git commands work, while object storage goes through
attic.

### 4.4 Commit = snapshot diff

Because attic manifests are SQLite, the diff between two snapshots is a query:

```sql
-- files changed between main and agent-123
SELECT path FROM current_files
WHERE snapshot = 'agent-123'
EXCEPT
SELECT path FROM current_files
WHERE snapshot = 'main';
```

A commit becomes:

1. Snapshot the agent's volume.
2. Query the manifest diff vs parent.
3. Build a git tree from the diff.
4. Write a git commit object.
5. Push.

No `git status` scanning the disk. No index. Just manifest diffing.

### 4.5 Tarball as a first-class export

Attic snapshots can be exported as tarballs. The gateway keeps a tarball per
commit, stored as a single attic file. Since attic chunks large files, even a
big tarball is broken into cacheable 1 MB blocks.

For the agent use case, the tarball export is the fast path. For history
browsing, the object pool is the slow path.

### 4.6 Submodule volumes

A git submodule is just another attic volume. The parent repo's tree stores:

```
path: "vendor/libfoo"
mode: "160000"  # gitlink
sha1: <commit-sha>
```

The gateway maps that gitlink to an attic snapshot in the `libfoo` volume and
materializes it on demand. Recursive clones become recursive volume mounts.

### 4.7 Packfiles as attic files

Pre-built packfiles for common operations can live in the per-repo volume:

```
/github.com/oven-sh/bun/packs/main-full.pack      # full clone
/github.com/oven-sh/bun/packs/main-shallow.pack   # --depth=1
/github.com/oven-sh/bun/packs/main-blobless.pack  # --filter=blob:none
```

The gateway builds these asynchronously. An agent can request whichever format
its tooling needs. Packfiles benefit from attic's chunking: a small change at
the end of a pack doesn't require re-uploading the whole file.

### 4.8 Global object pool with local cache

All git objects from all mirrored repos go into one `git-objects` volume.
Popular objects (e.g., `package.json` files, common dependencies, base
Dockerfiles) stay cached on the gateway's local disk. An agent's first clone of
a popular repo is fast because the tarball is hot; subsequent operations are
fast because the objects are hot.

---

## 5. Very creative extensions

### 5.1 Git history as a DAG of attic snapshots

Don't just snapshot commits — snapshot trees and blobs too. The entire git
history becomes an attic DAG:

```
snapshot: commit-df55ab7
  ├── tree-abc123 (snapshot)
  │     ├── blob-111111
  │     └── blob-222222
  └── parent: commit-deadbeef
```

This lets you `attic restore` to any tree or blob, not just any commit. It
also makes `git log -- <path>` a manifest traversal instead of an object-graph
walk.

### 5.2 Lazy git object filesystem

Implement a FUSE filesystem where the paths are git object hashes:

```
/attic-git/objects/ab/cd1234...      # blob contents
/attic-git/trees/abc123...            # tree as directory
/attic-git/commits/df55ab7...         # commit metadata
```

`git clone --filter=blob:none` against this filesystem is instant because the
tree objects are local metadata and blobs are fetched on first read.

### 5.3 Differential tarball updates

If an agent already has yesterday's tarball, the gateway can send only the
chunks that changed. Because attic chunks are content-addressed, a delta is
just a list of chunk hashes. The agent reuses cached chunks and fetches new
ones.

### 5.4 Ephemeral CI volumes

CI jobs don't need a real git clone. They need a working tree at a specific
commit. The gateway gives each job a copy-on-write fork of that commit's
snapshot. The job runs, writes artifacts into the volume, and the volume is
discarded. No `.git`, no history, no pushback.

### 5.5 Repo garbage collection via attic GC

`git gc` repacks objects locally. With attic, GC is global: walk all manifests,
find reachable objects, delete unreachable chunks. Because chunks are shared,
deleteing one repo's unreachable objects doesn't affect other repos that still
reference the same bytes.

---

## 6. Minimal prototype

A focused weekend prototype:

1. One `attic` server holding two volumes:
   - `git-objects-global`
   - `github.com/oven-sh/bun`
2. A Python mirror script that:
   - Fetches `main` from GitHub.
   - Stores all objects in `git-objects-global`.
   - Builds a working-tree snapshot in the repo volume.
   - Builds a tarball export.
3. A tiny HTTP gateway:
   - `GET /:owner/:repo/:branch.tar.gz` → redirect to tarball.
   - `GET /:owner/:repo/:branch.git-metadata` → return commit + tree pack.
   - `POST /:owner/:repo/commit` → accept new objects, push upstream.
4. Benchmark clone and commit times vs raw GitHub.

If the clone numbers approach the `lazygit.py` results (~7 s for bun) and the
commit path avoids pulling old blobs, the prototype proves the concept.

---

## 7. Why volume forks beat even tmpfs+overlay staging

A June 2026 benchmark on a Fly `shared-cpu-4x` machine in `ewr` shows just how expensive the “materialize files onto a cloud rootfs” step is. Fly’s root filesystem is an overlay on an `ext4` volume mounted `nobarrier,nombcache`:

- Sequential direct write: ~9 MB/s
- Random 4K sync writes: ~1 MB/s, ~268 IOPS
- `/dev/shm` tmpfs: ~860 MB/s sequential

For `oven-sh/bun` (15,212 files, ~196 MB raw):

| path | time |
|---|---|
| ripclone direct-install to `/tmp` (rootfs) | ~37–42 s |
| ripclone with tmpfs+overlay staging | ~3–4 s |
| GitHub `--depth 1` to `/tmp` | ~44 s |

A separate 5 GB Fly volume (`ext4`, `/data`) avoids the rootfs-overlay penalty without consuming RAM. Worktree-add timings on the same machine:

| repo | rootfs (`/`) | 5 GB volume (`/data`) | speed-up |
|---|---|---|---|
| pandas | 10.5 s | 2.6 s | ~4× |
| bun | 57.7 s | 15.3 s | ~3.8× |

So even a plain attached volume is dramatically better than the overlay rootfs for small-file materialization.

The overlay staging hack works because it moves the small-file write storm off the network-backed rootfs and into tmpfs. But it is still a *materialization* step — every file is created on every agent.

An attic/Cinch volume fork goes one step further: it **removes per-agent materialization entirely**. The agent inherits the seed volume’s namespace, so the working tree appears instantly. Unchanged files share immutable chunks with the seed and can be served lazily from cache or S3. Only files the agent actually modifies become new extents. That makes the clone time essentially “fork the manifest + optionally warm the cache,” independent of how many files the repo contains.

In other words: overlay staging is the best portable Linux-VM optimization available today, but a copy-on-write volume seed is the architectural endgame for agent workspaces.

---

## 8. Summary

Attic gives exactly the primitives git wants:

- **chunks** for immutable, content-addressed objects;
- **snapshots** for commits;
- **forks/branches** for git branches;
- **S3-backed storage** for cheap durability;
- **local-disk manifests** for fast metadata;
- **tarball exports / mounts** for instant working trees.

A service that stores git objects in an attic global pool and exposes repo
HEADs as pre-built snapshots/tarballs could make agent clones an order of
magnitude faster for large repos, while keeping full git compatibility for
commit and push.
