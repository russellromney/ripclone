# Designing a fast git clone service — lessons from attic

This doc pulls back from "use attic directly" and abstracts the design
principles that make attic fast for versioned storage. The goal is a cloud
service that offers dramatically faster git clones for coding agents.

It does not depend on attic's implementation; it treats attic as a source of
lessons.

---

## 1. What attic gets right

Attic is a versioned object filesystem. These are the properties that matter
for git-like workloads:

| Principle | What it means | Why it helps git clones |
|---|---|---|
| **Metadata / data separation** | Small manifests live hot/local; large file chunks live in S3. | `git clone` should fetch a tiny manifest first, then only the bytes the agent touches. |
| **Content addressing** | Every chunk is named by its hash. | Git objects are already hashes; a global object pool deduplicates across repos. |
| **Immutable chunks** | Once written, a chunk never changes. | Aggressive caching at every layer: disk, CDN, edge. No invalidation complexity. |
| **Cheap snapshots** | A snapshot is a metadata pointer, not a data copy. | "Clone this commit" can be a metadata operation. |
| **Copy-on-write forks** | Branches/forks share data until someone writes. | Every agent can have an isolated workspace instantly, with no duplication. |
| **Pre-materialized exports** | Snapshots can be exported as tarballs or mounted. | The fast path for agents is "download a tarball," not "walk a graph." |
| **Lazy materialization** | Files aren't fetched until read. | Agents only pay for the files they actually touch. |
| **Tiered caching** | Hot chunks on local disk, cold chunks in S3. | Popular repos/objects are served from fast local cache. |
| **Queryable manifests** | Manifests are structured (SQLite) so diffs are queries. | `git status`, `git diff`, `git log --stat` can be fast metadata ops. |

The big insight: **git is already a content-addressed, immutable, snapshotting
system.** A cloud service just needs to expose those properties with the right
performance characteristics.

---

## 2. The agent workflow revisited

A coding agent usually does not need history. It needs:

1. The current files of a branch.
2. The ability to edit them.
3. The ability to make a new commit on top of HEAD.
4. The ability to push that commit upstream.

A service optimized for this workflow can skip most of what `git clone` does:

- It does not need to enumerate all objects.
- It does not need to build a packfile.
- It does not need to send old blobs to the agent.
- It does not need to materialize the entire `.git` directory.

The service should treat "clone" as **"give me a mutable fork of HEAD"** and
"commit" as **"turn my fork into a new commit."**

---

## 3. Abstract architecture

### 3.1 Core abstractions

```
Object Pool
    Immutable content-addressed store for all git objects (blobs, trees,
    commits, tags). Global across all repos. Backed by object storage (S3),
    with hot local cache.

Ref Store
    Mutable mapping of (repo, branch) → commit hash. Hot, fast, consistent.
    Can be a KV store, SQLite, or ETCD-like consensus store.

Snapshot Store
    Pre-materialized views of commits: tarballs, packfiles, filesystem
    snapshots. Cacheable, immutable, generated asynchronously.

Workspace Store
    Per-agent copy-on-write forks of a commit. Agents read/write here.
    Commits become snapshots.
```

### 3.2 Storage layout (example)

```
object pool (S3 + local cache)
    objects/<sha1-prefix>/<sha1>

per-repo metadata (fast store)
    repos/<owner>/<repo>/refs/heads/<branch>  → commit sha
    repos/<owner>/<repo>/refs/tags/<tag>      → commit sha
    repos/<owner>/<repo>/commit-graph         → adjacency list / reachability

snapshot cache (S3 + local cache)
    snapshots/<owner>/<repo>/<commit-sha>.tar.gz
    snapshots/<owner>/<repo>/<commit-sha>.pack
    snapshots/<owner>/<repo>/<commit-sha>.idx

agent workspaces (COW)
    workspaces/<workspace-id>/
        base_commit: <sha>
        overlay:     changes since base
```

### 3.3 Why this layout?

- **Object pool is global.** A `LICENSE` file that appears in a million repos is
  stored once. A dependency vendored in many repos is stored once.
- **Refs are tiny and hot.** A branch pointer is ~40 bytes. Millions of refs fit
  in RAM.
- **Snapshots are cacheable exports.** The most common clone operation becomes
  "serve a tarball from CDN."
- **Workspaces are isolated.** Each agent gets its own fork, but shares bytes
  with the base until it writes.

---

## 4. Operations

### 4.1 Clone

For an agent that wants the current files:

```
POST /clone
{
  "repo": "oven-sh/bun",
  "branch": "main",
  "format": "tarball"   // or "workspace", "packfile"
}
```

Service:

1. Looks up `refs/heads/main` → commit `C`.
2. Checks snapshot cache for `C.tar.gz`.
3. If missing, generates it asynchronously from the object pool.
4. Returns a signed URL to `C.tar.gz` + `{commit: C, tree: T}`.

Agent:

```bash
curl -fsSL "$tarball_url" | tar -xz
# minimal git metadata fetch
git fetch --depth=1 --filter=blob:none origin main
```

Result: full working tree + tiny `.git` directory, dominated by tarball
 download time.

### 4.2 Fork (even faster for agents)

For an agent that doesn't need local files at all, or wants a remote workspace:

```
POST /fork
{
  "repo": "oven-sh/bun",
  "branch": "main"
}
```

Service:

1. Creates a COW workspace pointing at HEAD.
2. Returns a workspace ID and a mount/read API.

The agent can read files lazily and write changes back. No tarball download, no
local `.git` directory, no extraction time.

### 4.3 Read object

```
GET /objects/<sha1>
```

Returns the object from the local cache if hot, otherwise fetches from S3.
HTTP/2 lets clients request many objects in parallel.

### 4.4 Commit

Agent has edited files in its workspace or local checkout. It sends only new
objects:

```
POST /commit
{
  "repo": "oven-sh/bun",
  "branch": "main",
  "parent": "df55ab7...",
  "tree":   "6b23ed2...",
  "commit": "13fce4a...",
  "new_objects": [ { "sha1": "...", "bytes": "..." } ]
}
```

Service:

1. Verifies the parent exists.
2. Verifies the tree can be built from cached/new objects.
3. Stores new objects in the object pool.
4. Pushes the new commit to upstream GitHub/GitLab.
5. Updates the ref and schedules snapshot generation.

The agent never had to fetch old blobs.

---

## 5. How each lesson from attic shows up

| Lesson | Service implementation |
|---|---|
| Metadata / data separation | Refs and commit graph in hot store; blobs in S3. |
| Content addressing | Object pool keyed by sha-1; global dedup. |
| Immutable chunks | Git objects never mutated; CDN cache forever. |
| Cheap snapshots | `clone` returns a pre-built snapshot URL. |
| Copy-on-write forks | Agent workspaces share base bytes until write. |
| Pre-materialized exports | Tarballs / packfiles generated asynchronously. |
| Lazy materialization | `/objects/<sha1>` fetches on demand; fork workspaces stream files. |
| Tiered caching | Local NVMe cache of hot objects + snapshots; S3 for cold. |
| Queryable manifests | Ref store and commit graph enable fast status/diff/log queries. |

---

## 6. Creative extensions

### 6.1 Reflink clones on local SSD

Keep canonical checkouts of popular commits on the service's local SSD. A
clone request becomes a reflink copy:

```bash
cp -c --reflink=always /cache/bun/df55ab7 /workspaces/agent-123
```

Sub-second, zero extra disk usage until writes. APFS, Btrfs, and XFS support
this.

### 6.2 Container layers per commit

Package each commit as an OCI image layer. An agent environment is:

```dockerfile
FROM cinch-git/oven-sh/bun:df55ab7
```

Container registries are already optimized for CDN distribution and layer
caching. This is the fastest possible "clone" for containerized agents.

### 6.3 Differential working-tree sync

If an agent already has commit `C` and wants commit `C'`, send only the chunk-
level diff. Because the object pool is content-addressed, the delta is just a
list of object hashes the agent is missing.

### 6.4 Git object filesystem (lazy FUSE/9P)

Expose the object pool as a filesystem:

```
/gitfs/objects/<sha1>
/gitfs/trees/<tree-sha>/<path>
/gitfs/commits/<commit-sha>/
```

A `git clone --filter=blob:none` against this filesystem is instant because
trees are local metadata and blobs are fetched on first read.

### 6.5 Predictive prefetch

Use simple heuristics or a small model to predict which files an agent will
touch. For example, if the agent is asked to "fix the Redis connection logic,"
prefetch files that import Redis-related modules. The tarball can be
augmented with likely-needed blobs.

### 6.6 Public object CDN

Open-source objects can be cached at CDN edges. If thousands of agents clone
`bun`, `react`, `vscode`, etc., the chunks are served from nearby edge caches,
not from a central server.

### 6.7 Serverless snapshot builders

Use stateless workers (Lambda, Cloudflare Workers) to build tarballs and
packfiles on demand from the object pool. No persistent server needed for the
batch work; only the ref store needs to be consistent.

### 6.8 Agent-local object cache

Each agent runner keeps a local object cache shared across all its jobs. The
first clone of a repo warms the cache; subsequent clones of the same or related
repos are almost instant.

### 6.9 History as a queryable graph

Store the commit graph and tree listings in a graph database or KV store. Code
search, `git blame`, and `git log --stat` become indexed queries instead of
object-graph walks.

### 6.10 Submodules as independent forks

Each submodule is its own repo in the service. The parent repo's tree stores a
commit reference; the service materializes the submodule as a nested fork on
demand.

### 6.11 Immutable git remote protocol

Instead of the git pack protocol, define a simple HTTP/2 object protocol:

```
GET /v1/repos/oven-sh/bun/refs/heads/main
GET /v1/objects/<sha1>
POST /v1/repos/oven-sh/bun/push
```

No negotiation, no packfile generation, no delta reconstruction. Just objects
and refs.

---

## 7. What to build first

A minimal viable service:

1. **Object pool**: store git objects in S3 keyed by sha-1, with local cache.
2. **Ref store**: small DB mapping `(repo, branch) → commit`.
3. **Mirror worker**: poll upstream refs, fetch new commits, store objects.
4. **Snapshot builder**: generate tarballs per commit, store in S3.
5. **HTTP API**: clone (tarball URL + metadata), read object, commit.
6. **Agent helper**: small script like `lazygit.py` but talking to the service.

Benchmark: clone `oven-sh/bun` via the service vs direct GitHub. Target is
tarball-download time plus a sub-second metadata fetch.

If that works, add workspaces/forks, reflink clones, and container layers.

---

## 8. Summary

The right way to speed up git clones is not to build a better git client; it is
to change the storage and serving model so that:

- **Clone** becomes "download a pre-built snapshot" or "fork a COW workspace."
- **Objects** are content-addressed, globally deduplicated, and tiered.
- **Metadata** is hot, small, and queryable.
- **Commits** only move new bytes.

Attic demonstrates that this model works for versioned files. A git service
that applies the same principles can make agent clones an order of magnitude
faster without sacrificing git compatibility.
