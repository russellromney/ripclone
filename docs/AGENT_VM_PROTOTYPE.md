# Agent/VM Repo-Ready Snapshot Prototype

## Goal

Validate that an AI agent in a fresh Firecracker microVM can start working on `oven-sh/bun` in under a second after boot, by preloading a small **skeleton snapshot** instead of running a full `git clone`.

## What we built

A snapshot flow that bypasses FUSE on the agent side:

1. **Server** (`POST /v1/repos/{owner}/{repo}/snapshot`):
   - Mirrors the repo from GitHub.
   - Builds a skeleton pack (commit + tree objects + symlink blobs).
   - Creates a gzipped tarball containing a minimal `.git/` directory and optionally a set of hot files.
   - Stores the tarball in the CAS and returns its hash.

2. **CLI**:
   - `ripclone snapshot create owner/repo --hot-files N --output snap.tar.gz`
   - `ripclone snapshot extract snap.tar.gz --dir /repo`
   - `ripclone prefetch owner/repo --dir /repo --count N`

3. **Key trick**: all non-materialized index entries are marked `skip-worktree`, so `git status` is clean instantly even though the working tree is mostly empty.

## Results on `oven-sh/bun` (DigitalOcean droplet, Ubuntu 24.04)

| Method | Time | Bytes transferred |
|---|---|---|
| `git clone --depth=1` | 8.63 s | 262,911,735 |
| `ripclone skeleton clone` | 0.50 s | 4,720,556 |
| **snapshot tarball download (warm)** | **19 ms** | **3,347,829** |
| **snapshot extract + first `git status --short`** | **141 ms** | — |
| warm `git status --short` after extraction | 31 ms | — |
| prefetch 50 likely files into snapshot | 835 ms | — |
| `git status --short` after prefetch | 30 ms | — |

### Interpretation

- The snapshot is **~3.3 MB** for a repo whose shallow clone is **263 MB**.
- A warm snapshot can be **downloaded in ~20 ms** on localhost; over a fast object-storage link it should be well under 500 ms.
- After `tar xzf`, the repo is **usable in ~140 ms** total, with subsequent `git status` calls in **~30 ms**.
- Prefetching 50 hot files takes **< 1 s**, leaving the working tree clean and ready for real work.

## Architecture

```
GitHub
  │
  ▼
ripclone-server (mirror + CAS)
  │ builds skeleton snapshot tarball
  ▼
CAS  ──►  /v1/packs/<snapshot-hash>
  │
  ▼
agent VM boots ──► curl tarball ──► tar xzf ──► git status
  │
  └── background: ripclone prefetch hot files
```

## Implementation notes

- **No FUSE in the VM.** The agent sees a normal directory with a real `.git/`.
- **Skip-worktree index.** Lets git treat missing files as present; hot/prefetched files are cleared individually.
- **Hot files.** Current heuristic: top-level tracked files + files changed in the last 5 commits.
- **Symlinks preserved.** The tarball preserves symlinks; `AGENTS.md -> CLAUDE.md` works correctly.
- **Index stat data.** `update_index_sizes` sets cached file sizes so `git status` trusts stat without re-reading blobs.

## Limitations / next steps

1. **Snapshot build is server-side and synchronous.** For production, snapshots should be built asynchronously on every push and stored in S3/R2.
2. **Object storage not yet used.** Currently the tarball lives in the local CAS; moving it to object storage is the next step.
3. **No signed/edge URLs.** Agents fetch from the ripclone server; real deployment should use CDN-signed URLs.
4. **Hot-file heuristic is basic.** Could be improved with repo-specific models or agent task context.
5. **No write/commit path yet.** Agents can read and edit files, but pushing commits back to GitHub is not implemented in this prototype.
6. **Linux-only for now.** macOS/Windows agents would need a non-FUSE transport (WebDAV/NFS) or remote VMs.

## Conclusion

The prototype closes most of the gap between `git clone` and `s3 get`:

- A full shallow clone of Bun takes **~9 seconds** and **263 MB**.
- A repo-ready snapshot takes **~140 ms** after a **3.3 MB** download.

The next milestone is to move snapshot storage to object storage and serve it via CDN, then measure end-to-end latency from a real VM boot.
