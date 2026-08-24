# ripclone design

How ripclone turns a `git clone` into a parallel download from object storage. This is the deep dive; for the overview, clone modes, and the design principles, start with the [README](../README.md).

## Why a normal clone is slow

A normal `git clone` is slow because it does several slow things in a row. The client and server figure out which objects to send. The client unpacks them and git builds an index. Then git writes out every file. Each step is fine alone, but chained together they make a lot of round trips and disk work that is hard to overlap.

ripclone moves that work to the server, ahead of time. By the time you clone, the hard parts are done — the client just downloads the finished pieces and writes them to disk.

## Building a clonepack

On every push, ripclone mirrors the repo and builds a **clonepack** for `HEAD` so the clone is fast. A clonepack has three parts:

- **Manifest.** A small file listing everything else. The client grabs this first to know what to fetch.
- **Metadata chunk.** The repo's shape: a skeleton pack, a ready-made `.git/index`, and a table that says where each file lives. The client uses it to build `.git/` without running any git commands.
- **Content.** The objects and file bytes themselves, built three ways (HEAD pack, history packs, archive) so each clone takes only what it needs.

### The skeleton

The skeleton is a git packfile with the `HEAD` commit and every tree, but no file contents. That's enough for git to know the shape of the repo — every folder, file, mode, and blob hash. The client drops it into `.git/objects/pack/` next to the prebuilt index, so `git ls-tree`, `git log`, and `git status` work right away.

It is exactly the [HEAD pack](#the-three-content-artifacts) **minus the blobs**, and it ships inside the metadata chunk — small, and downloaded first — so **every** clone has the repo's full shape before any file content arrives. The content artifacts then layer the file bytes on top. The trees end up in both the skeleton and the HEAD pack, but git dedupes objects by hash, so the overlap costs nothing. This shape-first split is what lets a clone be useful almost immediately and stream the bulk of the bytes behind it.

### The three content artifacts

ripclone builds the content three ways so each clone takes only what it needs:

***HEAD pack.*** One undeltified git packfile with `HEAD`'s commit, trees, and every blob — the skeleton's shape plus the actual file bytes, a complete `--depth=1` repo. The client drops it into `.git/objects` and reads the working tree straight out of it: one download, no archive, no extra work. Because the objects live in `.git`, the git content commands work. An **editable** `--depth 1` clone installs just this.

***History packs.*** The rest of the commits, trees, and blobs, delta-compressed and split into immutable levels plus a small tail. A deeper or full editable clone (`--depth N` / `--depth 0`) adds these on top of the HEAD pack; git reads them for older history and the client never hand-parses them.

***Archive.*** The same `HEAD` file bytes, zstd-compressed and split into chunks. Each chunk is made of independent frames, so the client can download many at once and start writing files as the first bytes land. It's the fastest path to a working tree, but the files don't go into `.git`, so git content commands don't work. This is what **files** mode uses.

### Separate exact results

Each exact commit stores three independent results: **Head**, **Full**, and
**Files**. A sync builds and publishes Head first. It then builds missing Full
history and Files archive work concurrently, publishing each as soon as it is
complete. A later job checks these fields and never rebuilds a result that is
already present.

A depth-1 editable clone requests Head, a full editable clone requests Full,
and files mode requests Files. A missing result returns `202` while the one job
for that exact commit is active. Readiness comes only from the requested stored
result; job state reports pending or failed work.

### Content-defined chunking and cheap re-syncs

The archive is split by content, not by fixed size: cut points land on the data itself (frames run about 1–16 MB). Each frame is compressed on its own and named by the hash of its bytes.

This makes re-syncs cheap. When a new commit lands, frames that didn't change hash to the same name and are reused as-is — no recompressing, no re-uploading. Only the frames that actually changed get rebuilt, and the builder reads just those changed regions, so the work matches the size of the diff, not the whole repo. The same is true of everything else a sync builds: only the commits and objects new since the last sync get packed, and re-syncing a commit that's already built does no work at all. A re-sync costs about what the diff costs.

## Performance

The headline clone numbers are in the [README](../README.md#performance), with the full sweep in [`BENCHMARKS.md`](BENCHMARKS.md). For `--depth 1` ripclone is roughly **3–6× faster** than native `git clone`; for a full clone it is up to **~10–12× faster** (repo-dependent — bigger on `oven-sh/bun` than on `pandas-dev/pandas`), because git makes the host compute and stream the whole history pack on demand while ripclone just downloads pre-built, content-addressed packs in parallel. `files` mode (working tree only, from the zstd archive) is the fastest of all.

Measured on a Fly `performance-8x` client (Newark) against a ripclone server in Ashburn with artifacts in Tigris; warm server cache, client artifact cache disabled, written to an NVMe volume. git clones are from GitHub over the same link. Median of 3 runs.

> `torvalds/linux` is shown at `--depth 1` only — the realistic case for a repo this size. Pre-building its full ~1.3M-commit history is a heavy one-time job that our dev box couldn't complete (the object-storage upload of that much data times out); the depth=1 path, which is what CI and agents actually use, is unaffected.

### Sync performance

How long a sync takes to build the artifacts (server-side, the same hardware as the clone numbers). There's no git equivalent — git builds nothing ahead of time.

| repo | Head ready | Full and Files background work |
|---|---|---|
| `facebook/react` | 5.4 s | +32 s |
| `oven-sh/bun` | ~8 s | +13 s |
| `torvalds/linux` | ~40 s | very large |

Head is what a `--depth 1` clone waits for. Full and Files run concurrently in
the background and gate only their matching clone modes. React's measurement
is a cold first build; Bun's is much shorter because the incremental re-sync
reuses unchanged history levels and archive frames. Linux's Head build is
dominated by its ~95k-file tree, and its full history is large enough that we
don't pre-build it on the dev box.

> In production the server syncs on push, so this happens once per commit, ahead of any clone — by the time a CI runner or agent asks for the repo, the artifacts are already built.
