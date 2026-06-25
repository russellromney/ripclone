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

### Two-phase publish

Most clones just want the latest `HEAD`, so a sync builds that first and the rest later.

- **Phase 1.** The server builds everything a `--depth 1` clone needs: the skeleton, the index, and a pack with `HEAD`'s files. This is ready fast and served right away. The ref says `build_status: "full history building"` so clients know more is coming.
- **Phase 2.** In the background, the server builds the full history, then the archive — reusing whatever didn't change since the last sync. When it's done, it clears `build_status`.

A clone that only wants `HEAD` is ready as soon as phase 1 finishes. An editable full clone (`--depth 0`) is ready once the history lands — it reads the working tree from the packs and never touches the archive, so only `files` mode waits for the archive. While a phase is still building, the server returns `202` and the client retries on its own.

### Content-defined chunking and cheap re-syncs

The archive is split by content, not by fixed size: cut points land on the data itself (frames run about 1–16 MB). Each frame is compressed on its own and named by the hash of its bytes.

This makes re-syncs cheap. When a new commit lands, frames that didn't change hash to the same name and are reused as-is — no recompressing, no re-uploading. Only the frames that actually changed get rebuilt, and the builder reads just those changed regions, so the work matches the size of the diff, not the whole repo. The same is true of everything else a sync builds: only the commits and objects new since the last sync get packed, and re-syncing a commit that's already built does no work at all. A re-sync costs about what the diff costs.

## Performance

The headline clone numbers are in the [README](../README.md#performance). For `--depth 1` ripclone is **4–6× faster** than native `git clone`; for a full clone it is **15–32× faster**, because git makes the host compute and stream the whole history pack on demand while ripclone just downloads pre-built, content-addressed packs in parallel. `files` mode (working tree only, from the zstd archive) is the fastest of all.

Measured on a Fly `performance-8x` client (Newark) against a ripclone server in Ashburn with artifacts in Tigris; warm server cache, client artifact cache disabled, written to an NVMe volume. git clones are from GitHub over the same link. Median of 3 runs.

> `torvalds/linux` is shown at `--depth 1` only — the realistic case for a repo this size. Pre-building its full ~1.3M-commit history is a heavy one-time job that our dev box couldn't complete (the object-storage upload of that much data times out); the depth=1 path, which is what CI and agents actually use, is unaffected.

### Sync performance

How long a sync takes to build the artifacts (server-side, the same hardware as the clone numbers). There's no git equivalent — git builds nothing ahead of time.

| repo | phase 1 (depth=1 clone-ready) | phase 2 (full history, background) |
|---|---|---|
| `facebook/react` | 5.4 s | +32 s |
| `oven-sh/bun` | ~8 s | +13 s |
| `torvalds/linux` | ~40 s | very large |

Phase 1 is what a `--depth 1` clone waits for; phase 2 runs in the background and only gates full clones. react's phase 2 is a cold first build; bun's is much shorter because the incremental re-sync reuses unchanged history levels and archive frames. linux's phase 1 is dominated by building the HEAD-closure pack for its ~95k-file tree, and its full history is large enough that we don't pre-build it on the dev box.

> In production the server syncs on push, so this happens once per commit, ahead of any clone — by the time a CI runner or agent asks for the repo, the artifacts are already built.
