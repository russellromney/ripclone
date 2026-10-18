# ripclone design

How ripclone turns a `git clone` into a parallel download from object storage. This is the deep dive; for the overview, clone modes, and the design principles, start with the [README](../README.md).

## Why a normal clone is slow

A normal `git clone` is slow because it does several slow things in a row. The client and server figure out which objects to send. The client unpacks them and git builds an index. Then git writes out every file. Each step is fine alone, but chained together they make a lot of round trips and disk work that is hard to overlap.

ripclone moves that work to the server, ahead of time. By the time you clone, the hard parts are done — the client just downloads the finished pieces and writes them to disk.

## Building a clonepack

On every push, ripclone mirrors the repo and builds a **clonepack** for `HEAD` so the clone is fast. A clonepack has three parts:

- **Manifest.** A small file listing everything else. The client grabs this first to know what to fetch.
- **Metadata chunk.** A ready-made `.git/index` and a table that says where each file lives, zstd-compressed. The client uses it to build `.git/` without running any git commands.
- **Content.** The objects and file bytes themselves, built three ways (HEAD packs, history packs, archive) so each clone takes only what it needs.

### The three content artifacts

ripclone builds the content three ways so each clone takes only what it needs:

***HEAD packs.*** Undeltified git packfiles with `HEAD`'s commit, trees, and every blob: a complete `--depth=1` repo. They are a base, shared by later commits, plus one small level per push that holds only the objects no earlier pack has. When the levels reach a quarter of the base, or 64 packs, Full rebases: it keeps each base pack that is still at least 90% live and packs the rest of the tree again (levels included) into a new base that later commits build on. A push that edits a few files re-uploads the packs those files were in, not the whole base. The client drops them into `.git/objects` and reads the working tree straight out of them as they land: no archive, no extra work. Because the objects live in `.git`, the git content commands work. An **editable** `--depth 1` clone installs just these.

***History packs.*** Every commit, tree, and blob, delta-compressed into at most 16 immutable levels, each one pack. A push adds a small level and merges the smallest neighbors, so earlier levels are reused. A full editable clone installs only these, each with its stored idx; it reads the HEAD packs only to write the working tree.

***Archive.*** The same `HEAD` file bytes, zstd-compressed and split into chunks. Each chunk is made of independent frames, so the client can download many at once and start writing files as the first bytes land. It's the fastest path to a working tree, but the files don't go into `.git`, so git content commands don't work. This is what **files** mode uses. A push keeps the earlier archive's chunks and adds the changed files as new tail chunks; a file that changed again leaves dead bytes behind. When the archive grows 25% past its compact form, or after 32 tails, the next push builds it compact again.

### Separate exact results

Each exact commit stores three independent results: **Head**, **Full**, and
**Files**. Head and the Files archive build at the same time from the
commit. Full history reuses Head's packs, so it starts once Head publishes.
Each publishes as soon as it is complete.
Head, Files and Full each have their own build slots, lease and attempt count,
so a Full never waits for its commit's Files, and a long Full never holds back
the next commit's Head or Files. A repository's Full results build one at a
time, in push order, so each can extend the history of the one before. A published result never changes,
and a later build never rebuilds one.

A depth-1 editable clone needs Head, a full editable clone needs Full, and
files mode needs Files. Each result is `pending`, `ready`, or `failed`.

### API

Two routes carry every build and clone. `POST /v1/repos/{provider}/{path}/builds`
resolves a selector (`HEAD`, a full commit SHA, or a branch or tag name) to one
commit and admits a build of it. `GET /v1/repos/{provider}/{path}/commits/{sha}?kind=head|full|files`
reads one result: ready with its artifacts, pending, or failed. The GET never
builds.

A clone POSTs once, then polls the GET for the commit it was given, so a
branch moving mid-clone cannot change what it installs. It uses the same GET
to refresh expired URLs. While Full is pending, the GET offers a ready Full of
an earlier commit (the branch's latest, or the parent's) as `top_up`. A full
clone installs that and fetches the rest with the user's own git from the URL
its own provider config gives, so it never waits on the server (`--wait` opts
out; see [Freshness](FRESHNESS.md)). Request and response shapes and error
codes: [API](API.md).

### Content-defined chunking and cheap re-syncs

The archive is split by content, not by fixed size: cut points land on the data itself (frames run about 1–16 MB). Each frame is compressed on its own and named by the hash of its bytes.

This makes re-syncs cheap. When a new commit lands, frames that didn't change hash to the same name and are reused as-is — no recompressing, no re-uploading. Only the frames that actually changed get rebuilt, and the builder reads just those changed regions, so the work matches the size of the diff, not the whole repo. The same is true of everything else a sync builds: only the commits and objects new since the last sync get packed, and re-syncing a commit that's already built does no work at all. A re-sync costs about what the diff costs.

Each result builds on the same result of its **base**: the newest commit of the repository with that result ready that is an ancestor of the commit being built, preferring commits pushed to the same branch. So a push of many commits, a merge, or a rewind costs the diff from the last built commit it contains, not just from its parent. A commit with no built ancestor, such as a force push onto unrelated history, builds from scratch. A non-ancestor is never a base, so a result never carries objects its commit cannot reach.

Clone and build timings are in [Benchmarks](BENCHMARKS.md).
