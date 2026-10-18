# Tags

Plan for one per-repo tags artifact that gives every clone the same tags
`git clone` gives. It replaces the per-output tags of #235.

## Goal

A ripclone clone has exactly the tag refs and tag objects that the
matching `git clone` would have at the same moment:

| ripclone clone | git clone it must match | git's rule |
|---|---|---|
| full (Ready) | `git clone` | every tag (see "Every branch and tag" below) |
| `--at <tag>` | `git clone --branch <tag>` | same |
| depth 1 | `git clone --depth 1` | tags whose target is in the clone: the tip, its tree or blobs, and tag chains on those |
| top-up | `git clone` of the target | same as full |

The full rows first matched `git clone --single-branch`, with the rule
below; "Every branch and tag" replaced them. The rule still applies to
depth 1, and to a full clone that could get neither the server's refs nor
the upstream's.

All of these come down to one rule, git's auto-follow: **a tag is
included when its fully peeled target is an object in the clone**. A
tag-of-tag chain comes along whole. For Ready clones, "an object in the
clone" is the same as "reachable from the commit", because the installed
packs hold exactly that set. A depth-1 clone's packs can hold more: Head
of a commit keeps the packs of the Head it was built on, with that older
commit and its tree. So a depth-1 clone keeps only the tags on its tip
and on the objects of its tip's tree, as `git clone --depth 1` does (G23).
A tag on any other commit drops by its target's kind, read from the
object header; the clone lists the tip's tree only when a tag on a tree
or blob is left, since that listing grows with the tree.

What #235 misses, and this plan fixes:

- A tag pushed after its commit was built. #235 baked tags into immutable
  manifests, so the tag waits for the next commit's outputs.
- Deleted and moved tags. Immutable manifests keep the old value forever.
- Tags on trees and blobs.
- A top-up target's own tags, such as a release tag pushed together with
  its commit.

Constraints:

- No server round trip added to the clone path ([[clone-path-latency]]).
- No history walk on the server per build.
- Nothing added to a build when the tags have not changed.

## Design

- **The mirror holds the tags.** The mirror's `refs/tags/*` holds every
  upstream tag whose target the mirror has.
- **One artifact per tag state.** Each distinct state of `refs/tags/*` is
  stored as one content-addressed artifact: the tag list plus the raw
  annotated tag objects. The control store keeps a per-repo pointer to the
  newest one.
- **The clone response points at it.** The ref response the client
  already fetches carries the artifact's signed URL. The client downloads
  it in parallel with the packs.
- **The client applies git's rule.** It keeps the tags whose peeled target
  exists in the installed objects. It writes their tag objects as one pack
  and the refs as `packed-refs`.
- **Top-up uses the same artifact.** The top-up fetch stays a single
  `--no-tags` request. The client filters the artifact after the fetch.

### How `refs/tags/*` in the mirror stays correct

1. **Commit fetch.** This is the existing `sync_bare_mirror_rev`, run with
   tag following on. In git 2.44, one fetch request then wants every tag
   whose target is local or about to arrive, and include-tag brings the
   tags on trees and blobs. Two flags matter:
   - `--atomic` writes `refs/ripclone/revs/<commit>` and the tags in one
     ref transaction. Without it, git writes the internal ref first and
     each tag in its own transaction afterwards.
   - An extra refspec that matches nothing, `refs/ripclone-none/*:refs/ripclone-none/*`,
     makes git send ref prefixes. The `ls-refs` listing then holds only
     `refs/tags/` instead of every ref. A bare-oid refspec alone sends no
     prefix, so the upstream lists all branches (and pull refs on GitHub).

   Cost: tag following adds one `ls-refs` round trip to each commit fetch.
   `--no-tags` with a bare oid sends `fetch` alone. #235 already pays this
   round trip, with the unfiltered listing.
2. **Tag push webhook.** Today these are ignored (`webhooks.rs:165`). Now
   one starts a refresh, which never admits a build.
3. **Poller.** The pass already runs `ls-remote` for HEAD per repo. The
   same call now also lists `refs/tags/*` (one request per repo, as now).
   The pass hashes that listing and compares the hash with
   `upstream_hash`. When the hashes differ, it starts a refresh with that
   listing.

A refresh, given the upstream listing (`name oid peeled`):

1. Keeps the entries whose peeled oid is in the mirror. The check is one
   `git cat-file --batch-check` process.
2. Compares them with the mirror's `refs/tags/*` to get the tags to add or
   move, and the tags to delete.
3. Does nothing more when nothing differs. That is the common case.
4. Otherwise, it takes the repo read lock, then the write lock, and:
   - runs `git fetch --no-tags --atomic --stdin origin` with
     `+refs/tags/<name>:refs/tags/<name>` for each tag to add or move. The
     targets are already local, so the transfer is only the tag objects.
     Step 1 confirmed this: three refs moved three tag objects (including
     a chain's inner tag), and no commits, trees or blobs;
   - deletes the tags to delete with one `git update-ref --stdin`;
   - publishes (below).

Tags whose target the mirror lacks are skipped. A later commit fetch that
brings the target also brings the tag through include-tag.

A commit fetch also writes tags, and its upstream view can be newer than
the refresh's listing. Each `RepoLock` counts commit fetches; when the
count changed between the listing and taking the write lock, the refresh
lists again (up to three times, then leaves it to the next poll). So a
refresh never deletes a tag a newer fetch wrote. Refresh requests for one
repo fold into the running one, which makes one more pass.

### Publish

`server::tags::publish` runs under a per-repo tags mutex, which is separate
from the mirror write lock. Publishes of one repo never interleave, and
each one reads the refs after the one before it, so the pointer only moves
forward. It runs in two places: in every Head build, and after every
refresh that changed refs. It:

1. Lists the refs with `for-each-ref refs/tags/` (name, oid, peeled) and
   hashes that output as `refs_hash`.
2. Stops when `refs_hash` equals the stored one. On a push with no tag
   change, this is the only added cost: one `for-each-ref`.
3. Otherwise, builds the artifact, uploads it to the CAS, and writes the
   pointer row.

**Head ordering.** When Head becomes ready, the artifact must already
cover the fetch that brought Head's commit. Otherwise a depth-1 clone of
a freshly tagged tip could miss its tag.
- Head calls `publish` once `ensure_commit` returns, alongside the rest
  of the Head build (`try_join`), so Head publishes only after it.
- `ensure_commit` returns only when `refs/ripclone/revs/<commit>` exists
  (`mirror_fetched_commit`), and only that commit's own fetch writes that
  ref. With `--atomic`, the ref and that fetch's tags land in one ref
  transaction. So the tags are there whenever Head goes on, and Head never
  waits on another commit's fetch.

## Artifact format

The artifact is a zstd-compressed proto message, at the repo's
compression level:

```proto
message TagsArtifact {
  repeated Tag tags = 1;       // sorted by name
  repeated bytes objects = 2;  // raw annotated tag objects, every member of
                               // every chain, sorted by object id
}
message Tag {
  string name = 1;    // without refs/tags/
  string object = 2;  // what the ref points at
  string peeled = 3;  // fully peeled target; empty for lightweight tags
}
```

- **Deterministic.** The artifact is a pure function of `refs_hash`, since
  objects are immutable by id. The same tags always give the same bytes
  and the same CAS id.
- **Built locally.** The tag objects are read from the mirror with gix.
  There is no history walk.
- **Stored in the CAS as is**, like the other artifacts. It is served
  through `/v1/artifacts/{hash}` or a signed URL.

## Control store

There is a new table:

```sql
CREATE TABLE repo_tags (
  repo          TEXT PRIMARY KEY,  -- RepoId::storage_key()
  artifact      TEXT NOT NULL,     -- CAS id of the TagsArtifact
  len           INTEGER NOT NULL,
  refs_hash     TEXT NOT NULL,     -- hash of the mirror's refs/tags listing
  count         INTEGER NOT NULL,  -- tags in it; 0 sends no URL
  upstream_hash TEXT NOT NULL      -- hash of the last upstream listing applied
);
```

`SCHEMA_VERSION` goes from 3 to 4 with no migration, as with #229. There
are no users yet, so a refused database is simply recreated. The
backend-conformance tests cover the new table.

A repo with no row gets no tags. That is today's behavior, so nothing
breaks before a repo's first publish. Existing mirrors have no tags,
because they were fetched with `--no-tags`. The poller's first pass after
deploy refreshes each repo and fills them in.

## API

`RefResponse` gains `tags_artifact` and `tags_artifact_url`. These are
optional and signed like the others. `ref_response` reads them with one
primary-key lookup alongside the build row. They apply to every response
kind: Ready, depth 1, and Pending with top-up. Older clients ignore them.
`docs/API.md` documents them.

## Client

1. Start the tags download alongside the packs, with the same downloader.
2. Once the packs are installed, open the object database with gix. Keep
   each tag whose peeled target is present, using an idx lookup; there is
   no walk. A lightweight tag's own oid is its target.
3. Validate everything before writing anything, as `write_tags` does
   today:
   - every name passes `validate_tag_name`;
   - every object's sha1 matches;
   - each chain, read from the objects' `object` lines, ends at `peeled`.

   Any failure fails the clone and writes nothing. The data comes from the
   server, so it is not trusted.
4. Write the kept chains' objects as one undeltified pack, indexed by `git
   index-pack`, and the refs as sorted `packed-refs` with no peeled lines.
   Loose objects are ruled out: linux would need about 900 files. Shipping
   the server's whole tag pack is ruled out too, because unused tags would
   show up in `git fsck` as dangling objects that `git clone` does not
   have.

**Top-up** (the base is Full(A) and the target is B):

1. Install Full(A), then fetch B as today: one `--no-tags` request for a
   bare oid.
2. Filter the artifact against the objects now present. Tags pushed
   together with B are in the artifact once the mirror has fetched B. That
   fetch is the first thing Head does, and top-up only happens while
   Full(B) is still building.
3. If A is not an ancestor of B (a force-push, using the existing
   `is_ancestor` check), keep only the tags that
   `for-each-ref --merged=B refs/tags/` lists, run over refs written
   straight to each kept tag's target. This replaces #235's rule of
   dropping every tag. The result is correct for shared history, and the
   walk runs only on this rare path.

Step 1 rejected using git's tag following on the top-up fetch. It does
give exactly `git clone`'s tags in a single `fetch` request. But it adds
an `ls-refs` round trip to the clone path, and with a bare oid that
listing holds every upstream ref.

**`--at <tag> --depth 1` fetch refspec.** This was handed over from the
status review. `git clone --depth 1 --branch v1` writes
`remote.origin.fetch=+refs/tags/v1:refs/tags/v1`, but ripclone writes the
branch glob. `POST .../builds` now answers `tag` when the selector named
one, and `write_git_config` writes the tag refspec for a shallow clone.

## What #235 loses

#235 has not merged. Reworking it on the same branch avoids shipping two
manifest fields and a Full pack kind that would then stay reserved in
immutable manifests for good.

- **Removed:**
  - `ClonepackManifest.tags` and `tag_objects`;
  - `TagRef`;
  - `FullResult.tag_pack`;
  - `PackBuilder::build_tags`;
  - `git::reachable_tags` (the `--merged` walk per Full);
  - `gix_util::tag_objects` and `gix_util::tags_at`;
  - the Head `tags_at` task;
  - the `history_idx` tag-pack chain;
  - the ancestor check in `materialize`.
- **Kept:**
  - the mirror fetch without `--no-tags`, plus `--atomic` and the
    tags-only prefix refspec;
  - `validate_tag_name`;
  - the validation in `write_tags` (it changes to write a pack);
  - the harness helpers `tag_refs`, `git_clone_tags` and `describe`;
  - the G14 (was G13) and E1–E3 assertions against `git clone`.

## Performance

| Path | Added cost |
|---|---|
| Clone, all modes | No added round trips: the URL rides on the existing ref response, and the download runs alongside the packs. CPU is a decode, N idx lookups, and writing a pack of the kept tags. Target: under 10 ms for 1k tags and under 100 ms for 100k tags. |
| Top-up clone | Nothing beyond the other modes. The fetch is still one request. |
| Commit fetch (Head path) | One `ls-refs` round trip, listing only `refs/tags/`. #235 already pays this with the listing of every ref. Measure it against `--no-tags`. |
| Build, no tag change | One `for-each-ref refs/tags/`, run alongside the Head build. |
| Build, tags changed | Build the artifact, upload it, write one row. Only for pushes that change tags. |
| Upstream | Per push, only the `ls-refs` above. A tag webhook costs one `ls-remote`, plus one fetch of tag objects only, and only when something differs. The poller folds tags into its existing `ls-remote`. |
| Storage | One CAS object per distinct tag state. #235's per-Full tag pack copied every reachable tag object into every Full, so this is smaller overall. Remote CAS objects are never deleted (`retention.rs` trims only the local cache), so old states stay. |

Measure before merge, on a scratch Hetzner box (cost approved separately)
or locally if it is fast enough:

- **Repos:** linux (about 900 signed annotated tags) and one repo with
  10k–100k tags.
- **Artifact size:** compressed, compared with the Head output.
- **Publish time**, when unchanged and when changed.
- **Client filter-and-write time.**
- **Commit fetch time**, with tag following (tags-only listing) and with
  `--no-tags`, on a repo with many refs.

If the artifact is a large share of small depth-1 clones, split the list
from the objects later. Not now.

### Measured (2026-10-17, local)

Synthetic bare repos made with `git fast-import` on this laptop, with the
crate at opt-level 2 (dependencies at 3, as in every build). "linuxish" is
2,000 commits and 1,000 annotated tags carrying a 700-byte random signature
each, like linux's signed tags; "many" is 5,000 commits and 100,000 plain
annotated tags. Warm runs:

| | linuxish (1k) | many (100k) |
|---|---|---|
| Artifact, zstd | 0.78 MB | 4.5 MB |
| Publish, no change (listing) | 1.3 ms | 60 ms |
| Publish, changed (build) | 17 ms | 740 ms |
| Client decode | 2.2 ms | 42 ms |
| Client keep (validate, look up targets) | 1.8 ms | 105 ms |
| Client write (pack, idx, packed-refs) | 3.0 ms | 60 ms |
| **Client total** | **7 ms** | **~210 ms** |

- That table's clones held most targets. A depth-1 clone holds almost
  none, and there gix's `has_object` was the cost: on every miss it
  rescans the pack directory. Synthetic artifacts with 1 target in 1,000
  present, 750-byte tag objects, crate at opt-level 3:

  | | 1k | 100k |
  |---|---|---|
  | Decode and check, while the packs download | 1.9 ms | 260 ms |
  | Look up targets, gix `has_object` (before) | 58 ms | 7.25 s |
  | Look up targets, gix with `refresh_never` | 2.6 ms | 350 ms |
  | Look up targets, the clone's idx files and one listing of loose objects (now) | 0.23 ms | 26 ms |
  | Write | 0.5 ms | 6 ms |

- **Both client targets are met on the clone's critical path:** after the
  last pack lands a clone spends about 0.7 ms (1k) or 32 ms (100k) on
  tags. Decoding and checking the artifact needs no clone objects, so it
  runs while the packs download. At 100k it is 260 ms of work that a
  clone of a repo that size does not wait on.
- **What got the client there:**
  - the client writes the idx itself, instead of running `git
    index-pack` (30 ms at 1k, 430 ms at 100k);
  - tag objects go into the pack as stored zlib streams, not deflated:
    one deflate per object cost 11 ms at 1k and 370 ms at 100k. They stay
    on local disk, and the download is the zstd artifact;
  - hex ids are parsed bytewise;
  - targets are looked up in the clone's idx files, not through gix,
    which checks the disk for a loose object on every miss; the refresh's
    lookups in the mirror use gix with `refresh_never`.
### Measured (2026-10-18, Hetzner ccx33 in Ashburn, torvalds/linux)

linux has 952 tags. A clone's tags match `git clone` in every mode
checked, and `git fsck` is clean:

- depth 1 at the untagged tip: 0 tags, as `git clone --depth 1`;
- `--at v7.3-rc7 --depth 1`: 1 tag, as `git clone --depth 1 --branch`;
- full: 948 tags, as `git clone --single-branch`. Plain `git clone` has
  4 more, because it fetches every tag and the history under it: two
  tags on side branches and two on a tree that no commit holds.

Tag following in the mirror's commit fetch, against `--no-tags`, from
the same mirror state (medians of 4 or 5 runs):

| Fetch | with tags | `--no-tags` |
|---|---|---|
| 25 commits, no new tag | 1.17 s | 1.06 s |
| 432 commits, one new tag | 2.41 s | 1.81 s |

- Without a new tag, the 0.11 s is upstream's work: the 952-tag listing
  (11 ms) and finding the tags to put in the pack. Every fetch of a
  repo with tags pays it.
- A new tag also costs a second step: git writes followed tags after the
  main fetch and first checks they are connected (0.15 to 0.25 s). Only
  a fetch that brings a new tag pays it.
- The bytes are the tag objects only: 580 bytes for one tag.

The tag steps on linux's real tags:

| | |
|---|---|
| Artifact | 293 KB zstd, 946 tag objects (556 KB raw) |
| Listing (every Head publish) | 8 ms |
| Build (only when tags change) | 72 ms |
| Client decode and check (during the download) | 2.3 ms |
| Client lookups: depth 1 / full | 13 ms / 41 ms |
| Client write: depth 1 / full | 0.06 ms / 1.6 ms |

The lookups cost more than in the synthetic runs because a real clone's
idx is large: a full linux clone's idx covers about 10M objects, and each
lookup faults pages of it in. A depth-1 clone of linux took 2.3 s in all.

## Freshness

The goal is to match `git clone` as of the last publish, in every mode. A
publish happens:

- **on the commit fetch**, so the tags pushed with a commit are there when
  Head becomes ready;
- **after a tag webhook**, one `ls-remote` plus one publish later;
- **at the latest after one poller interval** (default 300 s) when there
  are no webhooks.

A tag that upstream added, deleted or moved since the last publish is
missing or stale in the clone. Later fetches by the user do not correct
deleted or moved tags, because git's tag following never rewrites or
prunes an existing tag; plain `git clone` users live with the same thing.
This gap goes in `docs/FRESHNESS.md`.

## Tests

Run only the targeted tests locally; CI runs the suite.

- **Unit tests:**
  - artifact determinism: the same refs give the same bytes;
  - the client's validation rejects a bad name, a bad object hash, or a
    chain that does not end at `peeled`, and writes nothing;
  - the filter keeps only present targets.
- **G14 (was G13; keep, adapt):**
  - full, `--at v1`, depth 1 untagged, and depth 1 with a tagged tip are
    each compared with `git clone`;
  - a late tag on A (with no new commit) appears in a clone of the *same*
    commit after the webhook;
  - a deleted tag disappears, and a moved tag moves.
- **G15 (new):** a lightweight tag and an annotated tag on the tip's tree
  and on a blob. Full and depth 1 are compared with `git clone`.
- **E1–E3 (keep):**
  - the TopUp fixture parks every server fetch, so the mirror never gets
    B. E1–E3 keep comparing A's tags with `git clone`;
  - new E5: Head(B) builds, with `RIPCLONE_FULL_SLOTS=0` holding Full(B).
    A tag pushed together with B is in the top-up clone;
  - E3 adds shared history: tags on commits shared with B stay, and tags
    only on A go.
- **Webhooks:** `webhook_tag_push_is_ignored` becomes "a tag push
  refreshes tags and admits no build".
- **Poller:** a tag pushed with no webhook shows up after one pass.
- **C5** (manifests are immutable) stays as it is, since manifests no
  longer carry tags.
- **Mutation checks** (each failed a test when tried):
  - skip the client's tag install (G14);
  - skip top-up step 3 (E3);
  - skip the client filter (G14, G15);
  - skip the refresh's deletes (G14).

## Steps

All on `clone/tags`, replacing #235's per-output design:

1. **Done (git 2.44, file:// protocol v2):**
   - tag following on a bare-oid fetch gives `git clone`'s tags in a
     single `fetch`, but adds a full `ls-refs`;
   - a tags-only `fetch --stdin` moves only tag objects;
   - without `--atomic`, the internal ref is written before the tags. With
     `--atomic`, everything lands in one transaction;
   - an empty glob refspec limits `ls-refs` to `refs/tags/`.
2. **Server:** artifact format, the mirror fetch flags, `publish` in
   Head, the `repo_tags` table with schema 4, and the `RefResponse`
   fields.
3. **Refresh:** the webhook tag push, and the poller listing with hash
   comparison.
4. **Client:** download, filter, pack-and-refs write, top-up order.
5. **Remove** #235's per-output pieces. #235 never merged, so proto fields
   14 and 15 need no reservation.
6. **Tests and docs:** FRESHNESS, API, WEBHOOKS and CONTRACT_TESTS.
7. **Measure** (Performance), and update the PR description.

Steps 2–6 are done. Step 7 is measured locally (Performance); the network
and real-linux numbers wait on the Hetzner box.

## Every branch and tag, as plain `git clone`

**Status: implemented (2026-10-18), with the changes listed under "As
built" at the end of this section.** The owner ruled that a full ripclone
clone must match plain `git clone`, not `git clone --single-branch`:
someone porting a git command should get what git gives. On git/git
today a full clone lacks 7 of 9 remote branches (`maint`, `next`,
`seen`, ...) and 2 tags (`junio-gpg-pub` on a blob, `v1.4.4.5` on a
commit off `master`). The same holds for any repo with more than one
branch.

| ripclone clone | git clone it must match | refs |
|---|---|---|
| full, `--branch b`, `--at <ref>` | `git clone [--branch b]`, then checkout | every branch as `refs/remotes/origin/*`, every tag, with all their objects |
| depth 1, `--at <ref> --depth 1` | `git clone --depth 1 [--branch]` | unchanged: one branch, followed tags |

**Design.** The tags artifact becomes the repo's refs artifact:

1. **The mirror holds every upstream ref.** The refresh (any webhook
   push, or the poller seeing upstream's refs change) runs one
   `git fetch --atomic --prune origin +refs/heads/*:refs/heads/*
   +refs/tags/*:refs/tags/*` under the same locks as today. This replaces
   the tags-only `ls-remote` + `fetch --stdin` + `update-ref` steps. Build
   fetches stay as they are.
2. **The artifact gains the branches and a side pack.** Branches as
   `(name, commit)`, the tags as now, the default branch's tip D, and one
   pack and idx of the objects reachable from any branch or tag but not
   from D (`git pack-objects --revs` with `^D`). It is rebuilt when the
   mirror's refs change, never per build. On git/git the side pack is
   11.8 MB (3.5% of the 339 MB a full clone downloads), built in 1.0 s.
3. **Client, full clones.** The artifact downloads alongside the packs,
   as now. Once the packs (and any top-up) are in place:
   - **D is in the clone** (the common case: the clone is of D, or
     topped up past it). Install the side pack, write every branch to
     `refs/remotes/origin/*` with git clone's reflog line, and keep the
     tags as now; all of them are now present. Every ref is complete,
     since the clone holds D's history and the side pack the rest.
   - **D is not in the clone** (`--at` an older commit, or a Full that
     lags D with no top-up). Fetch the refs from upstream with git, as
     top-up does: correct, but one upstream round trip slower.
4. **Depth 1 is unchanged.** `git clone --depth 1` implies
   `--single-branch`.

**Cost.** No new round trip on the common path. The server builds one
pack per change of a branch or tag other than the default branch. A push
that only moves the default branch forward keeps the side pack (see "As
built").

**Tests.** A new G test: a repo with side branches, a tag on a side
commit and one on a blob, compared with `git clone` for `branch -r`,
tags and `fsck` in full, `--branch` and `--at` modes, and with a top-up.
Webhook and poller tests cover a branch push refreshing the artifact.
The trial scripts compare `git branch -r` with `git clone`'s.

**As built.**

- **Two artifacts, not one.** The tags artifact stays as it was. A
  separate branches artifact (`BranchesArtifact`: default branch, base D,
  and each branch's commit) and the side pack with its idx sit next to it
  in the `repo_refs` row (schema 6). Only a Full response carries the
  branches artifact and side pack URLs.
- **The side pack is kept while only the default branch moves on.** The
  row stores the commit the side pack leaves out (`side_base`) and a hash
  of the tips it completes: every branch but the default branch, and every
  tag. A refresh with the same tips, whose new D has `side_base` in its
  history, publishes the new branches with the old side pack: what that
  pack leaves out is still in D's history. A changed tip, or a force-push
  of the default branch, builds a new one (G24).
- **Who publishes what.** Head publishes the tags only, as before, so a
  ready Head still never points at older tags and pays no side pack
  build. The refresh publishes everything. It builds and uploads the
  branches artifact and side pack before it takes the publish lock, which
  a Head's tags publish waits on, so a Head never waits on a side pack
  (K6); refreshes of one repo run one at a time, so nothing else writes
  the branches meanwhile. A tags publish on a repo with no branches row
  yet starts a refresh, so a new repo gets its branches after its first
  build.
- **Refresh.** One `ls-remote` of `HEAD` names the default branch, then
  `git fetch --atomic --prune --no-tags origin +refs/heads/*:refs/heads/*
  +refs/tags/*:refs/tags/*` and `symbolic-ref HEAD`, under the mirror read
  and write locks; the publish runs under the read lock so compaction
  cannot delete the packs the side pack is built from. Every webhook push
  or delete of a branch or tag starts one; a default-branch push still
  admits its build first. The poller lists `HEAD`, `refs/heads/*` and
  `refs/tags/*` in its one `ls-remote` and refreshes when the hash of that
  listing differs from the published one. An upstream whose `HEAD` names
  no branch, such as a detached `HEAD`, leaves the mirror's `HEAD` alone,
  so the poller hashes its listing with the last published default branch
  and does not refresh on every pass (I15). Requests that arrive while a
  refresh runs fold into one more pass, which runs after a failed pass
  too; the failure is logged (K7). The tags-only `fetch --stdin` and
  `update-ref` steps and the fetch counter are gone.
- **Refs the store cannot read** are left out of a ready response, as
  before the first refresh, with a warning in the server log; the clone
  still comes, and fetches its refs from the upstream (H15).
- **Tag limits.** A tag more than 32 tag objects deep is left out of the
  artifact with a warning, where it once failed every clone (G21). The
  client refuses an artifact that lists a name twice (G22).
- **Client completeness check.** After the packs, any top-up and the side
  pack are installed, the refs come from the server when the clone holds
  D and every branch commit and tag target. Since the clone's objects are
  whole histories, that proves every ref complete. Otherwise one `git
  fetch` from the upstream of `+refs/heads/*:refs/remotes/origin/*
  +refs/tags/*:refs/tags/*` writes them. If the upstream or the network
  fails that fetch, the clone warns and keeps only its own branch and the
  tags it holds. If it fails on the user's machine (git does not start, a
  full disk, refs it cannot write), the clone fails and leaves nothing
  (G25).
- **The checked-out branch's remote-tracking ref** is the installed
  commit, even when the branches artifact is older.
- **A clone of another commit** (`--branch`, `--at`) does what `git clone
  --branch` does: it downloads every ref's history, then checks out its
  commit. A Full response of a commit other than D carries `refs_base`,
  Full(D)'s ready body, when Full(D) is ready (the base is kept in
  `repo_refs.branches_base`, schema 6). The client writes the working
  tree and index from its own Full's Head packs and metadata, installs
  Full(D)'s history packs and the side pack in place of its own history,
  and adds its own history only when its commit is on no ref, which the
  objects show. While Full(D) is not ready it takes the upstream fetch.
  A commit admitted from D's branch after D (a refresh not yet
  published) gets no `refs_base`: its own history holds D's. After a
  force-push such a commit does not hold D and takes the upstream fetch.

G14 runs its full clones, `--at v1` included, with an origin base that
does not exist, so a clone that fell back to the upstream fails it. G16
counts the upstream fetches of tag, branch and orphan-commit clones (none),
G17 covers Full(D) not ready, and G18 a lying refs base and a corrupt side
pack; skipping the side pack on
the server and skipping the remote branches on the client each failed it
when tried.

## Open

- **Tags on trees or blobs in top-up after a force-push.**
  `--no-merged` only sees commits, so such a tag could survive from A's
  history. This is rare; accept it and document it.
