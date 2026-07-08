# Safe garbage collection of durable storage

Status: **design.** `remote_gc.rs` exists and does the right reachability walk, but
its deletion timing can delete chunks out from under an in-flight clone, so it
ships **off by default**. This makes it safe to turn on.

## What GC does

Every build writes content-addressed chunks (clonepack manifests, packs, archive
frames) to durable storage (Tigris/S3). When a newer build supersedes an older
commit, that commit's unique chunks stop being referenced. GC reclaims them.

The reachability part is already correct (`remote_gc.rs::collect_reachable_hashes`):
walk every repo and branch, load each `RefInfo`, decode its manifests, and keep
every chunk they reference. Shared chunks survive; only fully unreferenced ones
are candidates. **We are not changing this.**

## The problem: deletion timing

Two gaps make it unsafe to enable (`remote_gc.rs`):

1. **The grace window keys off the object's mtime, not when it became
   unreferenced.** The delete check is `if entry.modified > cutoff { keep }` where
   `cutoff = now - grace`. So a chunk written long ago that *just now* lost its
   last reference already has an old mtime and is deleted on the very next pass.
2. **The grace is not tied to the signed-URL lifetime.** A clone is served
   presigned URLs valid for `REF_SIGNED_URL_TTL_*` (1200s public / 300s private).
   GC's grace (`RIPCLONE_REMOTE_GC_GRACE_SECS`, 24h) is configured separately;
   nothing guarantees `grace >= TTL`.

There is a partial mid-pass re-check today (re-collect the reachable set with a
fresh ref-cache read before deleting). It rescues a chunk that a *concurrent
build* re-references, but it does nothing for a **client already holding a signed
URL** — that client is not in the ref store.

### The bad case

1. A client starts cloning commit `A`; the server hands it signed URLs for `A`'s
   chunks, good for 20 minutes.
2. Someone pushes `B`; `B`'s build supersedes `A`; `A`'s unique chunks become
   unreferenced. Their mtime is old (written when `A` was first built).
3. GC runs, sees them unreferenced and past the mtime cutoff, and deletes them —
   while the client is mid-download.
4. The clone 404s partway through. From the user's view, ripclone corrupted a
   clone.

## Design: grace from "unreachable-since", floored by the URL TTL

Two changes, both small and local to `remote_gc.rs` + its config.

### 1. Count the grace from when a chunk was first seen unreferenced

Keep a small durable **orphan ledger**: `hash -> first_seen_unreferenced` (epoch
seconds), stored as one object in the same backend (e.g. `gc/orphans.json`), so
it survives restarts and is visible to whatever process runs GC.

Each GC pass:

1. Collect the reachable set (unchanged).
2. Load the ledger.
3. For every stored chunk:
   - reachable → drop it from the ledger (no longer orphaned) and keep it.
   - unreferenced and **not** in the ledger → add it with `now`, keep it (this is
     its first sighting; the grace clock starts now).
   - unreferenced and in the ledger with `now - first_seen >= grace` → **delete**
     it and drop it from the ledger.
   - unreferenced and in the ledger but younger than `grace` → keep it.
4. Write the ledger back.

So a just-orphaned chunk always gets a full grace window, regardless of how old
the file is. A chunk that becomes referenced again (re-pushed, or a build
publishes) is removed from the ledger and never deleted.

Keep the existing mtime check as a *second* guard, ANDed in: never delete a chunk
younger than `grace` by mtime either. This protects a chunk a build is writing
*right now* but whose ref hasn't published yet (it looks orphaned but is fresh).

### 2. Floor the grace at the signed-URL TTL

At startup, compute the effective grace as
`max(RIPCLONE_REMOTE_GC_GRACE_SECS, max(public_ttl, private_ttl))` and log it.
Read the same TTL constants/env GC-side so they can't drift apart. This guarantees
any client holding a still-valid presigned URL finishes before its chunks can go.

### 3. Then enable it

With the two above, GC is safe to run on a sane interval. Keep the default
conservative (e.g. hourly), `dry_run` honored, and the orphan ledger means the
*first* run after enabling never deletes anything (everything is freshly
tombstoned) — a built-in safety pass.

## Edge cases

- **In-flight build, ref not yet published.** Its chunks look orphaned. First pass
  tombstones them; the mtime guard also protects them (fresh). If the build
  publishes within grace, next pass sees them reachable and clears the tombstone.
  If the build was abandoned, they're collected after grace. Correct either way.
- **Re-push of a deleted commit.** If a chunk was deleted and the same content is
  re-uploaded later, it's a normal new object; the ledger entry was already
  dropped on delete. No special handling.
- **Clock skew / long clones.** The TTL floor covers normal clones. A clone that
  runs longer than the grace (huge repo on a slow link) is the one residual risk;
  size the grace above the realistic worst-case clone, not just the URL TTL.

## Multi-runner note

The ledger is read-modify-write of one object. With a single GC runner (the norm)
this is fine. Multiple server replicas each running GC would race on the ledger
and on deletes; gate GC to one runner (a leader lease) when that day comes. Out of
scope here.

## Config

| Env | Meaning |
|---|---|
| `RIPCLONE_REMOTE_GC_INTERVAL_SECS` | GC sweep interval; 0 = off (today's default). Set once the changes below are in. |
| `RIPCLONE_REMOTE_GC_GRACE_SECS` | Minimum age before an orphaned chunk is collected; floored at the signed-URL TTL. |
| `RIPCLONE_REMOTE_GC_DRY_RUN` | Log deletions without performing them. |

## Phasing

1. Add the orphan ledger + grace-from-first-seen (keep the mtime guard and the
   mid-pass re-check). Default still off.
2. Floor grace at the URL TTL, logged at startup.
3. Turn it on with a conservative interval; watch the metrics (chunks scanned,
   tombstoned, deleted, bytes reclaimed) before lowering the interval.

## Testing

- Orphan a chunk, run GC once → not deleted (freshly tombstoned); advance the
  clock past grace, run again → deleted.
- Orphan a chunk, re-reference it before grace, run GC → tombstone cleared, not
  deleted.
- A chunk with an old mtime but only just unreferenced is NOT deleted on the first
  pass (the core fix; would fail today).
- Grace is floored: set grace below the URL TTL → effective grace == TTL.
- Reachability unchanged: shared chunks and current-ref chunks always survive.
