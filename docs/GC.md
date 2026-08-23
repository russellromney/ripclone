# Durable-storage garbage collection

Remote GC is enabled by default for remote storage and runs hourly. It is safe
for in-flight clones: reachability comes from durable exact results, and a
durable orphan ledger gives every newly unreachable object a full grace period
before deletion.

## What is retained

Each repository keeps immutable results keyed by exact commit. Publishing a
later commit does not change an older result's reachability. An exact result
remains reachable until exact-result retention evicts that particular result;
checkout names and moving branch tips are not reachability inputs.

For every added repository, GC enumerates its exact result commits, loads each
`RefInfo`, and decodes the referenced clonepack manifests. It retains all hashes
referenced by every retained exact result, including shared packs, metadata,
idx bundles, and archive frames. GC ignores checkout names and moving branch
pointers when deciding reachability.

## Safe deletion

GC stores `hash -> first_seen_unreferenced` in `gc/orphans.json` in the same
storage backend as artifacts. For each sweep:

1. It runs warm-result retention, which may mark idle exact results evicted.
2. It collects reachability from the remaining exact results.
3. It lists stored artifact hashes and reads the orphan ledger.
4. A newly unreferenced hash is tombstoned in the ledger and retained.
5. A previously tombstoned hash is deleted only when both its ledger age and
   object mtime exceed the effective grace period.
6. A reachable hash is retained and removed from the ledger if it had been
   tombstoned.

This means publication of B cannot remove A while A's exact result is retained,
including during a clone pinned to A. If A is later evicted, its former artifact
hashes still receive a new grace period before collection. A re-published or
otherwise re-referenced hash is removed from the orphan ledger.

The effective grace is floored at the longest public/private signed-URL TTL, so
a client holding a valid URL has at least that long before its artifact can be
deleted. The object-mtime check is an independent second guard for artifacts a
build has uploaded before publishing their exact result.

## Configuration

| Env | Default | Meaning |
|---|---:|---|
| `RIPCLONE_REMOTE_GC_INTERVAL_SECS` | `3600` | Sweep interval. `0` disables the background GC task. |
| `RIPCLONE_REMOTE_GC_GRACE_SECS` | `86400` | Minimum orphan age before deletion; raised to the signed-URL TTL floor. |
| `RIPCLONE_WARM_TTL_SECS` | `604800` | Idle time before exact-result artifacts are eligible for eviction. |
| `RIPCLONE_REMOTE_GC_DRY_RUN` | `false` | Report would-delete objects without deleting them. |

GC is skipped for local storage. On remote storage it logs each sweep's scanned,
reachable, tombstoned, deleted, and reclaimed-byte totals. Run only one GC
leader per storage backend: the orphan ledger is a single read-modify-write
object and is not a multi-writer coordination protocol.

## Operational checks

- Start with `RIPCLONE_REMOTE_GC_DRY_RUN=1` when validating a new deployment.
- Watch tombstoned and deleted counts before reducing the grace or warm TTL.
- Keep the grace above realistic clone duration as additional operational margin;
  the signed-URL floor is the minimum correctness bound.
