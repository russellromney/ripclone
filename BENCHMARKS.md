# Benchmarks

Reproduce with [`scripts/benchmark_clone_compare.sh`](scripts/benchmark_clone_compare.sh)
(run on a client machine pointed at a ripclone server).

## Clone: ripclone vs native `git clone`

**Repo:** `oven-sh/bun` (15,771 commits, ~840 MB full `.git` from GitHub).
**Client:** Fly `performance-8x` (8 dedicated vCPU, 16 GB RAM), region `ewr`.
**Server:** Fly `ripclone-server-dev` (`iad`), artifacts in Tigris (`iad`/`sjc`).
**Target:** durable NVMe volume (`/data`) unless noted. Client artifact cache
disabled (`RIPCLONE_NO_CACHE=1`). Warm = server mirror already synced.

| clone | ripclone (warm) | native `git clone` | speedup |
|---|---|---|---|
| **depth=1** | **~1.0 s** | 4.0 s | ~4× |
| **full (depth=0)** | **~2.2 s** | 38.3 s | **~17×** |

Full ripclone clone (all 15,771 commits, durably written) is **faster than git's
shallow `--depth 1` clone**.

### Durable volume vs tmpfs (`--temp`), warm

| clone | volume `/data` (total) | tmpfs `--temp` (total) |
|---|---|---|
| depth=1 | 977 ms | 1029 ms |
| full | 2192 ms | 2051 ms |

`--temp` (tmpfs) is within noise on Fly NVMe (≤ ~6%). Pure-RAM staging doesn't
speed up the write, so **the clone write floor is CPU/syscall-bound (creating
~19k files + git index work), not disk-throughput-bound.** The lever for going
faster is fewer/cheaper file operations, not faster storage or io_uring.

### Cold vs warm (depth=1)

| phase | cold | warm |
|---|---|---|
| resolve | 2203 ms | 33 ms |
| write | 838 ms | 829 ms |
| **total** | **3296 ms** | **991 ms** |

The entire cold→warm delta is `resolve`: a stale mirror makes the server do a
`git fetch` to GitHub. In production the server syncs on push, so the mirror is
always fresh and `resolve` is ~30 ms. Even cold (3.3 s) beats `git clone
--depth 1` (4.0 s).

## Laptop over home wifi (network-bound)

Same repo, but cloned to a laptop (8-core Mac) over home wifi — i.e. the
download link, not git's server compute, is the bottleneck. This is the *worst*
case for ripclone and the fairest stress test.

| clone | ripclone | native `git clone` |
|---|---|---|
| depth=1 | 18.1 s | **16.3 s** |
| full | **90.9 s** | 148.4 s |

- **depth=1 is roughly tied (git slightly ahead).** Measured download is nearly
  identical — ripclone 70.8 MB packs (72.1 MB `.git/objects` incl. idx) vs git
  69.2 MB (~4% apart). At a single commit there is almost nothing to
  delta-compress, so the undeltified HEAD closure is barely larger. The tie is
  **latency, not bytes**: ripclone makes ~70 round-trips (resolve + manifest +
  34 pack + 34 idx GETs to the server/Tigris) where git does one negotiation +
  one pack stream. On a fat low-latency pipe those round-trips are nearly free
  (Fly), so ripclone's no-`index-pack` CPU win shows; on high-latency home wifi
  the round-trips roughly cancel it.
- **full is ~1.6× faster** — here it *is* bytes: 628 vs 840 MB (deltified
  history), plus no server pack-compute / client `index-pack`. That byte gap
  dwarfs the round-trip overhead, so the win survives the slow link.

**Takeaway:** ripclone's large wins are where git's *server-side* work dominates
and round-trips are cheap (CI runners, cloud dev boxes, fast/colocated networks —
see the Fly numbers above, 4×/17×). On a high-latency client, depth=1 is a wash
(round-trip-bound) while full still wins (byte-bound). Levers to also win depth=1
on slow links: fewer requests (a single concatenated head pack, or bundle
idx into the pack download) rather than 34+34 separate GETs.

## Why ripclone is faster

`git clone` makes GitHub **compute and stream a pack on demand** (delta
negotiation + compression server-side), then the client runs `index-pack`. That
dominates, especially for full clones (the 38 s). ripclone serves **pre-built,
content-addressed packs from object storage**, downloaded in parallel; the
worktree is hand-parsed from undeltified HEAD-closure packs and history is just
installed (no negotiation, no on-the-fly delta compression, no `index-pack`).
The expensive work happens once at sync time, not per clone.

## Honest caveats

- **Sync pays the cost.** The fast clone is amortized against a full build per
  sync (bun: ~1m40 first full build; the LSM incremental build makes *re*-syncs
  cheap). First sync of a big repo is still the price.
- **Tigris edge cache.** These runs had objects warm in the in-region Tigris
  edge. A first clone from a cold region pays an edge-cache miss (region warmers
  are on the roadmap). This is inherent to using object storage as the CDN; git
  has no equivalent client-facing cache.
- **Same-datacenter network.** Fly→Tigris is a fat pipe. A laptop on home wifi
  is bounded by its own download speed — but still skips git's server-side pack
  compute, so the win holds (smaller for depth=1, large for full).
- **No client-local artifact cache.** Verified: `~/.cache/ripclone` absent and
  `RIPCLONE_NO_CACHE=1` produces identical times — every clone fetches bytes
  from object storage over the network.
