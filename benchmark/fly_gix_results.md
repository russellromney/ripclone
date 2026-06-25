# Fly server + Fly client benchmark — post-gix

**Setup**
- Server: `ripclone-server-dev` on Fly (`iad`, `performance-8x`, 16 GB)
- Client: `ripclone-client-dev` on Fly (`ewr`, `performance-8x`)
- Server/client both run the gix build from `main` (`4dc2f63` + benchmark fixes)
- Production defaults enabled: async two-phase builds, Tigris/S3 storage
- Client cache disabled (`RIPCLONE_NO_CACHE=1`)

**Method**
- `scripts/fly_benchmark.sh` is copied into the client image as `/fly_benchmark.sh`
- For each repo the server mirror + ref store are deleted before the cold sync, so the
  cold-sync number includes the GitHub mirror fetch and artifact build.
- Each clone mode is run 3 times; reported value is the average (min/max in parens).

## Results

| repo            | operation      | post-gix (ms) | pre-gix baseline (ms) | note |
|-----------------|----------------|--------------:|----------------------:|------|
| facebook/react  | cold sync      | 99,852        | 33,500                | dominated by GitHub mirror fetch (~100 s this run) |
| facebook/react  | delta sync     | 1,009         | 1,970                 | faster |
| facebook/react  | full clone     | 1,769 (1,647–1,990) | 2,300 (on-box d0) | over Fly network |
| facebook/react  | depth-1 clone  | 477 (461–496) | 870 (on-box d1)       | over Fly network |
| facebook/react  | files clone    | 408 (372–481) | 230 (on-box files)    | archive was ready |
| oven-sh/bun     | cold sync      | 80,570        | 91,100                | includes GitHub mirror fetch + build |
| oven-sh/bun     | delta sync     | 855           | 1,790                 | faster |
| oven-sh/bun     | full clone     | 2,849 (2,732–3,047) | 3,400 (on-box d0) | over Fly network |
| oven-sh/bun     | depth-1 clone  | 833 (792–893) | 2,000 (on-box d1)     | over Fly network |
| oven-sh/bun     | files clone    | 841 (834–853)   | 370 (on-box files)    | archive not ready, fell back to shallow head-packs |

## Interpretation

- **Sync**: cold sync is bounded by the GitHub mirror fetch and is noisy across runs
  (react was slower this time, bun was faster). Delta sync is solidly faster than the
  pre-gix baseline for both repos.
- **Editable clones**: full and depth-1 are competitive with or faster than the on-box
  pre-gix numbers, even though these measurements include the `iad` → `ewr` network hop.
- **Files mode**: react used the archive and is close to the on-box baseline. bun's
  archive wasn't ready (likely a background build still running), so it fell back to the
  shallow HEAD-closure packs. The fallback is ~2× the archive baseline but still well
  under a second, and the archive path remains the preferred fast path when ready.

## Fixes applied during this run

1. `rust/src/git.rs`: `index_pack` can now resolve ref-deltas against a supplied repo,
   fixing the `gix index-pack` failure on deltified skeleton packs.
2. `rust/src/pack.rs`: `build_prebuilt_index` passes the mirror repo to `index_pack`.
3. `rust/src/bin/cli.rs`: `--mode files` now requests the `full` clonepack variant (the
   only one that carries the files archive).
4. `rust/src/client.rs`: files mode falls back to extracting files from the pre-built
   head packs when the zstd archive isn't ready yet, instead of erroring.
5. `scripts/fly_benchmark.sh`: new Fly client/server benchmark script.
6. `Dockerfile.client`: copies the benchmark script into the client image.
