# Adversarial Review — ripclone (2026-06-18)

Method: the honker adversarial-review playbook (`.intent/review/adversarial-review-playbook.md`),
adapted to ripclone's risk boundaries — a content-addressed clone backend whose inputs are
attacker-controlled repo contents and internet-facing HTTP, not a job queue. Seven tracks,
parallel deep review, second pass, then every Critical/High verified directly against source.
8,430 lines of Rust reviewed.

> Status: findings are **unfixed** as of this writing. This document records the hunt; nothing
> here has been remediated yet.

## Headline

The dangerous combination is real and live. In the default no-token config, an anonymous internet
caller can **read arbitrary server files** (`/v1/artifacts/..%2F..%2Fetc%2Fpasswd`) and **write
arbitrary server files** (a crafted `commit` becomes `git archive --output=`). Separately, the CAS
has no write-atomicity and no read-integrity check, so any interrupted write silently corrupts every
future clone and propagates to durable S3. And the extractor counts frames, not files, so several
malformed manifests produce a silently-empty working tree reported as success.

The root cause behind most Criticals is the one the playbook warns about: **the code assumes instead
of enforces.** It assumes the artifact id is a hash, the commit is a revision, the manifest is a
valid git tree, a written file is complete, and a stored object's bytes still hash to its name. None
are checked.

## Summary by risk boundary

| Boundary | State | Worst finding |
|---|---|---|
| **Server / auth / DoS** | Critical | Unauth arbitrary file read (path traversal in artifact id); fail-open auth |
| **Build pipeline (git subprocess)** | Critical | `commit`/`branch` arg-injection -> `git archive --output=` arbitrary write |
| **CAS / storage / retention** | Critical | Non-atomic write + no read-verify -> permanent silent corruption to S3 |
| **Clone correctness / extraction** | High | Frame-counting (not file-counting) -> silently empty tree reported `Ok` |
| **Path safety (untrusted manifest)** | High | Dir pre-creation escapes target; symlink-parent write escape; setuid modes |
| **Archive format / framing** | High | Empty files dropped at frame boundaries (real repos); manifest unvalidated |
| **FUSE / overlay / remote helper** | Medium | `read()` offset overflow panics and can wedge the mount; helper hang |

## Ranked top-10 fixes

1. **Validate artifact ids as `^[0-9a-f]{64}$` at the handler boundary** (and reject `/`, `\`, `..`
   in `Cas::object_path`). Closes unauth arbitrary file read. *(D1 — verified: cas.rs:18-24,
   server.rs:181-183)*
2. **Make `RIPCLONE_TOKEN` mandatory** (refuse to start / bind loopback if unset) — auth is
   fail-open today. *(D2 — verified: server.rs:218)*
3. **Validate `owner`/`repo`/`branch`/`commit` (safe charset, no leading `-`) and add
   `--end-of-options`/`--` to every git call.** One shared validator closes the `git archive
   --output=` arbitrary write and the `rev-parse --all` ref confusion. *(F1/F2/D5 — verified:
   git.rs:847, git.rs:260)*
4. **CAS: write to a temp file + `fsync` + atomic `rename`; drop the `path.exists()` short-circuit
   (or verify before trusting it).** *(E1 — verified: cas.rs:32-42)*
5. **CAS/S3: re-hash bytes against the requested hash on full reads; assert collected length ==
   Content-Length.** *(E2/E3 — verified: cas.rs:44, s3_storage.rs:76)*
6. **Extractor: after the collect loop, assert `files_written == manifest.files.len()`** and reject
   a manifest whose `frames` can't cover all fragments. Closes silently-empty/short trees.
   *(A1/B1 — verified: extract.rs:413-457)*
7. **Path safety: apply the `..`/absolute check before the directory pre-creation loop, and resolve
   the final path beneath `target_dir` with no symlinked parent** (openat2 `RESOLVE_BENEATH` on
   Linux, lstat-walk elsewhere). *(C1/C2 — verified: extract.rs:179-191, 462-471)*
8. **Validate `entry.mode` against the git-legal set** (0644/0755/symlink); never pass attacker bits
   through `& 0o7777` (setuid/setgid today). *(C3 — verified: extract.rs:514)*
9. **Rate limiter: key on the real socket peer IP, bound/prune the map.** It's keyed on the
   spoofable `Authorization` header today and grows unbounded. Also cap request bodies
   (`git-upload-pack` reads `usize::MAX`). *(D4/D6 — verified: server.rs:276, 424)*
10. **Retention: count both 40-char (SHA-1 blob) and 64-char (SHA-256) objects, and protect/confirm-
    in-S3 before eviction.** Blobs are invisible to retention today -> unbounded cache; eviction can
    race ahead of durability. *(E4/E5)*

## Full findings list

### Critical

- **D1 — Unauthenticated arbitrary file read via artifact path traversal.** `Cas::object_path`
  (cas.rs:18) does `root.join(&hash[..2]).join(hash)` with no validation; `{hash}`/`{sha}` route
  params (server.rs:179-183) reach it after percent-decoding, so `..%2F..%2F..%2Fetc%2Fpasswd`
  escapes the CAS root. S3 backend `key()` has the same issue (read outside prefix). Verified.
  LLM-pattern.
- **F1 — `commit` argument injection -> arbitrary file write.** `build_path_tar`
  (git.rs:842-851) places `commit` *before* `--`, so `commit="--output=/path"` makes `git archive`
  write a tar anywhere the server can write. `batch_files` passes `body.commit` unvalidated.
  Reproduced against a throwaway repo. Verified. LLM-pattern.
- **E1 — Non-atomic CAS write leaves truncated file under final content-addressed name.**
  `put_with_hash` (cas.rs:40) `fs::write`s directly to the final path; a crash/disk-full leaves a
  partial file, and the `path.exists()` short-circuit (cas.rs:34) prevents self-heal. Propagates to
  S3 and snapshots. Verified. LLM-pattern.
- **E2 — No read-integrity check.** `get`/`get_range` (cas.rs:44, s3_storage.rs:100) return bytes by
  name, never re-hashed. With E1, corruption is silent and permanent, served to every clone.
  Verified.

### High

- **D2 — Fail-open auth.** `auth_middleware` skips all checks when `token_hash` is `None`
  (server.rs:218); no warning logged. Makes D1/D5 unauthenticated in the default config. Verified.
- **D4 — Rate limiter is bypassable and unbounded.** Keyed on the attacker-controlled `Authorization`
  header, falling back to `"anonymous"` (server.rs:276); a fresh header value = a fresh bucket =
  unlimited rate, and the map (server.rs:70) is never pruned -> memory DoS. Comment claims "by client
  IP" but IP is never used. Verified.
- **D5/F3 — `owner`/`repo`/`branch`/`commit` unvalidated everywhere.** Plain `String`, no charset
  check (server.rs:90-125). Enables arbitrary/huge repo clone (disk/CPU DoS), `git rev-parse --all`/
  option injection (git.rs:260), and mirror-dir path traversal via
  `repo_root.join("{owner}_{repo}.git")`. Verified.
- **D6 — `git-upload-pack` body read with `usize::MAX` cap** (server.rs:424); no `DefaultBodyLimit`
  -> single request OOM. Verified.
- **A1/B1 — Extractor counts frames, not files -> silently empty/short tree reported `Ok`.** The
  collect loop runs `manifest.frames.len()` times (extract.rs:413) and never checks `files_written
  == files.len()` (extract.rs:454). An empty `frames` table, a trailing zero-length frame, or a
  zero-fragment entry yields a working tree missing files with no error. The **empty-file-drop on
  real repos** was reproduced (an empty file sorting after a large file, or an all-empty-blob commit
  -> builder writes a `frame_index` that's never flushed; archive.rs:187-194). Missing check
  verified.
- **C1 — Directory pre-creation escapes the target dir.** The pre-create loop (extract.rs:179-191)
  `create_dir_all`s `target_dir.join(path).parent()` for every entry *before* `write_entry`'s safety
  check (extract.rs:462). A `../../evil/x` entry creates dirs outside the clone. Verified.
- **C2 — Symlink-parent write escape on re-extraction.** `write_entry` checks the entry path but not
  whether a parent component is a pre-existing symlink; `create_dir_all` and `OpenOptions::open`
  follow it. A symlink `a -> /outside` from a prior extraction + a later `a/b` file writes outside
  target. Reproduced at syscall level. Missing check verified.
- **E3 — S3 stream collected without length check** (s3_storage.rs:76) -> early-EOF truncation
  cached under the real hash. **E4 — retention ignores 40-char SHA-1 blob objects**
  (retention.rs:259) -> unbounded cache. **E5 — eviction can race ahead of S3 durability**
  (server.rs:1170-1196) -> only-copy deleted.

### Medium

- **C3 — Arbitrary file mode incl. setuid/setgid/sticky.** `entry.mode & 0o7777` (extract.rs:514)
  trusts an unvalidated manifest field; `0o104755` -> a setuid file with attacker content. Verified.
- **A-track — No cleanup on partial clone failure** (client.rs install path): a failed extraction
  leaves a partial `.git`, and the "target already exists" guard then blocks retry; no
  temp-dir+rename, no atomic install.
- **A4 — Range responses not validated**: streaming fetcher accepts `200` (whole body) for a `Range`
  request and never checks response length vs requested (extract.rs:585-599); only SHA-1 saves it,
  with no defense-in-depth.
- **D3 — Non-constant-time token compare** (`token == expected`, server.rs:251) — timing oracle on
  the secret.
- **F4 — Non-default branches never fetched into an existing mirror** (git.rs:652) -> stale/missing
  ref served as valid. **F5 — no per-repo lock** -> concurrent sync corrupts the mirror dir.
- **G1 — FUSE `read()` offset overflow** (fusefs.rs:789): `off + size` on an attacker/huge `i64`
  offset panics (or wraps to an invalid slice) -> FUSE thread unwinds, mount wedges. **G5 —
  remote-helper hang**: `try_join!(stdin_to_child, stdout_to_parent)` (git-remote-ripclone.rs:128)
  can't cancel the stdin copy -> hung `git clone` when upload-pack exits early; stderr is
  `/dev/null`.
- **B3 — Manifest decode trusts all geometry** (manifest.rs:35): no bounds/monotonicity/index
  validation; release builds (`overflow-checks=false`) let `off+len` wrap past the guard -> slice
  panic on a corrupt manifest.

### Low (with the bug they guard noted)

- **G4** proxy forwards all client headers + open-relay to upstream (SSRF if exposed);
  `.expect("valid host header")` panics.
- **G6** remote helper replies `ok` to `depth`/`dry-run` options it ignores.
- **G7** `trim_end_matches(".git")` over-strips `repo.git.git`.
- **G2** `lookup(".git")` fabricates an entry without existence check.
- **G3** FUSE inode churn/leak on repeated `readdir`.
- **D7** rate-limiter `Mutex` poisoning can wedge the server.
- **E7** `get_range(len=0)` off-by-one differs between CAS and S3.
- **F6** empty `parent_commit` written to manifest at the shallow boundary.

## Suspected-but-proven-safe (recorded with the mechanism)

- Extractor **cannot deadlock**: every `done_tx`/`compressed_tx` clone drops on thread exit, so
  `done_rx.recv()` errors instead of hanging — this disconnect is the load-bearing safety net for
  B2/A8 (it converts an undercount into a loud, if misleading, error).
- `bounded(frames.len())` done-channel can't block (strict 1:1 send per frame, including the
  fetch-error path).
- Truncated/short range response *is* detected (`off+len > bytes.len()`, extract.rs:265); only the
  over-long/200 case (A4) is unhandled.
- Multi-fragment SHA assembly is correct (extract.rs:377-387). Multi-chunk reassembly and the
  `chunk_size` split are correct (verified with reassembly tests).
- `shell_escape` + `sh -c` in pack.rs only interpolate server-controlled temp paths — not
  attacker-reachable today (but fragile; recommend dropping `sh -c`).
- Subprocess exit-status handling is generally correct — the classic "`.output()` then use stdout
  ignoring `.status`" anti-pattern was **not** found.
- Retention evicts oldest-first correctly; the unsafe `statvfs`/`getuid` blocks in overlay/fuse are
  sound.

## Invariants violated

1. *A clone is byte-exact or fails loudly* — violated (A1/B1: silent empty/short tree).
2. *A stored artifact's bytes hash to its name; corruption is detected* — violated (E1/E2/E3).
3. *No filesystem object is created or read outside its boundary* — violated client-side (C1/C2) and
   server-side (D1).
4. *Attacker input never becomes a git option or shell token* — violated (F1/F2).
5. *Protected endpoints require auth; rate limits actually bound* — violated (D2/D4).
6. *Cache is reconstructable from S3 before eviction* — violated (E4/E5).
7. *Extracted modes are git-legal* — violated (C3).

## Areas needing deeper review

- **The skeleton/index install path in `client.rs`** (only lightly covered): does the prebuilt
  `.git/index` actually agree with materialized modes/content such that `git status` is clean across
  symlinks, gitlinks, and executable bits? Mode is never cross-checked against the index (C3/A-track).
- **`build_delta_skeleton_pack` and delta updates** consuming `parent_commit` (F6) — empty-string
  parent handling.
- **S3 multipart upload completeness** (`put` trusts `send() == Ok`; not visible whether the SDK
  switches to multipart).
- **axum percent-decoding semantics** for the `{hash}` capture across versions (confirm `%2F`
  reaches the handler as `/` — the D1 fix should not depend on this).

## Suggested tests

- **Property/fuzz the manifest**: random `FileEntry`/`FrameInfo`/`Fragment` tables -> extractor must
  either produce exactly `files.len()` files or return `Err` (never silent short tree). Seeds: empty
  `frames`, trailing zero-len frame, zero-fragment entry, non-monotonic `chunk_offset`, `frame_index`
  out of range.
- **Path-safety integration tests** (none exist today): manifest entries `../../x`, `a`(symlink)+
  `a/b`, pre-existing symlinked parent, `mode=0o104755`, NUL byte — assert nothing is created/read
  outside the target dir and modes are git-legal.
- **Server traversal/auth tests**: `/v1/artifacts/..%2f..%2fetc%2fpasswd` -> 400; unset token ->
  server refuses to start; `commit="--output=/tmp/x"` on `/batch` -> rejected; 10k unique
  `Authorization` headers -> bounded memory + 429.
- **CAS crash test**: `kill -9` mid-write, restart -> truncated object is detected/repaired, not
  served; corrupt one byte -> `get` errors.
- **Cron of git oracle**: build a commit older than mirror depth, and a moved-ref-mid-build race ->
  fails loudly, recorded commit == archived commit.

## Track coverage map

| Track | Scope | Findings |
|---|---|---|
| A — Clone correctness/extraction | extract.rs, client.rs | A1 (file-count), partial-clone cleanup, A4 (range validation), mode-not-authenticated, poisoned-mutex cascade |
| B — Archive format/framing | archive.rs, manifest.rs, pack.rs, clonepack.rs | B1 (empty-file drop, real repos), B2 (first-chunk offset latent), B3 (manifest unvalidated/overflow) |
| C — Path safety | extract.rs, git.rs, overlay.rs | C1 (dir-precreate escape), C2 (symlink-parent escape), C3 (setuid mode) |
| D — Server/auth/DoS | server.rs, metrics.rs, proxy | D1 (unauth traversal), D2 (fail-open), D3 (timing), D4 (rate-limit bypass+OOM), D5 (input validation), D6 (body cap), D7 (poison) |
| E — CAS/storage/retention | cas.rs, storage/, retention.rs, snapshot.rs | E1 (non-atomic), E2 (no verify), E3 (S3 truncation), E4 (blob retention), E5 (evict-before-durable), E6/E7 (TOCTOU/zero-len) |
| F — Build/git pipeline | git.rs, pack.rs, server build path | F1 (`--output` write), F2 (`rev-parse` injection), F3 (SSRF/path), F4 (stale branch), F5 (mirror lock), F6 (parent_commit) |
| G — FUSE/overlay/remote helper | fusefs.rs, overlay.rs, bin helpers | G1 (read overflow), G2-G3 (fuse lookup/inode), G4 (proxy SSRF), G5 (helper hang), G6-G8 (protocol/overlay) |

---

The four Criticals (D1, F1, E1, E2) and fixes #1–#4 are what to land before this is exposed to any
untrusted traffic. The two live remote-exploitable holes are the artifact-id traversal (D1) and the
`git archive --output` injection (F1).
