# Adversarial Review — ripclone (origin/main, 2026-06-25)

> Historical review artifact. Some findings may be resolved or superseded; use
> current CI, docs, and issue tracking as the source of truth before treating a
> finding as open.

**Scope:** Rust source under `rust/src/`, reviewed at `origin/main` (`e6991f1`) in a fresh detached worktree (`/private/tmp/turbogit-adv-review`).
**Method:** The adversarial-review playbook (`intent/.intent/review/adversarial-review-playbook.md`) plus the Rust addendum (`rust-addendum.md`). Eight parallel track-specific reviews, then a second-pass verification of the highest-severity convergent findings against the actual code.
**Goal:** Real bugs at boundaries — lifecycle law, failure/crash paths, cross-process races, capacity semantics — not style.

The three top findings below were **independently surfaced by multiple track agents** and then **verified by hand** against the source (line refs confirmed). High convergence + direct verification = high confidence.

---

## Executive summary by risk boundary

| Boundary | Top risk | Key findings |
|---|---|---|
| **Metadata ordering / "newer never loses"** | **Critical** | The README's "a newer sync never loses to an older one" is enforced on **none** of: the *file* ref store (default when no S3), S3 *branch* refs, and is **racy (TOCTOU, no transaction)** on every SQL store. Surfaced by 3 independent tracks. |
| **Build/sync lifecycle & queue** | **High** | `finish`/`ack` is an unguarded `UPDATE … WHERE id=?` → double-settle after a time-based reclaim; no `attempts` column → a SIGKILL/OOM build crash-loops forever with no dead-letter; two-phase build acks `done` then fires phase 2 into a detached `tokio::spawn` that an ephemeral worker loses. |
| **io_uring writer (unsafe)** | **High** | On any harvest/submit error with windows in flight, `PendingDirectWindow` buffers (incl. kernel-written `statx`) are freed while the kernel still owns them → UAF/data-race with the kernel. The most safety-critical `unsafe` has no SAFETY note. |
| **Auth / multi-tenant trust** | **High** | No per-repo/per-tenant authz in the backend — one shared bearer token reads any tenant's cached artifacts and signs URLs for any repo; rate limiter keyed on raw socket IP (useless behind the gateway, bypassable over IPv6); private-vs-public TTL read from a client-trusted header that fails open. |
| **Storage / GC / retention** | **High** | GC grace period is anchored on object **mtime**, not reference time; a *reused* (not re-uploaded) artifact keeps its old mtime and can be deleted out from under a concurrent sync. Local-backend retention can delete the only copy of a still-referenced artifact. |
| **Clone correctness (files mode)** | **Medium** | Non-UTF-8 symlink targets abort the whole clone (forced through `str::from_utf8`); one decompression path is unbounded where its sibling is capped. Integrity model is otherwise strong (3 independent hash anchors). |
| **Pack / git-object correctness** | **Medium** | `--depth N` (N>1) is silently ignored → full clone, no `.git/shallow`; `HashingWriter` double-hashes on a short write → bad pack trailer; head-delta build has no fallback when its immutable base is gc'd after a force-push. |
| **Rust perf (the project's whole point)** | **Medium** | Per-artifact `.to_vec()` copy, per-frame `Vec` clone in the writer loop, and a `std::Mutex` locked once per file just to poll an error flag — all on the core download/extract hot path; all cheap mechanical fixes. |

A note on the auth findings: several are architecture/deployment assumptions (the backend trusts the gateway as the only authz layer). They are real but may be "by design, undocumented." Treat them as "document the trust boundary and add defense-in-depth," not necessarily "drop everything."

---

## Invariants violated

1. **"A newer sync never loses to an older one."** False on the file ref store; false on S3 branch refs; racy on all SQL stores; the read cache poisons even correct backends; the ordering signal is build wall-clock (cross-host, second-granular), not logical/commit sequence.
2. **Every sync settles exactly once and a crash-looping build eventually dead-letters.** No `attempts` bound, no terminal `failed` on hard-kill; `finish` is unguarded so two workers both settle.
3. **A claim is owned by exactly one worker until its window elapses.** Time-based reclaim with no liveness check reclaims a slow-but-alive worker → concurrent double-build on a shared mirror.
4. **Status never reports ready before artifacts are durable.** Two-phase acks `done` after phase 1; phase 2 is fire-and-forget.
5. **io_uring SQE buffers stay alive + pinned until their CQEs are reaped.** Holds on the success path; violated on every harvest/submit/Drop error path.
6. **GC never deletes a referenced or in-flight artifact.** Holds for freshly-*written* objects; violated for *reused* objects whose mtime is stale.
7. **A client may only read artifacts for repos it is authorized for.** No per-repo authz exists in the backend.
8. **The working tree is byte-exact vs `git checkout`.** Violated for non-UTF-8 symlink targets (clone aborts).

---

## Ranked top 10 fixes

1. **Make the ref-write ordering atomic and present on every backend.** Hoist the compare-policy into one shared helper; give `FileRefStore::save*` the read-compare-then-rename it lacks; make `SqlRefStore::save_branch` a single conditional upsert (`… DO UPDATE … WHERE excluded.synced_at >= refs.synced_at OR excluded.commit_id = refs.commit_id`; MySQL has no WHERE on `ON DUPLICATE KEY` — use `IF(...)` column expressions or `SELECT … FOR UPDATE`); route S3 branch saves through the same ETag CAS as HEAD. *(verified: ref_store.rs:126/188, meta/mod.rs:119)*
2. **Guard job settlement and bound attempts.** Make `finish`/`ack` conditional: `UPDATE … WHERE id=? AND worker_id=? AND status='claimed'`, check rows-affected; add an `attempts` column and route `attempts >= MAX` reclaims to terminal `failed` (dead-letter) instead of back to `queued`. *(verified: sqlite_db.rs:97/151 — and the sibling pg/mysql/libsql adapters)*
3. **Don't ack two-phase `done` until phase 2 is durable.** Run phase 2 inline before returning, or model it as a second retryable `queued` row that survives worker death; re-acquire `repo_lock` inside the phase-2 task. *(server.rs:4314-4357, ripclone-worker.rs:122-130)*
4. **Drain the io_uring ring before freeing buffers on every error/Drop path.** On any harvest/submit error with windows outstanding, loop `submit_and_wait`/drain the CQ to quiescence before dropping `PendingDirectWindow`; make `Drop` drain-to-quiescent, not best-effort flush; add the missing SAFETY note tying SQE-buffer lifetime to `pending_windows`. *(worktree_writer.rs:1486-1516, 1639-1648, 1710-1716, 1792-1796)*
5. **Anchor GC grace on reference time, not object mtime.** Touch/re-PUT (or copy-in-place) every artifact a sync references even when reused, or re-check each delete candidate's owning-ref version after computing the reachable set and skip anything that changed during the pass. *(remote_gc.rs:140-208)*
6. **Add a per-repo authorization check and stop fanning out a single shared token.** Carry a gateway-signed principal and authorize `(principal, repo)` before signing URLs / serving content; at minimum document that the backend must be network-isolated and never multi-tenant-shared. *(server.rs:581, 1525, 2685-2745)*
7. **Fix the rate limiter for the real topology.** Key on a validated forwarded-for / authenticated token, collapse IPv6 to /64; today it keys on the raw socket IP and is a no-op behind the gateway. *(server.rs:643-650)*
8. **Make the worktree writer durable (or document that it isn't).** No `fsync` on files or parent dirs before the clone reports success and writes the index stat cache → a crash can leave a torn tree that `git status` calls clean. Batch `IORING_OP_FSYNC` or fsync on the POSIX path. *(worktree_writer.rs, extract.rs index-stat path)*
9. **Handle the symlink and depth correctness gaps.** Build symlink targets from raw bytes (mirror the path handling) instead of `str::from_utf8` so non-UTF-8 targets clone; either implement real `--depth N` (the pack builders exist) or reject N>1 with a clear error instead of silently serving full history with no `.git/shallow`. *(worktree_writer.rs:750 / extract.rs:788; git-remote-ripclone.rs:101, mode.rs:96)*
10. **Cap the unbounded decompression path and stop the per-artifact / per-frame copies.** Use the bounded `Decompressor::decompress(_, raw_len)` in the no-dictionary branch (the one the clone actually uses); return `bytes::Bytes` from `fetch_artifact_*` instead of `.to_vec()`, borrow the fragment-pair map instead of `.cloned()`, and poll the writer error flag with an `AtomicBool`. *(extract.rs:433; client.rs:283, extract.rs:446/1603)*

---

## Full findings by track

### Track A — Build/sync lifecycle, exactly-once, queue
- **A1 [High] Hard-killed build crash-loops forever** — no `attempts` column; `reclaim_stale` unconditionally requeues; SIGKILL/OOM never reaches a terminal `failed`. *(verified sqlite_db.rs:97; schema has no attempts col)*
- **A2 [High, Critical for farm-out] Double-settle** — time-based reclaim of a slow-but-alive worker + unguarded `finish` (`WHERE id=?` only) → two workers build the same job on a shared mirror and both settle; last writer wins. *(verified sqlite_db.rs:151)*
- **A3 [High] Two-phase fire-and-forget** — job acks `done` after phase 1; phase 2 is a detached `tokio::spawn` with no durability tie; an ephemeral/serverless worker that exits after ack loses the full clonepack and nothing re-drives it; phase 2 also runs without `repo_lock`.
- **A4 [Medium] Coalescing onto an already-fetched (claimed) build drops the newer push** — `enqueue` coalesces onto `claimed` rows whose `git fetch` already happened, so the newer commit is never built until a future push.
- **Verified-safe:** credentials are never stored in the queue (no column; worker re-resolves via broker); `try_claim` is a correct atomic CAS; catchable panics do dead-letter.

### Track — Metadata ordering & cross-dialect parity (converges with A3-A6)
- **M1 [Critical] SQL `save_branch` is TOCTOU** — `get` then unconditional `upsert`, ordering decided in Rust between two awaits, no transaction/row-lock. *(verified meta/mod.rs:119)*
- **M2 [High] File ref store has no ordering guard at all** — default backend without S3; unconditional write+rename. *(verified ref_store.rs:126/188)*
- **M3 [High] S3 *branch* refs skip the ETag CAS** that HEAD uses → branch refs are last-writer-wins. *(ref_store.rs:393-404)*
- **M4 [Med-High] Ordering signal is worker wall-clock**, second-granular, compared `>` — cross-host clock skew and same-second ties reorder/ignore writes. *(server.rs:3816)*
- **M5 [Med] Caching ref store poisons its cache with the older ref** even when the durable store correctly kept the newer one. *(ref_store.rs:504-510)*
- **M6 [Med] MySQL VARCHAR caps vs TEXT elsewhere** — long branch/path/key errors on MySQL or silently truncates (queue key collision → dropped build). *(meta/mysql.rs, queue/mysql_db.rs)*
- **M7 [Med] Backend-selection footgun** — `RIPCLONE_QUEUE=postgres` + no S3 + unset metadata silently picks a *per-host* file ref store; shared queue, unshared metadata, no error. *(backends.rs:107-168)*
- **Verified-safe:** no SQL injection anywhere (all bound params; only static DDL is formatted).

### Track — Storage / signed URLs / GC / retention
- **S1 [High] GC grace anchored on mtime, not reference time** — reused (not re-PUT) artifacts keep old mtime and can be deleted under a concurrent sync; the authors already patched one instance (`head_base_packs`) but the general race remains. *(remote_gc.rs:140-208)*
- **S2 [High] Local-backend retention can delete the only copy** of a referenced artifact — protected set is a best-effort side file, not derived from the ref store; `is_durable` returns true when `durable_storage is None`. *(retention.rs:73-160)*
- **S3 [Med] Transient manifest-fetch error during GC silently shrinks the reachable set** (warn-and-continue while siblings abort) — latent today because chunks are also added from `RefInfo`, dangerous the moment any chunk becomes manifest-only. *(remote_gc.rs:237-246)*
- **S4 [Med] Whole-object in-RAM fetch, no range/resume; signed-URL TTL can be shorter than a large pack's download** → 403 → full restart, no progress; blows client memory. *(client.rs:263-300, server.rs:1508)*
- **Verified-safe:** freshly-written objects are protected by grace; downloaded content is hash-verified; CAS writes are tmp+atomic-rename.

### Track — Auth / credentials / multi-tenant trust
- **AU1 [High] No per-repo/per-tenant authz** — one shared `RIPCLONE_SERVER_TOKEN` reads any repo's refs/artifacts and signs any repo's URLs. *(server.rs:581, 1525, 2685-2745)*
- **AU2 [High] Rate limiter keyed on raw socket IP** — one global bucket behind the gateway; bypassable per-IPv6-address. *(server.rs:643-650)*
- **AU3 [Med] `visibility_is_private` fails open** — private TTL read from a client-trusted header, defaults to the long public window. *(server.rs:1674-1680)*
- **AU4 [Med] SSRF** — provider `clone_url` accepts arbitrary host + `http://`, no internal-address guard; server fetches it on sync. *(provider.rs:120-128, git.rs:1106)*
- **AU5 [Med] Header smuggling** — configured token / `auth_template` with CR/LF injects headers into upstream git HTTP. *(provider.rs:137-165, git.rs:1116)*
- **AU6/7/8 [Low]** — direct-artifact redirect ignores private TTL; token-file chmod-after-write TOCTOU with swallowed error; `auth_header` forces the `Authorization` name.
- **Verified-safe:** server token compared in constant time; OIDC verification is sound (RS256/JWKS/iss/aud/exp + repo binding); no credential logging; path-traversal into repo_root/cas-dir blocked.

### Track — io_uring writer / concurrency (unsafe)
- **U1 [High] Buffer freed while kernel owns it** on harvest/submit/Drop error paths (incl. kernel-written `statx`) → UAF/data race. *(worktree_writer.rs:1486-1516, 1639-1648, 1792-1796)*
- **U2 [Low] The most safety-critical `unsafe` (`push_multiple`) has no SAFETY invariant.** *(worktree_writer.rs:1710)*
- **U3 [Med] No fsync before reporting success + writing index stat cache** → crash leaves a torn tree git calls clean (POSIX and io_uring both, so consistent-but-non-durable).
- **U4 [Med, latent] `write_regular_batch_deferred` doesn't chunk to `MAX_BATCH_FILES`** → slot overrun if a caller ever passes >512 (all current callers cap at 512).
- **U5/U6/U7 [Low]** — normal-fd fallback close-after-submit race; divergent racy `safe_create_dir_all` copy in extract.rs; dead `write_entry` lacking `O_NOFOLLOW`.
- **Verified-safe:** completion accounting under SKIP_SUCCESS + IO_LINK is robust; short writes and error completions are caught, not swallowed; no shared mutable ring (thread-local); `user_data` packing can't collide.

### Track — Clone correctness (files mode)
- **F1 [Med] Non-UTF-8 symlink target aborts the clone** — `str::from_utf8(content)?` two functions from the raw-byte path helper. *(worktree_writer.rs:750, extract.rs:788)*
- **F2 [Low/Med] Unbounded decompression in the no-dictionary branch** (the one the clone uses) where the sibling is capped. *(extract.rs:433, manifest.rs:140)*
- **F3 [Low] No test covers symlinks / exec bit / non-UTF-8 names through the archive (files) path** — which is why F1 is invisible.
- **Verified-safe (strong):** three independent integrity anchors (per-chunk SHA-256, per-file SHA-1, manifest geometry); zip-slip blocked (`validate_relative_path`, `O_NOFOLLOW`, no-descend-through-symlink); mode allowlist; clone is temp-dir + atomic rename (crash-safe); empty/zero-byte boundaries covered.

### Track — Pack / git-object correctness
- **P1 [Med] `--depth N` (N>1) silently ignored** → full history, no `.git/shallow`; the depth-N builders exist but are unreachable. *(git-remote-ripclone.rs:101, mode.rs:96)*
- **P2 [Med] `HashingWriter` double-hashes on a short write** (hashes full `buf`, writes a possibly-short count; `write_all` re-hashes the remainder) → bad pack trailer → `index_pack` rejects, for blobs compressing >256 KiB. *(blob_pack.rs:380-384)*
- **P3 [Med] Head-delta build has no cold-base fallback** when the immutable base is gc'd after a force-push (contrast `build_history_tail`, which does fall back). *(server.rs:4032, pack.rs:309)*
- **P4 [Low] SHA-256 repos hardcoded to `Kind::Sha1`** → opaque index failure.
- **P5 [Low] LSM/history ranges trust stored level tips as ancestors** — force-push can yield incomplete/orphan history for deep clones (self-contained packs, so no missing-object error).
- **P6 [Low] Empty repo fails files-mode clone** (0-object pack → `index_pack` bails).
- **Verified-safe (strong):** head-pack disjointness is a true set-difference of depth-1 closures; shallow boundary correct; pack naming consistent server↔client; manifest published only after packs land (no torn publish); gix `index_pack` verifies every object id + pack checksum.

### Track — Rust performance & design (hot paths)
- **R1 [High impact] Per-artifact `.to_vec()`** copies every downloaded chunk/pack a second time; return `bytes::Bytes`. *(client.rs:283)*
- **R2 [High] Per-frame `Vec` clone of the fragment-pair list** in the writer loop; borrow the `Arc<HashMap>` directly. *(extract.rs:446, 1219)*
- **R3 [High] `std::Mutex` locked once per file to poll an error flag**; use an `AtomicBool` for the poll. *(extract.rs:1603)*
- **R4 [Med-High] Compressed bytes copied per frame** across the fetcher→writer channel; carry `(Arc, range)`. *(extract.rs:365, 1136)*
- **R5–R12 [Med→Low]** `clear_skip_worktree` rebuilds O(files) `String`s via lossy UTF-8; server build clones the full oid list several times (`pack.rs`); one tokio task spawned per chunk eagerly instead of `buffer_unordered`; `build_http_client` silently drops auth headers on error (`unwrap_or_else`); local-archive double copy; eager `gateway_url` `format!`; `&PathBuf` params; missing SAFETY note.
- **Not flagged (checked):** gix thread-local decode-buffer clones are required for correctness; `WorktreeWriter: Clone` is a ZST.

---

## Areas needing deeper review
- The in-process async `build_waiters` vs `LocalJobQueue` capacity (`server.rs:1937-1972`) — can a later coalesced waiter hang if the first enqueue returns `Full`?
- `CachingRefStore` holds the single cache write-lock across inner I/O on every load/save (`ref_store.rs:488-510`) — read-heavy contention.
- `FileRefStore::path` joins an unescaped `storage_key()` for the GitHub-default path — confirm repo-path input validation upstream rules out `..`/extra slashes (path-traversal).
- `extract_archive_streaming` (`extract.rs:1898`) verifies only length, not content hash, and uses the unbounded decode — confirm it is dead before relying on the SHA-1 net.
- `build_into_cas_bounded` prefix/suffix tiling math (`archive.rs:594`) — property-test bounded vs full build byte-for-byte.
- The middle of the io_uring window machinery (`worktree_writer.rs:1563-2380`) and the server HTTP handlers were only partially read by the perf track.

## Suggested tests (highest value first)
1. **Concurrency/ordering property test:** N tasks race `save_branch` with shuffled `synced_at` against each meta engine + `FileRefStore` + `S3RefStore` branch path; assert max-`synced_at` always wins (fails M1/M2/M3 today).
2. **Worker hard-kill:** claim, simulate SIGKILL (no ack), assert bounded attempts → `failed`; assert a reclaimed slow worker's late `ack` is rejected (A1/A2).
3. **Two-phase durability:** process exits right after phase-1 ack → assert a durable full clonepack exists and the job is retryable (A3).
4. **GC reuse race:** interleave a sync that re-references an aged reused object with `RemoteGc::run()`; assert the object survives (S1).
5. **io_uring fault injection:** force `submit_and_wait` to return `EINTR` with ≥2 windows in flight, run under KASAN/ASan; catch the UAF on `statx_buffers` (U1).
6. **Files-mode fidelity property test:** random tree (exec bits, symlinks incl. non-UTF-8 targets, empty files, chunk-boundary sizes) → clone byte-identical to `git checkout` (F1).
7. **Authz:** server with token T, request a repo the caller has no claim to → must be 403 (currently 200) (AU1); two IPv6 addrs in one /64 each exhaust the burst (AU2).
8. **Depth + short-write:** remote-helper `--depth 3` asserts history length + `.git/shallow` (P1); >256 KiB-compressing blob through `StreamingBlobPackBuilder` behind a short-writing `Write` (P2).

## Track coverage map
| Track | Findings |
|---|---|
| Build/sync lifecycle & queue | A1–A4 |
| Metadata ordering & dialect parity | M1–M7 |
| Storage / GC / retention | S1–S4 |
| Auth / credentials / trust | AU1–AU8 |
| io_uring writer / concurrency (unsafe) | U1–U7 |
| Clone correctness (files mode) | F1–F3 |
| Pack / git-object correctness | P1–P6 |
| Rust perf & design | R1–R12 |

The "newer never loses" failure was found independently by the lifecycle, metadata, **and** storage tracks — the strongest signal in this review. The "credentials never stored in the queue" claim was checked by two tracks and **holds**.
