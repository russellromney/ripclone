# Adversarial review: ripclone product plan

This doc tries to break the `PRODUCT_PROPOSAL.md` plan by walking real user
flows, questioning the update/sync model, and surfacing risks. The goal is to
find the simplest version that still wins.

---

## 1. User flow: first clone

**Plan:** Agent runs `ripclone clone https://github.com/oven-sh/bun.git`. The
service returns a tarball URL + tiny metadata pack.

**What actually happens on a cold cache:**

1. Service receives request.
2. Checks GitHub App permissions.
3. Finds no cached tarball.
4. Fetches `refs/heads/main` from GitHub.
5. Fetches commit + tree + all HEAD blobs from GitHub (this is basically a
   shallow clone).
6. Builds a tarball.
7. Uploads tarball to CDN/object storage.
8. Builds a metadata pack (commit + tree).
9. Returns URLs to agent.
10. Agent downloads tarball + metadata.

**Problems:**

- **Cold-start is slower than raw GitHub.** Steps 4–8 add latency on top of the
  underlying GitHub fetch. The first user pays for everyone else's cache.
- **Tarball build is CPU/disk heavy.** For a repo like bun (223 MB working
  tree), gzip + upload is not instant. If synchronous, the agent waits 10–30s.
  If async, the agent gets a "not ready yet" response.
- **Private repos make this worse.** The service can't proactively mirror
  private repos; it must fetch on first authorized request.

**Mitigation:** Pre-warm popular public repos. For private/unknown repos,
accept that cold-start is slow and highlight the repeat-clone win.

---

## 2. User flow: repeat clone (the happy path)

**Plan:** Tarball is cached. Agent gets URL immediately.

**What can go wrong:**

- **Branch moved since tarball was built.** If a push happened 30 seconds ago
  and the cache hasn't refreshed, the agent clones stale code.
- **Webhook missed.** GitHub webhooks are not guaranteed. A missed push means
  stale cache until polling catches up.
- **Polling is too slow or too expensive.** Polling every active repo every
  few seconds burns GitHub API quota. Polling every minute means stale data.
- **Agent commits on stale base.** Agent builds a commit on top of `C`, but
  GitHub is at `C'`. Push fails with non-fast-forward.

**This is the central risk of the whole design:** the service trades freshness
for speed. If agents push, they must handle non-fast-forwards. That's normal
in git, but the service makes it more likely.

**Mitigations:**
- Always fetch latest ref from GitHub before a push.
- Use force-with-lease semantics (`expected_commit` in request).
- Offer `strict` clone mode that verifies ref freshness.
- Polling + webhooks + on-demand refresh as layered fallback.
- Make the CLI handle 409 by fetching latest and rebasing.

---

## 3. Update/sync model

### 3.1 Webhooks

**Plan:** GitHub App webhook pushes notify the service of changes.

**Failure modes:**

- Webhook not configured by user.
- Webhook URL behind firewall / not reachable (common in self-hosted).
- GitHub delays or drops webhooks.
- Duplicate webhooks cause redundant work.
- Webhook secret mismanagement.
- Force-push events need special handling.
- Branch deletion events need cache invalidation.

**Verdict:** Webhooks are necessary but not sufficient. Need polling fallback.

### 3.2 Polling

**Plan:** Poll `git ls-remote` for active repos.

**Failure modes:**
- Rate limits. `ls-remote` against many repos consumes quota.
- Active repos change frequently; quiet repos waste polls.
- Can't poll private repos without a token.

**Verdict:** Polling is a fallback, not a primary strategy.

### 3.3 On-demand refresh

**Plan:** On clone request, check ref age; if stale, refresh.

**Failure modes:**
- Makes the clone slow again.
- If many agents hit simultaneously, thundering herd to GitHub.
- Doesn't help if the agent wants strict freshness.

**Verdict:** Best combined with webhooks/polling, not alone.

### 3.4 Recommended sync model

A layered approach:

1. **Webhooks** for near-real-time updates.
2. **Polling** at low frequency for webhook misses.
3. **On-demand refresh** with short staleness budget.
4. **Agent-specified commit SHA** for reproducibility.

The service should be honest about staleness. Return `cached_at` and
`expected_fresh_until` in clone responses.

---

## 4. How agents actually use it

### 4.1 Simple edit → commit → push

**Flow:**
1. `ripclone clone <repo>`
2. Edit file.
3. `ripclone commit -m "fix"`
4. `ripclone push`

**Problems:**
- If step 4 fails due to stale base, the agent must recover. Most agent
  frameworks don't handle git rebase/merge well.
- The agent has no history, so it can't reason about recent changes.
- If the agent needs to see `git log` or `git blame`, it can't.

**Verdict:** Works for simple additive edits. Breaks for anything requiring
context or concurrent modification.

### 4.2 Multi-file changes

**Flow:**
1. Clone.
2. Edit many files.
3. Commit all.

**Problems:**
- `write-tree --missing-ok` handles new/deleted files fine if they are staged.
- But if the agent forgets to stage a deletion, the new tree still references
  the old blob (which is missing locally). This is actually correct behavior —
  the file is unchanged.
- Large commits with many new blobs require sending many objects to the
  service.

**Verdict:** Works, but the commit payload can be large.

### 4.3 Running tests / builds

**Flow:**
1. Clone.
2. Run `npm install` / `cargo build` / etc.

**Problems:**
- Tarball extraction preserves file content but may not preserve mtimes the
  same way `git checkout` does.
- Build systems may see all files as "new" and do full rebuilds.
- Symlinks and executable bits must be preserved correctly.

**Verdict:** Need to test and possibly normalize mtimes/permissions after
extraction.

### 4.4 Submodules

**Flow:**
1. Clone parent repo.
2. Agent needs files from submodule.

**Problems:**
- GitHub tarballs don't include submodules.
- Our tarball must either include submodules or omit them.
- Including them requires recursive fetching and tarball building.

**Verdict:** Submodules are a v2 feature. v1 should explicitly not support
 them or only support shallow inclusion.

### 4.5 Large files / Git LFS

**Flow:**
1. Clone repo with LFS files.
2. Agent needs actual file content.

**Problems:**
- Tarball includes LFS pointers, not actual files.
- Agent must run `git lfs pull` separately, which fetches from LFS server.

**Verdict:** LFS not supported in v1. Document limitation.

---

## 5. Security risks

### 5.1 Token storage

The service must hold GitHub App installation tokens or user tokens. If
compromised, attackers can read private repos and push code.

**Mitigations:**
- Encrypt tokens at rest.
- Use short-lived tokens (GitHub App tokens rotate hourly).
- Least privilege: `contents:read` for clone, `contents:write` for commit.
- Never log tokens.

### 5.2 Snapshot access

Tarball URLs must not be guessable or shareable. Signed URLs with short TTL
help, but the underlying storage must not be public.

**Mitigations:**
- Store private tarballs in non-public buckets.
- Proxy tarball downloads through the service for private repos, or use
  single-use signed URLs.
- Scope tarball access to the requesting token/installation.

### 5.3 Cache poisoning

If an attacker can inject a bad tarball or object, agents get corrupted code.

**Mitigations:**
- Verify tarball against known commit SHA.
- Content-addressed objects are self-verifying by hash.
- Sign tarballs or include checksums.

### 5.4 Supply chain

Agents pushing through the service could introduce malicious code. The service
is just a conduit, but it becomes part of the trust boundary.

**Mitigations:**
- Audit logs of every push.
- Optional commit signing verification.
- Optional require-signed-commits for protected branches.

---

## 6. Cost and operational risks

### 6.1 Hot storage is expensive

Keeping unpacked repos on NVMe sounds fast but is costly. A repo like bun is
~300 MB with `.git`. Thousands of repos add up.

**Mitigations:**
- Only keep hot what is actually accessed.
- Aggressively tarball and evict after inactivity.
- Use cheaper warm storage (compressed on local disk) before cold.

### 6.2 Bandwidth costs

Every clone downloads a full tarball. For CI doing thousands of clones per day,
bandwidth can dominate cost.

**Mitigations:**
- Agent-side object cache so repeat/related clones share bytes.
- Differential tarball updates.
- COW workspaces so agents don't download at all.

### 6.3 Compute for tarball generation

Building tarballs is not free. For every push to a hot repo, the service may
rebuild a tarball.

**Mitigations:**
- Build tarballs asynchronously.
- Only build for refs that are actually cloned.
- Defer cold-repo tarball building until requested.

### 6.4 GitHub rate limits

If the service is popular, it will hit GitHub API and git protocol limits.

**Mitigations:**
- GitHub App for higher limits.
- Cache aggressively.
- Enterprise customers use their own rate limit pool.
- Self-hosted option bypasses shared limits.

---

## 7. Biggest flaw in the plan

The plan tries to do too much in the MVP:

- GitHub App auth
- Object pool
- Tarball cache
- Cold eviction
- Commit proxy
- CLI
- Webhooks + polling
- Cross-repo dedup
- COW workspaces

That's months of work. The risk is building infrastructure before proving the
 core value proposition.

**A simpler, safer MVP:**

1. **No object pool.** Just proxy/fetch GitHub's own tarball and add a tiny
   metadata pack.
2. **No cold eviction yet.** Cache tarballs locally for N hours/days.
3. **No COW workspaces yet.** Just fast clone + commit.
4. **Commit via GitHub API or git push with user token.** Don't over-engineer.

This proves the core loop: *tarball clone is faster than git clone, and we can
commit back.*

Once that's proven, add:
- Object pool + cross-repo dedup.
- Cold eviction.
- COW workspaces.
- Webhook-driven updates.

---

## 8. What code.storage's actual model teaches us

Their insight is correct: **hot unpacked, cold tarballed, evicted after
inactivity.** But they bury it behind marketing about distributed ref storage.

Our opportunity is to make that model:
- **Transparent:** users understand exactly what's happening.
- **GitHub-native:** no repo creation, no migration.
- **Agent-optimized:** tarball + minimal metadata + commit proxy.

We don't need to out-infrastructure them. We need to out-simplify them for the
GitHub-backed agent use case.

---

## 9. Revised MVP recommendation

### Must-have for v0

1. GitHub App auth.
2. `POST /clone`:
   - Accept GitHub URL.
   - Fetch GitHub tarball + HEAD commit/tree.
   - Return tarball URL + metadata pack.
   - Cache tarball locally.
3. `POST /commit`:
   - Accept new commit + tree + new blobs.
   - Verify with GitHub (auth + current ref).
   - Push to GitHub via git protocol.
4. CLI: `ripclone clone`, `ripclone commit`.
5. Basic webhook/polling ref refresh.

### Explicitly out of v0

- Global object pool / cross-repo dedup.
- Cold eviction to S3.
- COW workspaces.
- Submodule support.
- LFS support.
- Multi-region.

### Success metric

`ripclone clone` of `oven-sh/bun` is consistently under 10 seconds end-to-end
(cold) and under 5 seconds (warm), vs ~18 seconds for `git clone --depth=1`.
Commit round-trip is under 3 seconds.

---

## 10. Conclusion

The product direction is right, but the initial plan is too ambitious. The
biggest risks are freshness/staleness, cold-start latency, and security/token
management. The winning move is to build the smallest version that proves the
core loop — fast GitHub-backed clone + commit — and then layer on the
infrastructure that makes it cheap at scale.
