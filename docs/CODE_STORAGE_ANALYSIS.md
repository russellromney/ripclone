# code.storage analysis

## What it is

`code.storage` (Pierre Computer Company) is an **API-first programmable Git
infrastructure layer built for machines / AI agents**. It is a hosted Git
service with native `git clone`/`push`/`fetch` endpoints, SDKs in TypeScript,
Python, and Go, and a sync engine for GitHub-backed repos.

Tagline: *"Off the shelf Git infrastructure for machines."*

---

## Core offering

1. **Hosted Git repos**
   - Create repos via API.
   - Get a `git clone https://name.code.storage/repo` remote.
   - No rate limits, no managing your own GitHub tokens for basic usage.

2. **GitHub Sync**
   - Create a repo with `baseRepo: { owner, name, defaultBranch }`.
   - GitHub is treated as the **source of truth**.
   - Code Storage mirrors upstream reads and forwards writes back to GitHub.
   - Webhook-driven near-real-time sync.

3. **Performance claims**
   - "60x faster clones than all r2/s3-based storage solutions."
   - Sharded distributed Git ref storage.
   - Replicated 3+ times.
   - Colocated near agents or self-managed on customer hardware.

4. **Warm / cold storage tiers**
   - Warm: $1.00/GB/month (touched in last 7 days).
   - Cold: $0.15/GB/month (untouched > 7 days).
   - Bandwidth: $0.06/GB in, $0.15/GB out.

---

## Feature set (from changelog)

| Feature | What it implies |
|---|---|
| GitHub Sync | Two-way mirror with GitHub as source of truth. |
| Improved Shallow Repo Support | Server preserves shallow boundaries; depth-limited workflows stay shallow. |
| Git Archive | Export repo snapshots as archives. |
| Ephemeral Branches | Short-lived branches for agents/CI. |
| CreateCommit Endpoint | Programmatic commit creation via API, not just git push. |
| File API Updates | Direct file read/write beyond raw git. |
| List Files With Metadata | Fast file listings without checking out the repo. |
| GREP | Server-side code search. |
| Git Fork | Programmatic forks. |
| Git Notes | Support for git notes. |
| Repo Explorer | Web UI for browsing. |
| Commit Signing Verifications | Verify signed commits. |
| Granular Branch Protection | Branch protection rules. |
| Squash Merge | Merge strategies. |
| Otel Analytics Endpoint | Observability / usage analytics. |

---

## Architecture inferences

From the marketing copy and features:

- **Distributed ref storage.** Branches/tags/refs are sharded and replicated,
  so reading a branch pointer is a fast, local-ish operation.
- **Object storage backend.** "60x faster than R2/S3" implies they use object
  storage but with a smarter access pattern than naive S3 (probably a hot local
  cache + index, plus their own ref storage).
- **Native git protocol.** They expose `git clone`/`push`/`fetch` endpoints,
  not just REST. This maximizes compatibility.
- **Programmatic API layer.** `createRepo`, `createCommit`, file API, grep,
  etc. are designed for agent/CI use cases, not human developers.
- **Webhook sync.** For GitHub-backed repos, changes propagate via webhooks
  rather than polling.

---

## What it gets right

1. **Positioning.** "Git infrastructure for machines" is precise. It targets
   AI agents, codegen platforms, CI, not human devs.

2. **GitHub Sync model.** Treating GitHub as source of truth while offering a
   faster mirror is exactly the right shape. Users don't have to choose.

3. **Native git endpoints.** Developers/agents can use existing git tooling.
   No custom client required.

4. **Warm/cold pricing.** Aligns incentives: active code costs more, archived
   code is cheap. Good fit for agents that churn through many repos.

5. **Agent-oriented features.** Ephemeral branches, shallow support, createCommit
   API, grep, file API — all built for automated workflows.

6. **Performance story.** "60x faster than R2/S3" is a clear claim. Even if
   the number is best-case, the direction is right.

---

## Gaps / opportunities

### 1. No obvious "current-files-only" fast path

Code Storage supports shallow clones and git archive, but there is no clear
"I just want the working tree + a 2 MB `.git`" endpoint optimized for agents.
A tarball/minimal-metadata endpoint could be even faster than their native
`git clone --depth 1` for the agent use case.

### 2. No clear cross-repo global deduplication

They mention sharded ref storage but not a global content-addressed object
pool across repos. If 1,000 repos share a vendored dependency, there's an
opportunity to store those bytes once.

### 3. Copy-on-write workspaces are not highlighted

Ephemeral branches exist, but "give me a writable fork of HEAD" as a
filesystem/workspace primitive isn't emphasized. This could be a major agent
feature.

### 4. No clear local-cache / edge story for agents

Colocation is mentioned, but a per-agent-runner object cache that warms across
jobs isn't obvious. For CI, this matters a lot.

### 5. Pricing may not favor tiny agent repos

Warm storage at $1/GB/month is fine for active repos, but agents often create
short-lived repos or forks. A model that optimizes for ephemeral, short-lived
workspaces could win.

### 6. GitHub auth is managed by them

Users grant Code Storage access. For enterprises that want to keep tokens in
their own infrastructure, a self-hosted or bring-your-own-cloud option is
needed (they mention "Managed Code Storage" but details are light).

---

## How this relates to our design

Our `GITHUB_INTEGRATION.md` design is conceptually very close to Code Storage's
GitHub Sync:

- GitHub = source of truth.
- Service = fast read cache + write proxy.
- Webhook/polling for updates.
- Tarball + minimal metadata for fast agent clone.

The main differences:

| Aspect | Code Storage | Our design (so far) |
|---|---|---|
| Primary interface | Native `git clone`/`push` + SDK API | HTTP API + tarball + minimal git metadata |
| Repo creation | Create repo on Code Storage, link GitHub | Point at any GitHub URL directly |
| Fast path | Shallow clone / git archive | Tarball + commit/tree metadata |
| Cross-repo dedup | Not emphasized | Global object pool |
| COW workspaces | Ephemeral branches only | Forked workspaces as first-class |
| Object storage | Optimized, but opaque | Content-addressed pool with hot/cold cache |

---

## Refined design direction

Given Code Storage exists, a competing or complementary service should probably:

1. **Stay GitHub-native.** Don't make users create a repo on our service first.
   Just accept `github.com/owner/repo` URLs. Lower friction.

2. **Offer both native git and snapshot endpoints.**
   - `git clone git.acme.dev/oven-sh/bun.git` for compatibility.
   - `POST /clone` returning a tarball + metadata for agents.

3. **Global object pool with local agent cache.** Cross-repo deduplication and
   per-runner warm caches.

4. **First-class copy-on-write workspaces.** Not just ephemeral branches, but
   "fork this commit into an isolated writable workspace" for each agent.

5. **Self-hostable / BYOC.** Let enterprises run the service on their own
   hardware/cloud with their own GitHub tokens.

6. **Price for ephemeral use.** Short-lived agent workspaces should be cheap
   or free; charge for bandwidth and long-term warm storage.

---

## Bottom line

Code Storage validates the market and the architecture. The right answer is not
"build a better GitHub" but "build a faster, programmable layer on top of
GitHub." The remaining differentiation is in friction (GitHub-native URLs),
speed (tarball/snapshot fast path), efficiency (global dedup + local cache),
and agent primitives (COW workspaces).
