# Product proposal: ripclone — GitHub-native fast clone for agents

## The insight from code.storage

`code.storage` markets itself as distributed sharded ref storage, but the
actual mechanism is simpler and smarter: **keep hot repos unpacked, tarball
them after ~7 days of no access, push the tarball to cheap cloud storage, and
delete from the hot cache.**

That is the right economic answer. Hot code needs low latency; cold code just
needs to be cheaply retrievable. Agent workflows typically touch a repo once
and then forget it, so an explicit warm/cold model fits well.

## What we should build

A **GitHub-native fast clone and commit proxy for AI agents**.

No new repo hosting. No "create a repo on our service first." The user points
at any GitHub URL and gets a faster clone path. Commits still land on GitHub.

Working name: **`ripclone`** (placeholder).

## Core promise

> Clone any GitHub repo in tarball-download time, commit back without fetching
> old blobs, and let the service handle GitHub rate limits and packfile
> negotiation.

## Important caveat: cold vs. warm

- **Cold clone** (first time the service sees a repo) is *slower* than raw
  `git clone --depth=1` because the service must fetch from GitHub, build a
  tarball, and cache it.
- **Warm clone** (repo already cached) is the win: the agent downloads a
  pre-built tarball from the service's cache instead of negotiating a packfile
  with GitHub.

The product is valuable for:
- Agent platforms doing many clones of the same repos.
- CI systems with redundant clones.
- Any workload where the cache hit rate is high.

## Target user

- AI coding agents and agentic frameworks.
- CI/CD systems doing many redundant clones.
- Teams that want faster repeat clones without migrating off GitHub.

## v0 MVP architecture

v0 is intentionally minimal. It proves the core loop: fast warm clone + commit
back to GitHub. Everything else is deferred.

### 1. GitHub App auth

Users install a GitHub App on their orgs/repos. The service receives
installation tokens and acts on behalf of the user.

- Read permission → clone/fetch.
- Write permission → commit/push.
- No user PATs to manage.

### 2. Local disk cache

```
data/cache/
  repos/<owner>/<repo>/
    refs/<branch>            # cached ref + timestamp
    tarballs/<commit>.tar.gz # working tree export
    metadata/<commit>.pack   # commit + tree objects
```

No S3, no CDN, no object pool in v0. Just local disk.

### 3. Fast clone endpoint

```
POST /v1/clone
{
  "repo": "oven-sh/bun",
  "branch": "main",
  "staleness": "30s"   // optional, default 30s
}
```

Service:

1. Checks GitHub App read permission.
2. Looks up cached ref.
3. If missing or older than `staleness`, fetches `refs/heads/main` from GitHub.
4. If tarball is missing/stale, fetches GitHub's tarball + commit + tree.
5. Builds metadata pack (commit + tree).
6. Caches tarball and metadata.
7. Returns local URLs.

Response:

```json
{
  "ref": "refs/heads/main",
  "commit": "df55ab7...",
  "tree": "abc1234...",
  "tarball_url": "http://localhost:8000/cache/oven-sh/bun/tarballs/df55ab7.tar.gz",
  "metadata_url": "http://localhost:8000/cache/oven-sh/bun/metadata/df55ab7.pack",
  "cached_at": "2026-06-15T21:00:00Z",
  "fresh_until": "2026-06-15T21:00:30Z"
}
```

Agent:

```bash
curl -fsSL "$tarball_url" | tar -xz
git init
git remote add origin https://github.com/oven-sh/bun.git
curl -fsSL "$metadata_url" | git unpack-objects
git read-tree HEAD
```

### 4. Fast commit endpoint

Agent edits files and creates new objects without fetching old blobs:

```bash
git add README.md
tree=$(git write-tree --missing-ok)
commit=$(git commit-tree "$tree" -p HEAD -m "agent change")
```

Then sends only new objects:

```json
POST /v1/commit
{
  "repo": "oven-sh/bun",
  "branch": "main",
  "expected_commit": "df55ab7...",
  "commit_object": "base64...",
  "tree_object": "base64...",
  "new_blobs": [{"sha1": "...", "object": "base64..."}]
}
```

Service:

1. Verifies write permission with GitHub.
2. Fetches current `refs/heads/main` from GitHub.
3. Rejects if it does not match `expected_commit` (force-with-lease).
4. Stores new objects locally.
5. Pushes the commit to GitHub via git protocol.
6. On success, invalidates the cached ref/tarball for that branch.

### 5. Sync model

Three layers, because no single mechanism is reliable:

1. **Webhooks:** GitHub App push events invalidate cached refs.
2. **Polling:** low-frequency `git ls-remote` for repos without webhooks.
3. **On-demand refresh:** clone request with `staleness=0s` forces a ref check.

The service returns `cached_at` and `fresh_until` so agents can decide if they
need stricter freshness.

### 6. CLI

```bash
ripclone clone https://github.com/oven-sh/bun.git
# or
ripclone clone oven-sh/bun --branch main --strict

cd bun
# edit files...

ripclone commit -m "agent change"
```

`--strict` means verify ref freshness with GitHub before serving.

## v0 explicitly out of scope

- Global object pool / cross-repo dedup.
- Cold eviction to S3 / R2 / Tigris.
- Copy-on-write workspaces.
- Submodule support.
- Git LFS support.
- Multi-region / CDN.
- Agent-side object cache.
- Commit signing.
- Branch protection handling.

These are on the roadmap after v0 proves the core loop.

## Success metrics for v0

| Metric | Target |
|---|---|
| Warm clone `oven-sh/bun` | < 5 seconds |
| Cold clone `oven-sh/bun` | < 15 seconds (document that it's slower than raw GitHub) |
| Commit + push round-trip | < 3 seconds |
| Push visible on GitHub | yes |

## Known v0 limitations

- **No history.** `git log` shows only HEAD. Agents needing history must fetch
  more objects normally.
- **Submodules not included.** GitHub tarballs don't include them.
- **LFS pointers only.** Large files are not materialized.
- **Stale cache possible.** Agents must handle non-fast-forward pushes.
- **Cold-start is slower.** First clone of a repo pays the cache-warm penalty.

## Differentiation vs code.storage

| | code.storage | ripclone v0 |
|---|---|---|
| Repo creation | Required on their service | Not required |
| GitHub relationship | Mirror/sync | Transparent proxy/cache |
| Fast path | Shallow clone | Tarball + minimal metadata |
| Cold storage | Tarball after 7 days (internal) | Deferred to v1 |
| Cross-repo dedup | Not emphasized | Deferred to v1 |
| Open source | Closed | Open-core |

## Why this wins

- **Lower friction** than code.storage: just point at a GitHub URL.
- **Faster warm clones** for agents because the fast path is a tarball download.
- **Transparent layer on GitHub**, not a competing host.
- **Proves value before building heavy infrastructure.**

## Risks

| Risk | Mitigation |
|---|---|
| Cold-start slower than raw git | Document; optimize build; pre-warm popular repos. |
| Stale cache causing push conflicts | Force-with-lease; fetch latest ref before push; layered sync. |
| GitHub rate limits | Cache aggressively; use GitHub App tokens. |
| Token storage security | Encrypt at rest; short-lived tokens; least privilege. |
| Private repo data leakage | Non-public cache paths; access checks on every request. |

## Next step

Build the v0: a local-disk Python service + CLI that can clone `oven-sh/bun`
via tarball and commit back to GitHub, with timing comparisons against raw
GitHub.
