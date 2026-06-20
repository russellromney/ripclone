# GitHub-native fast clone service

This doc designs a fast git clone service that sits **on top of GitHub** rather
than beside it. GitHub remains the source of truth for repos, permissions,
refs, pull requests, issues, and collaboration. The service is an
authentication-aware, eventually-consistent cache and accelerator.

The goal: `git clone` and related operations are dramatically faster, but every
write and permission check still flows through GitHub.

---

## 1. Core rule: GitHub is the source of truth

The service never owns the repo. It only caches and accelerates access to
GitHub-owned repos.

```
┌─────────────┐      auth + writes      ┌─────────────┐
│   Agent     │ ◄──────────────────────►│   GitHub    │
│             │                         │  (source)   │
└──────┬──────┘                         └─────────────┘
       │
       │ fast reads + cached snapshots
       ▼
┌─────────────────────┐
│  Fast Clone Service │
│  (cache + object    │
│   pool + snapshots) │
└─────────────────────┘
```

Rules:

1. **Refs are authoritative on GitHub.** The service caches them with a TTL.
2. **Permissions are authoritative on GitHub.** The service checks GitHub on
every write and caches read permissions with a short TTL.
3. **Commits are authoritative on GitHub.** All new commits are pushed to
GitHub first; the service updates its cache only after a successful push.
4. **The service can be stale, but not wrong.** It serves cached data quickly
and validates asynchronously or on write.

---

## 2. Authentication models

### 2.1 GitHub App (recommended)

Users install a GitHub App on their orgs/repos. The service receives
installation tokens.

Pros:
- Centralized rate limits and quota management.
- Webhook support out of the box.
- Fine-grained permissions via GitHub.
- No user PATs to manage.

Cons:
- Users must install the app.
- Read access to private repos requires explicit grant.

Token flow:

```
User installs GitHub App on oven-sh/bun
        │
        ▼
GitHub sends installation event to service
        │
        ▼
Service requests installation token from GitHub
        │
        ▼
Service uses token to read/write on behalf of the installation
```

### 2.2 OAuth App / user token

Users authorize the service via OAuth. The service holds a user access token.

Pros:
- Works for personal repos immediately.
- Fine-grained via GitHub scopes.

Cons:
- Token management per user.
- Rate limits tied to users.

### 2.3 Deploy keys / machine users

For CI/agents, the service can manage deploy keys or machine user tokens.

Use when:
- The caller is a machine, not a human.
- The service is self-hosted inside an org.

### 2.4 Permission caching

The service should not call GitHub for every read. It can cache permission
results:

```
Cache key: (token_hash, repo, permission)
TTL: 1-5 minutes for reads, 0 for writes
```

On a write, the service always re-checks GitHub. On a read, it trusts the
cache briefly.

---

## 3. Permission checks

### 3.1 Read operations

For `clone`, `fetch`, `read object`, `list refs`:

1. Service looks up the user's/installation's access to the repo.
2. Cache hit → serve. Cache miss → call GitHub:
   ```
   GET /repos/{owner}/{repo}
   ```
   If the response is not 404 and the token has `contents:read`, allow.
3. Serve cached data scoped to that repo.

### 3.2 Write operations

For `commit`, `push`, `update ref`:

1. Service calls GitHub to verify `contents:write`:
   ```
   GET /repos/{owner}/{repo}
   ```
   Or attempt a lightweight operation and handle 403.
2. Only then does it push the commit to GitHub.

### 3.3 Data isolation

- **Object pool:** content-addressed objects can be shared across users/repos
  because the sha-1 key reveals no content. However, for private repos, the
  service may choose to scope the object cache per repo/org to avoid even
  metadata leakage about which private objects exist.
- **Snapshots/tarballs:** always scoped by repo and access-controlled. A user
  can only fetch a tarball for repos they can read.
- **Workspaces:** always owned by a specific user/installation.

---

## 4. Mirror and update strategy

The service maintains three caches:

1. **Ref cache:** `(repo, branch) → commit sha`.
2. **Object cache:** `sha-1 → object bytes`.
3. **Snapshot cache:** `(repo, commit) → tarball/packfile`.

### 4.1 How refs stay fresh

**Webhook path (fastest):**

GitHub App is configured with a webhook URL. On every push:

```json
POST /github/webhook
{
  "ref": "refs/heads/main",
  "before": "abc123...",
  "after": "def456...",
  "repository": { "full_name": "oven-sh/bun" }
}
```

Service:
- Invalidates ref cache for `oven-sh/bun:main`.
- Fetches the new commit + tree.
- Schedules snapshot/tarball generation.

**Polling path (fallback):**

For repos without webhooks, or if a webhook is missed:

```bash
git ls-remote https://github.com/oven-sh/bun.git refs/heads/main
```

Run with backoff: every 5s for active repos, every 60s for quiet repos.

**On-demand refresh:**

When a clone request arrives:

1. Check cached ref age.
2. If older than `max_staleness` (e.g., 5s for active repos, 60s for quiet),
   do a lightweight `ls-remote` check.
3. If the ref changed, refresh before serving.

### 4.2 Object cache warming

On webhook/poll, the service fetches:

- The new commit object.
- The new tree object.
- Any blobs referenced by the new tree that are not already cached.

This can be throttled. For a huge repo, only fetch commit+tree eagerly; fetch
blobs lazily.

### 4.3 Snapshot cache invalidation

When a ref updates from `C` to `C'`:

- `C.tar.gz` is not deleted; it is still valid for that commit.
- `C'.tar.gz` is generated asynchronously.
- The ref cache now points to `C'`.

This means a clone request after a push may briefly get the old tarball if the
service hasn't refreshed yet. For most agent use cases this is fine; for
critical cases, the client can request a specific commit SHA.

### 4.4 Private repo handling

For private repos, the service should not proactively mirror unless a
permitted user has requested it. The mirror is user-scoped or
installation-scoped.

```
User A requests clone of private org/repo
        │
        ▼
Service verifies User A can read it
        │
        ▼
Service fetches objects using User A's token
        │
        ▼
Objects stored in object pool, tagged as accessible to User A's installation
```

When User B later requests the same private repo, the service re-verifies with
GitHub using User B's token before serving cached data.

---

## 5. Clone API

The clone endpoint should feel like a faster GitHub.

### 5.1 Request

```
POST /repos/{owner}/{repo}/clone
Authorization: Bearer <github-token-or-service-token>

{
  "ref": "refs/heads/main",
  "format": "tarball",      // tarball | packfile | workspace
  "staleness": "5s"         // max acceptable staleness
}
```

### 5.2 Response

```json
{
  "ref": "refs/heads/main",
  "commit": "df55ab7...",
  "tree": "abc1234...",
  "tarball_url": "https://cdn.fastgit.dev/.../main.tar.gz?sig=...",
  "metadata_pack_url": "https://api.fastgit.dev/.../metadata.pack",
  "expires_at": "2026-06-15T21:00:00Z"
}
```

The agent:

```bash
curl -fsSL "$tarball_url" | tar -xz
git init
git remote add origin https://github.com/oven-sh/bun.git
curl -fsSL "$metadata_pack_url" | git unpack-objects
git read-tree HEAD
```

Result: full working tree + minimal `.git` metadata, no old blobs fetched.

### 5.3 Staleness guarantee

- `strict`: service always verifies ref with GitHub before responding.
- `eventual` (default): service may return a tarball up to `staleness` old.

Most agent workflows want `eventual` for speed.

---

## 6. Commit and push flow

This is the most sensitive part because it mutates GitHub state.

### 6.1 Agent-side

The agent edits files and creates new git objects, but does **not** fetch old
blobs:

```bash
git add README.md
tree=$(git write-tree --missing-ok)
commit=$(git commit-tree "$tree" -p HEAD -m "agent change")
git update-ref HEAD "$commit"
```

The agent then collects the new objects:

```bash
git cat-file -p "$commit" > commit.obj
git cat-file -p "$tree"   > tree.obj
git cat-file -p <new-blob-sha> > blob.obj
```

### 6.2 Service-side

```
POST /repos/{owner}/{repo}/git/commits
Authorization: Bearer <token>

{
  "branch": "main",
  "expected_commit": "df55ab7...",
  "commit_object": "base64...",
  "tree_object": "base64...",
  "new_blobs": [
    {"sha1": "111111...", "object": "base64..."}
  ],
  "message": "agent change"
}
```

Service steps:

1. **Auth check.** Call GitHub to verify the token has `contents:write` on the
   repo.
2. **Ref check.** Fetch `refs/heads/main` from GitHub to confirm it is still at
   `expected_commit`.
3. **Object validation.** Verify the commit object's parent is
   `expected_commit`, tree hash matches the tree object, and all new blobs are
   provided.
4. **Push to GitHub.** Use git protocol with the token:
   ```bash
   git push https://<token>@github.com/oven-sh/bun.git <commit>:refs/heads/main
   ```
   Or use force-with-lease if the API supports it.
5. **On success:**
   - Store new objects in the object pool.
   - Update the service's ref cache to point to the new commit.
   - Schedule snapshot/tarball generation.
   - Return the new commit SHA.
6. **On non-fast-forward:**
   - Return `409 Conflict` with the current HEAD.
   - Agent must rebase or re-fetch.

### 6.3 Why push to GitHub instead of using the API?

GitHub's REST API supports creating trees/commits/refs, but it is slow and has
size limits. The git protocol is faster for bulk objects and supports the same
auth token.

### 6.4 Race handling

Multiple agents may push to the same branch. The service can:

- **Serialize pushes per branch:** queue push requests for the same branch and
  process them one at a time. This reduces races but adds latency.
- **Force-with-lease:** require the agent to specify `expected_commit`; reject
  if GitHub's HEAD has moved. This is the git-native way and scales better.

Recommended: use force-with-lease and let the agent handle conflicts.

---

## 7. Ref updates outside the service

Users can still push to GitHub directly, bypassing the service. This is fine
because:

1. Webhooks notify the service of the change.
2. Polling catches missed webhooks.
3. On the next clone/commit, the service refreshes its ref cache.

The service never assumes it is the only writer.

---

## 8. API shape: mirror GitHub where possible

To make adoption easy, the service can expose GitHub-compatible endpoints:

| GitHub API | Service equivalent | Purpose |
|---|---|---|
| `GET /repos/{o}/{r}` | same | repo metadata + permissions check |
| `GET /repos/{o}/{r}/contents/{path}` | cached | read file from snapshot |
| `GET /repos/{o}/{r}/git/blobs/{sha}` | cached object pool | read blob |
| `GET /repos/{o}/{r}/git/trees/{sha}` | cached object pool | read tree |
| `GET /repos/{o}/{r}/git/refs/heads/{b}` | cached ref | read branch pointer |
| `POST /repos/{o}/{r}/git/commits` | pushes to GitHub | create commit |
| `POST /repos/{o}/{r}/git/refs/...` | pushes to GitHub | update ref |
| `POST /repos/{o}/{r}/clone` | service-specific | fast clone |
| `POST /repos/{o}/{r}/fork-workspace` | service-specific | COW workspace |

The goal is that existing tools can switch their base URL to the service and
most operations work unchanged, while `clone` and workspace operations are
faster.

---

## 9. Handling GitHub rate limits

The service must minimize GitHub API calls:

- **Cache permissions** with short TTL.
- **Cache refs** and refresh via webhooks + polling.
- **Batch object fetches** during mirroring.
- **Use GitHub App tokens** for higher rate limits.
- **Avoid API for reads** when possible; use `git ls-remote` and git protocol
  for object fetching.

For very active repos, the service can subscribe to GitHub's webhook events
and rarely need to poll.

---

## 10. Security considerations

1. **Token storage.** Service should store tokens encrypted at rest, ideally
   short-lived (GitHub App installation tokens rotate hourly).
2. **No write caching.** Never acknowledge a write until GitHub confirms it.
3. **Snapshot access control.** Tarball URLs must be signed and short-lived.
4. **Private object scoping.** For paranoid deployments, scope cached objects
   by installation/org so private repo object hashes don't leak across
   tenants.
5. **Audit logging.** Log every write and permission check.

---

## 11. Deployment options

### 11.1 Managed SaaS

`fastgit.dev` or similar. Users install a GitHub App. The service handles
auth, caching, and CDN.

### 11.2 Self-hosted proxy

An org runs the service inside its own infrastructure. It uses org-level
GitHub App or deploy keys. Good for compliance.

### 11.3 GitHub Actions sidecar

A service that runs alongside GitHub Actions runners, caching objects and
snapshots for CI jobs. Reduces redundant clones.

---

## 12. Summary

The right integration model is **GitHub as source of truth + service as fast
read cache and push proxy**:

- Auth and permissions come from GitHub.
- Refs are cached but refreshed via webhooks and polling.
- Objects and snapshots are cached globally.
- Commits are always pushed to GitHub first.
- The service exposes GitHub-compatible APIs plus faster `clone`/`workspace`
  endpoints.

This gives agents the speed of a dedicated clone service without forking from
the GitHub ecosystem.
