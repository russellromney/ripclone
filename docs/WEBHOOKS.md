# Webhooks — provider-agnostic push → exact admission

## Why

A provider push hits the built-in receiver, which verifies and normalizes the
payload, validates its exact `after` commit, and admits that immutable target.
The response is fast; artifact construction remains asynchronous.

## Where it sits

A webhook is a thin **front door**. Everything heavy already exists — the build
queue, the worker, storage, and metadata store. The receiver does four things:
**verify → normalize → validate exact after → enqueue**.

```
provider push ─▶ POST /webhooks/{provider}
                   │  verify signature (over the RAW body)
                   │  normalize payload → CanonicalEvent
                   ▼
                 enqueue exact (state.build_queue) ──▶ worker ──▶ clonepack
                   ▲                                      (configured cred)
                   └─ the SAME enqueue path `/sync` uses
```

So this is mostly routing + per-provider parsing, not new build logic.

## Endpoint

`POST /webhooks/{provider}` — provider-scoped, mirroring `/v1/repos/{provider}/…`.
`{provider}` selects a configured `ProviderInstance` (`rust/src/provider.rs`).

- Respond **2xx fast** (providers time out ~10s); the exact commit build runs
  asynchronously on the queue. `200 {"ok":true}` means the event was accepted
  or coalesced, not that artifacts are ready. `401` is a bad signature, `503`
  means no secret is configured for that provider, and `200 {"ignored":…}` is
  used for events we deliberately do not act on.
- Register in the axum router in `rust/src/server.rs` (~line 506, next to the
  `dispatch_*` routes). The handler needs the **raw body** for the HMAC, so take
  `Request<Body>` like the `dispatch_*` handlers and read the bytes *before*
  parsing JSON. Put it behind the existing `rate_limited` layer.

## Provider adapter — the one thing you add per provider

```rust
struct CanonicalEvent {
    kind: EventKind,            // Push | BranchDelete | Ping | Other
    repo: RepoId,               // owner/name, provider-normalized
    ref_: String,               // "refs/heads/main"
    after: Option<String>,      // new tip sha (None / all-zeros => delete)
    default_branch: Option<String>,
    private: Option<bool>,
}

trait WebhookProvider {
    /// Constant-time signature/secret check over the RAW body.
    fn verify(&self, headers: &HeaderMap, raw: &[u8], secret: &str) -> bool;
    /// Parse a provider payload into the canonical shape (None => ignore).
    fn parse(&self, headers: &HeaderMap, raw: &[u8]) -> Option<CanonicalEvent>;
}
```

Per-provider specifics:

| Provider | Signature check | Event header | Repo / ref fields |
|---|---|---|---|
| GitHub | `X-Hub-Signature-256` = `sha256=` + HMAC-SHA256(secret, body) | `X-GitHub-Event` | `repository.owner.login` / `repository.name` / `repository.default_branch` / `repository.private`; `ref`, `after`, `deleted` |
| GitLab | `X-Gitlab-Token` == secret (constant-time) | `X-Gitlab-Event` | `project.path_with_namespace`; `ref`, `after`, `before`, `checkout_sha` |
| Gitea / Forgejo | `X-Gitea-Signature` = HMAC-SHA256(secret, body) hex | `X-Gitea-Event` | `repository.{owner.login, name, default_branch, private}`; `ref`, `after` |

Adding a provider = implement `WebhookProvider`. **GitHub, GitLab, and
Gitea/Forgejo are implemented** (`rust/src/webhook/{github,gitlab,gitea}.rs`).

Two adapter notes worth knowing:
- **GitLab** authenticates with a shared *token* echoed in `X-Gitlab-Token`, not
  a body HMAC — so its `verify` is a constant-time token equality, and the raw
  body is unused there. Only `Push Hook` is acted on; visibility comes from
  `project.visibility_level` (`< 20` ⇒ non-public).
- **Gitea/Forgejo** sends a *bare* hex HMAC in `X-Gitea-Signature` (no `sha256=`
  prefix), and its dedicated `delete` event carries a *short* branch name in
  `ref` — the adapter normalizes it back to `refs/heads/<branch>` so the handler
  stays uniform.

## Configuration

Per provider instance:

- **Webhook secret** — e.g. `RIPCLONE_WEBHOOK_SECRET_<provider>` (or a field on the
  `ProviderInstance` config). **No secret ⇒ the endpoint returns 503.** Never
  process an unverified webhook — this matches the rest of the server's
  fail-closed posture.
- **Upstream credential** — the existing broker token for that provider
  (`rust/src/auth/broker.rs`). The webhook carries no token, so private clones use
  the server's configured credential. For SQL farm-out the selected job carries
  that credential through the existing queue transport; it is not merged with a
  later commit's credential.
- **Repo allowlist (optional)** — `RIPCLONE_WEBHOOK_ALLOWLIST`, comma-separated.
  Only enqueue for listed repos; unset ⇒ allow all (single-tenant trust, with a
  loud startup log). Entries use the **natural key**: `owner/repo` for GitHub,
  provider-prefixed for others (`gitlab/group/sub/proj`, `gitea/owner/repo`) —
  *not* the slash-escaped storage key. (For GitHub the prefixed
  `github/owner/repo` form is also accepted, so the asymmetry isn't a footgun.)
- **Branch policy** — admit only a push whose signed payload identifies the
  pushed branch as the repository's default branch. A missing default-branch
  identity or a non-default push is acknowledged without work.

### Per-provider setup notes

- **GitHub** — set the webhook secret to `RIPCLONE_WEBHOOK_SECRET_GITHUB` and
  point it at `/webhooks/github`.
- **GitLab** — use the **Secret token** field (sent verbatim in `X-Gitlab-Token`),
  *not* the newer signing-token scheme (an HMAC `webhook-signature` header), which
  this receiver does not implement — it would be rejected (fail-closed), never
  silently accepted. Set `RIPCLONE_WEBHOOK_SECRET_GITLAB` to the same value.
- **Gitea / Forgejo** — the `X-Gitea-Signature` HMAC secret is
  `RIPCLONE_WEBHOOK_SECRET_GITEA`. Its dedicated `delete` event is normalized
  and acknowledged without changing exact results.

## Action

- **Push** to the payload-identified default branch → validate `after` as a full
  object ID and enqueue `(repository, after)` with the configured credential.
  **Reuse the shared exact enqueue path**: the webhook calls the same admission
  function used by `/v1/build` and the poll loop. It performs zero additional
  `ls-remote` probes, coalesces exact duplicates in queued/claimed/embedded
  Full work, and keeps a later commit as a separate job. Do **not** duplicate
  build logic.
- **Branch delete** (`after` all-zeros / `deleted: true`) → acknowledge without
  work. Exact results outlive checkout-name deletion and remain commit keyed.
- **Ping** → `200`. **Other** → ignore.

## Security

- Verify the HMAC over the **raw body**, before any JSON parse. Constant-time
  compare (`subtle::ConstantTimeEq` or equivalent).
- Fail closed: no secret ⇒ 503; bad signature ⇒ 401.
- Trust the payload only for **routing** (owner / repo / ref). Never use it to
  choose a credential or to escalate.
- Keep the route under the existing `rate_limited` router.
- No SSRF surface: we never fetch a payload-supplied URL. The worker clones the
  known origin of the configured `ProviderInstance`.

## Events — phase 1 vs later

- **Phase 1:** default-branch push (exact admission), delete (ignore), and ping.
  This is the whole webhook value for self-host.
- **Later:** provider repo-lifecycle events where available (visibility change →
  re-gate access / retune signed-URL TTL, rename → re-key, delete → stop future
  admission while retaining published exact results);
  tag/release pre-warm. These differ a lot per provider; keep them out of phase 1.

## Explicit add — the added-repo set

The `RIPCLONE_WEBHOOK_ALLOWLIST` above is a *static* gate: it answers "may this
pushed repo admit exact work?" but it lives in config and needs a restart to
change. For a server you keep running, you manage eligibility **at runtime** by
*adding* repositories. A push admits work only if its repo has been added—the
added-repo set is the dynamic watch-list.

An added repo is eligible for exact admission from a signed default-branch push,
and the set survives restarts.

### API

Authenticated with the server token (the same `RIPCLONE_SERVER_TOKEN` that gates
`/build`):

- `POST   /v1/repos/{provider}/{owner}/{repo}/add` — add the repo and admit its first exact build
- `DELETE /v1/repos/{provider}/{owner}/{repo}/add` — remove it

`add` is idempotent and admits an initial exact build. Its HTTP response and CLI
return after ready detection or queue acceptance; `202` does not mean the first
clonepack is complete. There is no separate `track`/`untrack`/`tracked` verb —
adding a repo is what makes it both cloneable and eligible for push admission.

### CLI

The CLI wraps the API against the configured server:

```
ripclone add owner/repo          # make it cloneable and eligible for exact pushes
```

Provider-prefixed forms — `gitlab:group/proj`, `gitea:owner/repo` — use the same
natural-key convention as the allowlist. Removal is server-token-gated via the
`DELETE …/add` endpoint.

### Storage

The added-repo set is a table in the server-owned SQLite/Turso control database,
alongside exact results and durable jobs. Artifact bytes remain in the selected
local or S3-compatible storage backend.

### How it combines with the allowlist

On a push, the receiver enqueues a sync only when **both** hold:

1. the repo has been **added** (`ripclone add`), and
2. the repo passes the allowlist — `RIPCLONE_WEBHOOK_ALLOWLIST` is unset
   (allow-all) or the repo is on it.

So the allowlist is the optional "set it and forget it" restriction, and the
added-repo set is the "manage it as you go" gate. With no repos added, a push
admits nothing: **explicit by default.** For an added, allowed repo, only a
signed push identifying its default branch admits the payload's exact commit.

## Implementation checklist

Phase 1 (GitHub) is implemented:

- [x] `webhook` module: `WebhookProvider` trait + `CanonicalEvent`
      (`rust/src/webhook/mod.rs`).
- [x] GitHub adapter (HMAC-256; push / delete / ping) in
      `rust/src/webhook/github.rs`.
- [x] GitLab adapter (`X-Gitlab-Token` constant-time equality; `Push Hook`) in
      `rust/src/webhook/gitlab.rs`.
- [x] Gitea/Forgejo adapter (bare-hex HMAC-256; push / delete / ping) in
      `rust/src/webhook/gitea.rs`.
- [x] `POST /webhooks/{provider}` in `server.rs` — raw-body handler, provider
      lookup, verify, parse, dispatch. Registered under `rate_limited`, *not*
      behind `auth_middleware` (the HMAC is the auth).
- [x] Admit the validated exact `after` through the shared trigger path (also
      used by `/v1/build` and the poll loop), with no second tip probe and no
      duplicated build logic.
- [x] Config: per-provider webhook secret (`RIPCLONE_WEBHOOK_SECRET_<ID>`) +
      `StaticBroker` credential for private clones + optional
      `RIPCLONE_WEBHOOK_ALLOWLIST`.
- [x] Delete events are acknowledged without deleting commit-keyed results.
- [x] Tests: signature verify (valid / invalid / missing), GitHub parse, enqueue
      invoked on default-branch push, delete ignored, allowlist gating,
      no-secret ⇒ 503, and non-default push ignored.
- [x] Docs: README "Webhooks" section; cross-links below.

**Follow-ups:** Repo-lifecycle events (visibility/rename/delete) and tag/release
pre-warm (see [Events](#events--phase-1-vs-later)).

## Open questions — resolved

- **Allowlist default:** allow-all (single-tenant trust) with a loud startup log
  warning that every added repo is eligible for default-branch push admission.
  Set `RIPCLONE_WEBHOOK_ALLOWLIST` to restrict. **Done.**
- **Non-default-branch policy:** only the payload-identified default branch
  admits exact work. Missing identity and non-default pushes are acknowledged
  without a build. **Done.**
- **Multi-instance routing:** `{provider}` in the path is the `ProviderInstance`
  id (same lookup as `/v1/repos/{provider}/…`), and the secret is keyed per
  instance id — so several instances of the same kind each get their own
  endpoint + secret. **Done.**

## See also

- [`CONFIG.md`](CONFIG.md) — current provider credential configuration for
  private clone and webhook builds.
- [`BACKENDS.md`](BACKENDS.md) — the build queue + worker the receiver enqueues
  onto.
