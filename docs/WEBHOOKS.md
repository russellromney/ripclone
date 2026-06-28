# Webhooks — provider-agnostic push → warm

## Why

Today `ripclone-server` only warms a repo when something calls `POST
…/{owner}/{repo}/sync` — manually, or from a CI Action you have to write.
Self-hosters get no automatic warming on push.

This adds a built-in **webhook receiver**: a provider push hits the server, we
verify it, normalize it, and enqueue a sync — so the next clone is already warm.
No CI Action, no glue.

It is the same warm-on-push the managed cloud gives you. The cloud just layers
zero-config + multi-tenant on top (see [Relationship to the managed
cloud](#relationship-to-the-managed-cloud)).

## Where it sits

A webhook is a thin **front door**. Everything heavy already exists — the build
queue, the worker, storage, the metadata store. The receiver does three things:
**verify → normalize → enqueue**.

```
provider push ─▶ POST /webhooks/{provider}
                   │  verify signature (over the RAW body)
                   │  normalize payload → CanonicalEvent
                   ▼
                 enqueue sync (state.build_queue)  ──▶ worker ──▶ clonepack
                   ▲                                    (StaticBroker cred, #55)
                   └─ the SAME enqueue path `/sync` uses
```

So this is mostly routing + per-provider parsing, not new build logic.

## Endpoint

`POST /webhooks/{provider}` — provider-scoped, mirroring `/v1/repos/{provider}/…`.
`{provider}` selects a configured `ProviderInstance` (`rust/src/provider.rs`).

- Respond **2xx fast** (providers time out ~10s); the build runs async on the
  queue. `200 {"ok":true}` accepted, `401` bad signature, `503` if no secret is
  configured for that provider, `200 {"ignored":…}` for events we don't act on.
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
Gitea/Forgejo are implemented** (`rust/src/webhook/{github,gitlab,gitea}.rs`);
Bitbucket would follow the same trait.

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
- **Upstream credential** — the existing `StaticBroker` token for that provider
  (`rust/src/auth/broker.rs`). The webhook carries no token, so private clones use
  the server's configured credential, and the job carries it through the queue
  (#55) so the worker can clone a private repo.
- **Repo allowlist (optional)** — `RIPCLONE_WEBHOOK_ALLOWLIST`, comma-separated.
  Only enqueue for listed repos; unset ⇒ allow all (single-tenant trust, with a
  loud startup log). Entries use the **natural key**: `owner/repo` for GitHub,
  provider-prefixed for others (`gitlab/group/sub/proj`, `gitea/owner/repo`) —
  *not* the slash-escaped storage key.
- **Branch policy** — always warm the default branch (from the payload, or the
  local mirror's HEAD if the provider omits it); warm other branches only if
  already tracked. `RIPCLONE_WEBHOOK_WARM_ALL=1` warms every pushed branch.

### Per-provider setup notes

- **GitHub** — set the webhook secret to `RIPCLONE_WEBHOOK_SECRET_GITHUB` (the
  legacy `RIPCLONE_WEBHOOK_SECRET` is still honored). Point it at
  `/webhooks/github` (or the back-compat `/v1/webhooks/github`).
- **GitLab** — use the **Secret token** field (sent verbatim in `X-Gitlab-Token`),
  *not* the newer signing-token scheme (an HMAC `webhook-signature` header), which
  this receiver does not implement — it would be rejected (fail-closed), never
  silently accepted. Set `RIPCLONE_WEBHOOK_SECRET_GITLAB` to the same value.
- **Gitea / Forgejo** — the `X-Gitea-Signature` HMAC secret is
  `RIPCLONE_WEBHOOK_SECRET_GITEA`. **Enable the "Delete" event** on the webhook:
  unlike GitHub, Gitea fires a dedicated `delete` event for branch deletions (not
  a zero-`after` push), so without it branch-delete cleanup won't fire.

## Action

- **Push** to a synced ref → enqueue a sync for `(provider, owner, repo, ref)`
  with the configured credential. **Reuse the shared enqueue path**: the webhook
  calls `trigger_build(state, repo, branch)` — the same fire-and-forget enqueue
  used by `/build` and the poll loop, which coalesces against an in-flight `/sync`
  build. Do **not** duplicate build logic.
- **Branch delete** (`after` all-zeros / `deleted: true`) → clean up that ref's
  metadata; do not try to build a ref that no longer exists.
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

- **Phase 1:** push (warm), branch-delete (cleanup), ping. This is the whole
  value — push → warm — for self-host.
- **Later:** provider repo-lifecycle events where available (visibility change →
  re-gate access / retune signed-URL TTL, rename → re-key, delete → purge);
  tag/release pre-warm. These differ a lot per provider; keep them out of phase 1.

## Relationship to the managed cloud

The managed cloud does **not** route GitHub App webhooks through this receiver —
it can't, because its front door must resolve which installation fired, check the
org's entitlement/billing, and mint a **per-install** token. None of that belongs
in OSS. Instead, both paths converge one layer down:

- **Cloud:** GitHub App webhook → cloud gateway → tenant auth + entitlement + mint
  per-install token → enqueue into the backend build queue.
- **Self-host:** provider webhook → this receiver → enqueue with the static
  `StaticBroker` credential.

Both feed the **same build queue + per-job credential (#55)**, so warm behavior is
identical. The only differences are the front door and the amount of setup:

| | Self-host | Managed cloud |
|---|---|---|
| Endpoint | you point the provider at `/webhooks/{provider}` | set for you |
| Secret | `RIPCLONE_WEBHOOK_SECRET_<provider>` | managed |
| Private credential | static PAT / deploy token (`StaticBroker`) | per-install minted token |
| Repo scope | optional allowlist | App installation |
| Pre-warm on add | first push (or a config warm-list) | `installation_repositories.added` |

Same engine, same features. The cloud just removes the setup. That is the point:
self-host is not a second-class citizen — it runs the identical warm-on-push path.

## Implementation checklist

Phase 1 (GitHub) is implemented:

- [x] `webhook` module: `WebhookProvider` trait + `CanonicalEvent`
      (`rust/src/webhook/mod.rs`).
- [x] GitHub adapter (HMAC-256; push / branch-delete / ping) in
      `rust/src/webhook/github.rs`.
- [x] GitLab adapter (`X-Gitlab-Token` constant-time equality; `Push Hook`) in
      `rust/src/webhook/gitlab.rs`.
- [x] Gitea/Forgejo adapter (bare-hex HMAC-256; push / delete / ping) in
      `rust/src/webhook/gitea.rs`.
- [x] `POST /webhooks/{provider}` in `server.rs` — raw-body handler, provider
      lookup, verify, parse, dispatch. Registered under `rate_limited`, *not*
      behind `auth_middleware` (the HMAC is the auth). `/v1/webhooks/github` is a
      back-compat alias into the same receiver.
- [x] Enqueue via the shared `trigger_build` path (also used by `/build` and the
      poll loop), which coalesces with `/sync` — no duplicated build logic.
- [x] Config: per-provider webhook secret (`RIPCLONE_WEBHOOK_SECRET_<ID>`, with
      legacy `RIPCLONE_WEBHOOK_SECRET` honored for github) + `StaticBroker`
      credential for private clones + optional `RIPCLONE_WEBHOOK_ALLOWLIST` +
      `RIPCLONE_WEBHOOK_WARM_ALL` to warm every pushed branch.
- [x] Branch-delete cleanup path (`RefStore::delete_branch`, file + S3 + caching
      impls).
- [x] Tests: signature verify (valid / invalid / missing), GitHub parse, enqueue
      invoked on push, delete → cleanup, allowlist gating, no-secret ⇒ 503,
      tracked/untracked non-default branch.
- [x] Docs: README "Webhooks" section; cross-links below.

**Follow-ups:** a Bitbucket adapter (another `WebhookProvider` impl + a match arm
in `webhook::provider_for`). Repo-lifecycle events (visibility/rename/delete) and
tag/release pre-warm (see [Events](#events--phase-1-vs-later)).

## Open questions — resolved

- **Allowlist default:** allow-all (single-tenant trust) with a loud startup log
  warning that all pushed repos warm. Set `RIPCLONE_WEBHOOK_ALLOWLIST` to
  restrict. **Done.**
- **Non-default-branch policy:** always warm the default branch (from the payload
  or, if absent, the local mirror's HEAD); warm other branches only if a build for
  them already exists (`ref_store.load_branch`). `RIPCLONE_WEBHOOK_WARM_ALL=1`
  opts into warming every pushed branch instead. **Done.**
- **Multi-instance routing:** `{provider}` in the path is the `ProviderInstance`
  id (same lookup as `/v1/repos/{provider}/…`), and the secret is keyed per
  instance id — so several instances of the same kind each get their own
  endpoint + secret. **Done.**

## See also

- [`GITHUB_INTEGRATION.md`](GITHUB_INTEGRATION.md) — GitHub auth / token setup
  the webhook reuses for private clones (`StaticBroker`).
- [`BACKENDS.md`](BACKENDS.md) — the build queue + worker the receiver enqueues
  onto.
