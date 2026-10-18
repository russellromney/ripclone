# Webhooks

A push webhook makes the server build each new default-branch commit as soon as
it is pushed, with nothing added to the repo itself.

## Setup

1. Add the repo on the server: `ripclone add owner/repo`. Webhooks only build
   added repos.
2. Set a secret for the provider instance, for example
   `RIPCLONE_WEBHOOK_SECRET_GITHUB`.
3. Point a repo or org webhook at `https://<server>/webhooks/<provider>`,
   content type `application/json`, `push` events, with the same secret.

Per provider:

- **GitHub**: HMAC in `X-Hub-Signature-256`. Secret `RIPCLONE_WEBHOOK_SECRET_GITHUB`.
- **GitLab**: use the **Secret token** field (`X-Gitlab-Token`), not the newer
  signing-token scheme, which is rejected. Secret `RIPCLONE_WEBHOOK_SECRET_GITLAB`.
- **Gitea / Forgejo**: HMAC in `X-Gitea-Signature`. Secret
  `RIPCLONE_WEBHOOK_SECRET_GITEA`.

## Behavior

- A signed push to the repo's **default branch** builds the pushed commit
  (`after`). Other branches and pings are acknowledged without a build.
- Any branch or tag push or delete, the default branch's included, refreshes
  the repo's branches and tags: the server fetches them from the upstream
  and publishes them (see [Freshness](FRESHNESS.md#branches-and-tags)). On
  GitLab, enable **Tag push events**; on Gitea, the **Delete** event covers
  branch and tag deletes.
- The response is fast: `200` means accepted, not built.
- No secret configured for the provider: `503`. Bad signature: `401`. A
  `generic` provider has no webhooks: `501`.
- The signature is checked over the raw body, in constant time, before parsing.
- The payload is used only to identify the repo and commit. It never selects a
  credential, and the server never fetches a URL from it.

Private repos build with the server's configured credential for that provider.
