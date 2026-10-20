# HTTP API

The `ripclone` CLI uses this API. CI jobs and other tools can call it
directly.

## Authentication

Every route below except the [public ones](#public-routes) needs the server
token:

```
Authorization: Ripclone <hex SHA-256 of RIPCLONE_SERVER_TOKEN>
```

A missing or wrong header gets `401 {"error": "unauthorized"}`. The header
carries the token's hash, and the server compares it with its stored hash. So
the hash works as a password: anyone who has it can call the API.

A client may send `x-ripclone-protocol: <n>`. If `n` is not the server's wire
protocol (see `GET /v1/version`), the server answers `426`. Callers that leave
the header out are not checked.

## Repositories

A repository is addressed as `{provider}/{path}`, where `provider` is a
provider instance id (`github` by default; see [Providers](PROVIDERS.md)), for
example `github/oven-sh/bun` or `gitlab/group/sub/project`.

| Route | Does |
|---|---|
| `POST /v1/repos/{provider}/{path}/add` | Resolve the default branch, add the repo, and admit a build of its tip. Answers with a build summary. |
| `DELETE /v1/repos/{provider}/{path}/add` | Remove the repo from the added set: `204`. Published artifacts stay. |
| `DELETE /v1/repos/{provider}/{path}` | Purge the repo: `204`, also for a repo the server does not know. |
| `GET /v1/repos` | The added repos, as `[{"provider": "github", "path": "acme/app"}]`. |
| `POST /v1/repos/{provider}/{path}/builds` | Resolve a selector to one commit and admit a build. |
| `GET /v1/repos/{provider}/{path}/commits/{sha}` | Read one commit's build. Never builds. |
| `GET /v1/repos/{provider}/{path}/status` | Every build of the repo, with output states and stored bytes. |

Only added repos build: `POST .../builds` for a repo that was not added
answers `404 repo_not_added`.

A purge removes the repo from the added set and deletes its builds and
published refs at once, so its commits read `404` and its running builds and
refreshes stop. Its mirror goes once no build holds it. Each artifact that no
other repo's builds or refs name is deleted
from storage after twice the signed URL lifetime, so a clone that read the
repo before the purge finishes. Two repos at the same commits share their
artifacts, so purging one leaves the other whole. Deletion runs every 5
minutes.

### Admit a build

```
POST /v1/repos/github/acme/app/builds
{"selector": "main", "retry": false}
```

`selector` is `HEAD` (the default branch), a full commit SHA (40 or 64
lowercase hex), or a branch or tag name. `HEAD` and names cost one read of
the upstream's refs: over HTTP, one protocol v2 `ls-refs` request on a
connection the server keeps open, or `git ls-remote` when the upstream does
not answer it. A full SHA is trusted as given and admitted
without asking the upstream. Anything else, such as `HEAD~1`, is `400
invalid_selector`. `retry: true` puts failed outputs of the commit back to
pending.

The answer comes at once and never waits for a build:

```json
{
  "checkout_name": "main",
  "commit": "9f2c1e4b7a0d3c5e8f1a2b4c6d8e0f1a3b5c7d9e",
  "files": {"state": "pending"},
  "full": {"state": "pending"},
  "head": {"state": "ready"}
}
```

`checkout_name` is the branch a clone checks out. It is `null` for a SHA or a
tag, which give a detached checkout. `tag` is present when the selector named
a tag, and gives its name; a depth-1 clone then fetches that tag, as `git
clone --depth 1 --branch <tag>` does. Admitting a commit that already has a
build changes nothing.

### Read a build

`GET /v1/repos/{provider}/{path}/commits/{sha}` without `kind` returns the
summary above, without `checkout_name` or `tag`. With `?kind=head`, `full`, or `files`
it returns one output. Responses carry `Cache-Control: no-store`.

Each commit has three outputs, built separately. Head is the depth-1 pack. It
is built first. Full (all history) and Files (the working-tree archive) are
built after it. A ready output never changes. See [Design](DESIGN.md).

**Ready.** The artifact hashes and, when storage is S3, signed URLs for them:

```json
{
  "clonepack_manifest": "<sha256>",
  "clonepack_manifest_url": "https://s3.example.com/ripclone/<key>?X-Amz-...",
  "commit": "9f2c1e4b7a0d3c5e8f1a2b4c6d8e0f1a3b5c7d9e",
  "host": "github.com",
  "kind": "full",
  "metadata_chunk": "<sha256>",
  "metadata_chunk_url": "https://s3.example.com/ripclone/<key>?X-Amz-...",
  "origin_url": "https://github.com/acme/app.git",
  "owner": "acme",
  "pack_chunk_urls": ["https://s3.example.com/ripclone/<key>?X-Amz-..."],
  "pack_idx_urls": [null, "https://s3.example.com/ripclone/<key>?X-Amz-..."],
  "parent_commit": "4a6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c",
  "provider": "github",
  "repo": "app",
  "state": "ready"
}
```

The `*_url` fields are left out when storage cannot sign URLs (local disk).
Fetch those artifacts from `GET /v1/artifacts/{hash}` instead. In a Full,
`pack_idx_urls` has the idx of each history pack, in manifest order, and
`null` for the Head packs, which a full clone reads only for the working
tree. In a Head, `idx_bundle_url` is one object with the idx files of the
base packs, which every Head on that base shares. `pack_idx_urls` has the
idx of each level pack and `null` for the base packs. A Head also has
`midx_url` when it has a pre-built multi-pack-index. A
Files output has `archive_chunk_urls` instead of `pack_chunk_urls`. A Head,
Full or Files output may also have `metadata_delta` and `metadata_delta_url`:
then its files table is the metadata chunk, an earlier output's table, with
that delta applied. Signed URLs last
20 minutes. Read the commit again to get fresh ones.

Head and Full outputs also carry `tags_artifact` and `tags_artifact_url`
when the repository has tags: the repository's newest tags, the same for
every commit. A clone keeps the tags whose target it holds, as `git clone`
does. The artifact can be newer than the output; it changes when tags are
pushed, moved, or deleted (see [Freshness](FRESHNESS.md#branches-and-tags)).

A Full output also carries `branches` once the repository's refs are
published: `artifact` and `artifact_url`, the repository's branches and the
default branch's tip, and `side_packs`, a list of layers (`pack`,
`pack_len`, `pack_url`, `idx`, `idx_len`, `idx_url`) that together hold the
objects the other branches and tags reach that the default branch's history
does not. It is empty when every ref is in that history. A full clone that
holds the default branch's tip installs every layer and writes every branch
as `refs/remotes/origin/<branch>`.

A Full of another commit also carries `refs_base` when the default branch's
tip has a ready Full and is not in the commit's own history: that Full's
ready body. A full clone installs its history packs with the side pack, in
place of its own commit's, and writes the working tree from its own
commit's Head packs. While the newest tip has no ready Full, `tags_artifact`,
`branches` and `refs_base` are those of the last refresh whose tip has one.

With `refs_base` comes `own_history` when the server packed the commit's own
history against a base that `refs_base`'s history holds: `packs`, layers as in
`side_packs`, with the objects the commit reaches beyond that base and its
Head packs, and `head_packs`, the indices in the manifest of the Head packs
a clone keeps as history next to them. A clone whose commit is in no ref's
history installs those in place of its own Full's history packs.

**Pending.** The output is not built yet:

```
{
  "attempts": 0,
  "commit": "9f2c1e4b7a0d3c5e8f1a2b4c6d8e0f1a3b5c7d9e",
  "kind": "full",
  "state": "pending",
  "top_up": {
    "base": { <the ready body of the base commit's Full> },
    "base_commit": "4a6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c"
  }
}
```

`attempts` counts this output's build attempts so far. After a failed attempt that will be
retried, `error` holds that attempt's error. `top_up` appears only for a
pending Full when an earlier Full is ready: the newest ready Full admitted from
the same branch, else the parent commit's. A full clone installs `base`, then
fetches the target commit from the upstream with the user's own git (see
[Freshness](FRESHNESS.md)).

**Failed.** The output failed for good: the error is not retryable, or it
failed 5 times. It builds again only when the
commit is admitted with `retry: true` (or through `/add`):

```json
{
  "commit": "9f2c1e4b7a0d3c5e8f1a2b4c6d8e0f1a3b5c7d9e",
  "error": "<the build error>",
  "kind": "files",
  "state": "failed"
}
```

### Errors

Repo routes answer errors as `{"error": "<message>", "code": "<code>"}`:

| Status | `code` | When |
|---|---|---|
| 400 | `invalid_request` | Malformed body, query, or repo path. |
| 400 | `invalid_selector` | The selector is not `HEAD`, a full SHA, or a branch or tag name. |
| 400 | `invalid_commit` | `{sha}` is not 40 or 64 lowercase hex characters. |
| 404 | `unknown_provider` | No provider instance has that id. |
| 404 | `repo_not_added` | The repo was not added. |
| 404 | `repo_not_found` | The upstream has no such repo, or the server's credential cannot read it. |
| 404 | `ref_not_found` | The upstream has no such branch or tag. |
| 404 | `build_not_found` | No build was admitted for that commit. |
| 500 | `credential_unavailable` | The server could not get an upstream credential. |
| 502 | `upstream_unavailable` | The upstream did not answer, or answered with a retryable error. |
| 503 | `control_unavailable` | The control database could not be read or written. |

Other errors (`401`, `426`, an unknown route, an artifact that does not
exist) are `{"error": "<message>"}` with no `code`.

## Artifacts

`GET /v1/artifacts/{hash}` returns one artifact by its SHA-256 (64 lowercase
hex characters). With S3 storage it redirects (`307`) to a signed URL. With
local storage it streams the bytes and honors a single `Range: bytes=a-b`.
Check every artifact against its hash before using it.

This route names no repository, so a proxy in front of the server cannot
check repository access on it. With S3 storage the CLI never uses it: every
artifact a commit read names comes with a signed URL, and when one expires
the CLI reads the commit again. A proxy that serves private repositories
can refuse this route when storage is S3, and must keep it with local
storage.

## Webhooks

`POST /webhooks/{provider}` takes a provider's push webhook. It is
authenticated by the provider's signature, not the server token. See
[Webhooks](WEBHOOKS.md).

## Public routes

These need no token:

| Route | Answers |
|---|---|
| `GET /healthz` | `200 {"status": "ok"}` while the process runs. |
| `GET /readyz` | `200 {"status": "ready"}`, or `503 {"status": "not_ready"}` when storage or the control database is down. The result is cached for 3 seconds. |
| `GET /v1/version` | `{"version": "0.1.0", "protocol": 3}`: the build version and the wire protocol. |
| `GET /metrics` | Prometheus text metrics. |

`POST /v1/clones/{id}/metrics` takes the CLI's post-clone report (see
[Telemetry](../README.md#telemetry)). It needs the token and answers `202`. The
server drops the report.
