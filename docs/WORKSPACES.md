# Workspace identity

A ripclone workspace owns exactly one upstream Git provider connection. GitHub,
GitLab, Gitea, and generic Git hosts remain supported provider *kinds*, but they
are no longer an independent repository-routing namespace.

Repository identity is:

```text
WorkspaceId + opaque repository path
```

The workspace ID remains the first segment in storage keys and server routes.
This is intentionally byte-compatible with the old provider-instance ID, so
existing refs, added-repo records, queues, mirrors, and object-store prefixes do
not need to move.

## Configuration

The canonical local/self-hosted configuration is:

```toml
default_workspace = "acme"

[workspace]
id = "acme"
provider = "github"
host = "github.com"
# token = "..." # prefer the credential broker/environment in production
```

The equivalent environment value is:

```text
RIPCLONE_WORKSPACE={"id":"acme","provider":"github","host":"github.com"}
```

Configure it with:

```text
ripclone workspace set acme --provider github --host github.com
ripclone workspace show
```

Normal commands select a workspace, never a provider:

```text
ripclone --workspace acme clone owner/repo
```

`--provider`, provider-prefixed repository arguments, `[providers.*]`,
`default_provider`, `RIPCLONE_PROVIDERS`, and `ripclone provider` remain as
deprecated migration inputs. Each legacy provider instance is interpreted as a
workspace with the same ID and exactly one upstream. New configuration is
serialized with `workspace`/`default_workspace`.

## Durable migration

- `RepoId` is named `workspace` in Rust and accepts either JSON field name. It
  continues writing `provider` during mixed-version server/worker rollouts so
  old workers can still read newly queued jobs; a later protocol bump can flip
  the wire name safely.
- Existing `{provider}/{escaped_repo}` storage keys are already valid
  `{workspace}/{escaped_repo}` keys and remain unchanged.
- The server's route remains `/v1/repos/{workspace}/{repo_path}`. Older clients
  already send their provider instance in that position, so they continue to
  address the same data.
- Ref responses include canonical `workspace` plus the legacy `provider` copy
  during the protocol transition. New clients prefer `workspace` and accept old
  responses.
- A canonical `[workspace]` declaration wins over a legacy `[providers.<id>]`
  declaration with the same ID. An explicitly selected but missing workspace is
  a startup/configuration error rather than silently routing to GitHub.

Hosted authentication can select the workspace from the authenticated
principal instead of a CLI flag. The identity and registry model is the same;
authorization is responsible for ensuring a principal cannot select another
workspace merely by changing the route segment.
