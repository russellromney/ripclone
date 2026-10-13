# Providers

By default ripclone knows one host, the built-in `github` instance. To build
from GitLab, Gitea/Forgejo/Codeberg, or a self-hosted host, register provider
instances on the server with `RIPCLONE_PROVIDERS` or `config.toml`:

```bash
export RIPCLONE_PROVIDERS='{"providers":[
  {"id":"gitlab","kind":"gitlab","host":"gitlab.com"},
  {"id":"company-gitea","kind":"gitea","host":"git.example.com","token":"gitea-token"}
]}'
```

Supported `kind` values: `github`, `gitlab`, `gitea`, `generic`. A `generic`
host needs an `auth_template` (for example `"token {token}"`) so ripclone can
build the auth header. Address a repo by instance id: `gitlab:mygroup/project`
on the CLI, `/v1/repos/gitlab/mygroup/project/...` on the API.

For private repos the server needs read access to the upstream: set a `token`
on the provider instance, or use a GitHub App for GitHub (see
[Configuration](CONFIG.md#github-app-credentials)).
