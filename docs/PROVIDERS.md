# Providers

By default ripclone knows one host: the built-in `github` instance. To mirror from GitLab, Gitea/Forgejo/Codeberg, or a self-hosted host, register provider instances on the server with the `RIPCLONE_PROVIDERS` environment variable or `config.toml`:

```bash
export RIPCLONE_PROVIDERS='{"providers":[
  {"id":"gitlab","kind":"gitlab","host":"gitlab.com"},
  {"id":"company-gitea","kind":"gitea","host":"git.example.com","token":"gitea-token"}
]}'
```

Supported `kind` values: `github`, `gitlab`, `gitea`, `generic`. A `generic` host needs an `auth_template` (e.g. `"token {token}"`) so ripclone knows how to build the auth header. Then address a repo by instance id — `gitlab:mygroup/project` on the CLI, or `/v1/repos/gitlab/mygroup/project/...` on the API.

For private repos the server needs read access to the upstream — configure a `token` on the provider, or pass one per request in the `X-Upstream-Token` header (the CLI's `--token` does this for you).
</content>
