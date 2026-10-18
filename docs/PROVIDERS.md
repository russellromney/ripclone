# Providers

By default ripclone knows one host, the built-in `github` instance
(`github.com`). To build from GitLab, Gitea/Forgejo/Codeberg, or a self-hosted
host, register provider instances in `config.toml`
(`~/.config/ripclone/config.toml`, or the path in `RIPCLONE_CONFIG`). The
table name is the instance id:

```toml
[providers.gitlab]
kind = "gitlab"

[providers.company-gitea]
kind = "gitea"
host = "git.example.com"
token = "gitea-token"
```

Each instance has:

| Key | Meaning |
|---|---|
| `kind` | `github`, `gitlab`, `gitea` (also `forgejo`, `codeberg`), or `generic`. |
| `host` | Hostname or base URL. Default `github.com` for `github` and `gitlab.com` for `gitlab`; required for the others. |
| `token` | Read token for private repos. |
| `auth_template` | How the token is sent, with one `{token}` placeholder, for example `"token {token}"`. Required for `generic`. |
| `auth_header_name` | Header that carries the token. Default `Authorization`. |

An entry with id `github` replaces the built-in instance's settings.

Address a repo by instance id: `gitlab:mygroup/project` on the CLI,
`/v1/repos/gitlab/mygroup/project/...` on the API. The CLI also takes a
pasted clone URL (`https://gitlab.com/mygroup/project.git` or
`git@gitlab.com:mygroup/project.git`) and picks the instance whose host
matches.

## Server and CLI

The server needs the instance in its own config file to build and serve its
repos. For private repos it needs read access to the upstream: a `token` on
the instance, or a GitHub App for GitHub (see
[Configuration](CONFIG.md#github-app-credentials)). Keep that file readable
only by the server's user; `ripclone provider add` writes it with mode 0600.

The CLI needs the instance too, to clone. A full clone that tops up fetches
from the instance's host with your own git, and `--verify-upstream` reads the
same host. The CLI never sends the instance's token to the ripclone server.
`ripclone provider add <id> --kind <kind> --host <host>` writes an instance to
the CLI's `config.toml`; run it again with only the flags to change. `ripclone
provider list` shows every instance the CLI knows, the built-in `github`
included, and `ripclone provider rm <id>` removes one. `ripclone provider test
<id> owner/repo` reads the repo's `HEAD` with `git ls-remote` and the
instance's token, so it checks your config, not the server's.
