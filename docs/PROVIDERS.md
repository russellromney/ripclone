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
```

The file holds no secrets, so it can be committed or templated. A provider
entry with a `token` key is an error.

Each instance has:

| Key | Meaning |
|---|---|
| `kind` | `github`, `gitlab`, `gitea` (also `forgejo`, `codeberg`), or `generic`. |
| `host` | Hostname or base URL. Default `github.com` for `github` and `gitlab.com` for `gitlab`; required for the others. |
| `app_id` | GitHub App id, for a `github` instance the server reads as an app. See [Credentials](#credentials). |
| `auth_template` | How the token is sent, with one `{token}` placeholder, for example `"token {token}"`. Required for `generic`. |
| `auth_header_name` | Header that carries the token. Default `Authorization`. |

An entry with id `github` replaces the built-in instance's settings.

Address a repo by instance id: `gitlab:mygroup/project` on the CLI,
`/v1/repos/gitlab/mygroup/project/...` on the API. The CLI also takes a
pasted clone URL (`https://gitlab.com/mygroup/project.git` or
`git@gitlab.com:mygroup/project.git`) and picks the instance whose host
matches.

## Credentials

The server reads private repos with a credential from its environment, named
by the instance id: the id upper-cased, with every byte other than a letter
or digit as `_` (`company-gitea` is `COMPANY_GITEA`). Set at most one per
instance:

| Variable | Meaning |
|---|---|
| `RIPCLONE_TOKEN_<ID>` | A read token, sent on every read from the instance. |
| `RIPCLONE_APP_KEY_<ID>` | The PEM private key of the GitHub App in the instance's `app_id`. One line with literal `\n` escapes also works. |

With neither, the server reads anonymously. An `app_id` without its key, a
key without an `app_id`, or a token beside an app stops the server at
startup.

A GitHub App gives short-lived tokens instead of a long-lived one. The server
looks up the app's installation on each repo, so one app serves every account
that installed it; a repo the app is not installed on is read anonymously. A
lookup is reused for ten minutes, so a new installation takes effect within
that time. The app reads the instance's API: `api.github.com` for
`github.com`, or `https://<host>/api/v3` for GitHub Enterprise, which must be
https. Give the app read access to repository contents and metadata.

```toml
[providers.github]
kind = "github"
app_id = 123456
```

## Server and CLI

The server needs the instance in its own config file to build and serve its
repos, and the instance's credential in its environment for private ones.

The CLI needs the instance too, to clone. It holds no upstream credential: a
full clone that tops up, `--verify-upstream`, and `provider test` all read
the instance's host with your own git, so its config and credential helpers
apply. The CLI never sends an upstream credential to the ripclone server.
`ripclone provider add <id> --kind <kind> --host <host>` writes an instance to
the CLI's `config.toml`; run it again with only the flags to change. `ripclone
provider list` shows every instance the CLI knows, the built-in `github`
included, and `ripclone provider rm <id>` removes one. `ripclone provider test
<id> owner/repo` reads the repo's `HEAD` with `git ls-remote`, so it checks
your config and git credentials, not the server's.
