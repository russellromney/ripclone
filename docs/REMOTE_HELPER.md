# Git remote helper: `git clone ripclone://…`

`git-remote-ripclone` lets plain `git` clone and fetch through a ripclone server,
without the `ripclone` CLI. Git sees a normal remote; the helper resolves the
ref, seeds `.git` from the prebuilt clonepack, and hands the objects back to git
over a local `git upload-pack`. Install it on `PATH` (the installer and release
tarball include it) and git picks it up automatically for `ripclone://` URLs.

Use the `ripclone` CLI for the full feature set (files mode, `--temp`, worktrees,
`--verify-upstream`, progress). The helper is for the cases where you want a
stock `git clone`/`git fetch` to go fast with no wrapper — CI steps, tools that
shell out to `git`, `go get`-style fetchers.

## URL syntax

```
ripclone://<provider>/<repo-path>[.git][#branch]
```

- `<provider>` is a registered provider instance id (`github`, `gitlab`, a
  self-hosted Gitea id, …). It is the URL host.
- `<repo-path>` is the provider-qualified path (`oven-sh/bun`,
  `mygroup/sub/project`). A trailing `.git` is optional.
- `#branch` optionally pins a branch; omit it to take the repo's default branch.

```sh
git clone ripclone://github/oven-sh/bun.git
git clone ripclone://gitlab/mygroup/project#dev
```

## Server resolution

The helper needs to know which ripclone server to talk to. It resolves the URL
in this order:

1. `RIPCLONE_SERVER` environment variable.
2. `git config remote.<name>.ripcloneServer` (per-remote, local config).

Set the per-remote config once and stock `git` commands just work:

```sh
git clone ripclone://github/oven-sh/bun.git bun
cd bun
git config remote.origin.ripcloneServer https://ripclone.example.com
git fetch            # uses the configured server
```

If neither is set the helper errors instead of guessing.

## Authentication

The server token is read from the environment (never from a URL):

1. `RIPCLONE_SERVER_TOKEN_HASH` — an already-hashed token.
2. `RIPCLONE_SERVER_TOKEN` — the raw token; the helper hashes it (SHA-256).

This is the same server token the CLI and server use. Upstream (per-repo)
credentials are not part of the helper flow — the server serves from artifacts it
already built.

## Depth limits

The helper maps `--depth` onto the clonepack kinds ripclone actually builds:

- **`--depth 1`** → the shallow clonepack. Git gets a proper shallow clone with a
  `.git/shallow` marker.
- **no `--depth`** (or a full clone) → the full-history clonepack.
- **`--depth N` for `N > 1`** → **rejected** with an actionable error. Arbitrary
  shallow depth is not implemented, and the helper refuses it rather than
  silently serving full history that git would record as a complete, non-shallow
  clone. Re-run with `--depth 1` or without `--depth`.

## Push

Push does **not** go through ripclone. `git push` over a `ripclone://` remote is
rejected on purpose — ripclone serves read artifacts, it is not a git host.

Send pushes to the real upstream (GitHub/GitLab/Gitea) with `pushInsteadOf`:

```sh
# fetch via ripclone, push straight to the upstream host
git clone ripclone://github/oven-sh/bun.git bun
cd bun
git config remote.origin.ripcloneServer https://ripclone.example.com
git config url."git@github.com:".pushInsteadOf ripclone://github/
git push            # goes to git@github.com:oven-sh/bun.git
```

`fetch` stays fast through ripclone; `push` bypasses it entirely.

## Tested by

`rust/tests/e2e_remote_helper.rs` clones a repo end-to-end through the helper
against a live server (real `git`, real binaries) and runs on every PR via
`scripts/ci.sh test`. `scripts/e2e_remote_helper.sh` is the manual, network-backed
smoke test against a public repo.
