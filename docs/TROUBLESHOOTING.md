# Troubleshooting

Common failures and what they mean.

## System Git is missing

The server, worker, and editable clone paths use the system `git` executable.
They check it before starting work and report:

```text
system Git is required; install `git` and ensure it is on PATH
```

Install Git with your operating system package manager. For example,
`apk add git` on Alpine or `apt-get install git` on Ubuntu. Files-only client
clones do not require Git.

## `error while loading shared libraries` on Linux

The Linux release binaries are static musl builds. If a release asset reports a
missing shared library, verify its checksum and reinstall it. A source build is
not the same artifact and may use libraries from the build machine.

## Clone prints "warming" / hangs, or the server returns `202`

There are two different `202` cases. A `202` from `sync` or `add` means the
exact branch commit was admitted or coalesced into background work; those
commands return without waiting for the build. Blocking library methods
poll the pinned commit's metadata after that first `202`, so they do not repeat
a moving-tip POST. A `202` from a clone/ref request means the artifacts for the
requested commit are still being built. On every push the server publishes
Head first, then builds missing Full history and Files archive work
concurrently. The requested stored result is the readiness signal; the job
reports pending or failed work.

- A depth-1 clone waits for Head.
- A full editable clone (`--depth 0`) waits for Full.
- A files clone waits for Files, independently of Full.
- If it never clears, the build is stuck or failing — check the server logs and `GET /readyz`. The 5-minute polling fallback (`RIPCLONE_POLL_INTERVAL_SECS`, on by default) re-checks known repos so a missed or stuck build self-heals.

## `401 Unauthorized` vs `403 Forbidden`

These mean different things — don't treat them the same:

- **`401`** — the **server token** is missing or wrong. The CLI and server both read it from `RIPCLONE_SERVER_TOKEN`; a mismatch, an empty value, or the wrong `Authorization` header returns `401`. (Webhook deliveries with a bad HMAC signature also return `401`.) Fix the token you send, not the repo access.
- **`403`** — the token is valid, but the caller may **not read this repo**. The repo is private and the credential you passed (`--token` / `X-Upstream-Token`, or the provider token configured on the server) doesn't grant read access to it. Fix the upstream credential or the repo's permissions.

Rule of thumb: `401` = "who are you?", `403` = "I know who you are, and no."

## Version / config drift

If clones behave oddly after an upgrade, check that the CLI and the server agree:

```sh
ripclone version    # prints CLI + server versions with a compatibility verdict
```

A client/server protocol mismatch is the usual cause of missing modes or unexpected `202`/`404` responses — deploy matching versions.

Also confirm the CLI is talking to the server you think it is. Resolution order is: `--server` > `RIPCLONE_SERVER` env var > saved login config (`~/.config/ripclone/`) > the built-in default server. A stale `RIPCLONE_SERVER` in your environment or an old saved login will silently override the server you meant to use. `ripclone logout` clears the saved login.

## Logging

Set `RUST_LOG` on the server or worker to choose the tracing level:

```sh
RUST_LOG=info ripclone-server
RUST_LOG=debug ripclone-worker
```

`info` is the normal operator setting. Use `debug` for a bounded diagnostic run;
it is much noisier.
