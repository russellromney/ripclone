# Troubleshooting

Common failures and what they mean.

## `error while loading shared libraries: libgit2` (or `libssl`, `libzstd`)

The prebuilt binaries link their C libraries dynamically, so the runtime packages have to be present on the machine:

- **Debian/Ubuntu:** `apt-get install libgit2-1.5 libssl3` (package names vary by release; `libgit2` and `libssl3` are the ones to look for).
- **Alpine:** `apk add libgit2 openssl`.
- **macOS:** `brew install libgit2 openssl@3`.

On macOS the error reads `dyld: Library not loaded: .../libgit2.dylib`. If you can't install the runtime packages, `cargo install ripclone --locked` builds the C libraries from source instead of linking them dynamically.

## Clone prints "warming" / hangs, or the server returns `202`

A `202 Accepted` means the artifacts for that commit are still being built. On every push the server builds a depth-1 clonepack first (ready fast) and the full history + archive in the background; while a phase is still building, the ref response carries `build_status` and the server returns `202`. The client retries on its own — this is expected on the **first** clone of a commit that was just pushed, or the first time you clone a repo the server has never synced.

- A depth-1 or `files` clone is ready as soon as phase 1 finishes.
- A full editable clone (`--depth 0`) waits for the history build.
- If it never clears, the build is stuck or failing — check the server logs and `GET /readyz`. For webhook-less deploys, set `RIPCLONE_POLL_INTERVAL_SECS` so a missed or stuck build self-heals.

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

A mismatch (old server, new CLI, or vice versa) is the usual cause of missing modes or unexpected `202`/`404` responses — upgrade the lagging side.

Also confirm the CLI is talking to the server you think it is. Resolution order is: `--server` > `RIPCLONE_SERVER` env var > saved login config (`~/.config/ripclone/`) > the managed cloud default. A stale `RIPCLONE_SERVER` in your environment or an old saved login will silently override the server you meant to use. `ripclone logout` clears the saved login.
</content>
