# Freshness

How a push becomes clonable, and what a clone does when the server has not
finished building the commit it asked for.

## From push to clonable

1. The server learns about the push: a webhook, a CI call to
   `POST .../builds`, or its poll (every 5 minutes by default). See
   [Keeping repos fresh](../README.md#keeping-repos-fresh).
2. It builds the commit's three results. **Head** (the depth-1 pack and index)
   publishes first. **Files** (the working-tree archive) and then **Full**
   (all history) build next, in the background, and each publishes when it is
   done. Each has its own build slots, so a long Full does not delay the next
   push's Files.
   Typical times are in [Benchmarks](BENCHMARKS.md#build-time).
3. A published result never changes.

A clone of a branch asks the server once which commit the branch points at
and installs that commit, even if the branch moves during the clone.

## What each clone waits for

| Clone | Needs | If it is still building |
|---|---|---|
| `--depth 1` | Head | waits for it |
| `--mode files` | Files | waits for it |
| full (default) | Full | tops up from an earlier Full, or waits if there is none |
| full with `--wait` | Full | waits for it |

## Top-up

While Full(B) is pending, the server offers the newest ready Full on the
branch (or the parent commit's Full) as a base A. A full clone then:

1. installs Full(A) exactly as it would install Full(B);
2. runs `git fetch <upstream> <B>` with your own git, so your git config and
   credential helpers apply. The upstream URL comes from your own provider
   config (the provider's host, or `RIPCLONE_ORIGIN_BASE` for the default
   GitHub provider), never from the server;
3. checks out B.

The clone ends up with B's tree and B's full history. The fetch carries only
what your git sends: no ripclone token and no server provider credential. If
B does not descend from A (a force-push), the fetch just transfers more. If
the fetch fails, the clone fails with git's error and installs nothing; retry
with `--wait` to clone the exact build instead.

Depth-1 and files clones never top up.

## Waiting

A clone polls the server for the result it needs. Polls start 100 ms apart
and double up to 2 s (`RIPCLONE_POLL_MS` sets the cap). After 40 polls,
about 70 s at the default cap, it stops with an error that names the commit
and says the result is still pending; run the same clone with `--at <commit>`
to keep waiting for that commit.

A result that failed for good fails the clone at once. The error names the
commit and the command that builds it again: `ripclone sync <repo> --at
<commit>`.
