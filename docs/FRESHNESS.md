# Freshness

How a push becomes clonable, and what a clone does when the server has not
finished building the commit it asked for.

## From push to clonable

1. The server learns about the push: a webhook, a CI call to
   `POST .../builds`, or its poll (every 5 minutes by default). See
   [Keeping repos fresh](../README.md#keeping-repos-fresh).
2. It builds the commit's three results. **Head** (the depth-1 pack and index)
   and **Files** (the working-tree archive) build at the same time. **Full**
   (all history) reuses Head's packs, so it starts when Head publishes. Each
   result publishes when it is done and has its own build slots, so a Full
   does not wait for its commit's Files, and a long Full does not delay the
   next push's Head or Files.
   Typical times are in [Benchmarks](BENCHMARKS.md#build-time).
3. A published result never changes.

A clone of a branch asks the server once which commit the branch points at
and installs that commit, even if the branch moves during the clone.

## What each clone waits for

| Clone | Needs | If it is still building |
|---|---|---|
| `--depth 1` | Head | waits for it |
| `--mode files` | Files | writes the same commit's tree from its Head, or waits if Head is pending too |
| `--mode files` with `--wait` | Files | waits for it |
| full (default) | Full | tops up from an earlier Full, or waits if there is none |
| full with `--wait` | Full | waits for it |

A depth-1 clone always waits: Head is the first result built for a commit,
so nothing else for that commit can be ready before it.

## Files from Head

While Files(B) is pending, a files clone checks whether Head(B) is ready. If
it is, the clone downloads Head(B)'s packs and writes B's working tree from
them, the same files with the same modes that Files(B) would give, and no
`.git`. Head's packs compress each blob on its own, so this can download
more bytes than Files. It never uses another commit.

## Top-up

While Full(B) is pending, the server offers the newest ready Full on the
branch (or the parent commit's Full) as a base A. A full clone then:

1. installs Full(A) exactly as it would install Full(B);
2. runs `git fetch <upstream> <B>` with your own git, so your git config and
   credential helpers apply. The upstream URL comes from your own provider
   config (the provider's host), never from the server;
3. checks out B.

The clone ends up with B's tree and B's full history. The fetch carries only
what your git sends: no ripclone token and no server provider credential. If
B does not descend from A (a force-push), the fetch just transfers more. The
clone has the branches and tags `git clone` would have, as of the server's
last publish (below), with B as `origin/<branch>`.
If the fetch fails, the clone fails with git's error and installs nothing; retry
with `--wait` to clone the exact build instead.

Depth-1 and files clones never top up.

## Branches and tags

A full clone gets what `git clone` gets: every upstream branch as
`refs/remotes/origin/<branch>` and every tag, with all the history they
reach. A depth-1 clone gets what `git clone --depth 1` gets: its own branch
and the tags whose target it holds.

The server keeps the repository's refs and publishes them:

- tags, when it fetches a commit, so tags pushed with a commit are there by
  the time the commit's Head is ready;
- branches and tags, after any branch or tag push or delete webhook;
- branches and tags, after a poll that sees the upstream's refs changed
  (every 5 minutes by default).

A branch or tag added, moved or deleted upstream since the last publish is
missing or stale in a clone; your checked-out branch is always the commit
you cloned. A later `git fetch` brings branches up to date, but, like git
with any clone, never moves or deletes a tag you have.

A full clone writes the server's refs when it holds the default branch's
tip. It does when it clones that tip or tops up past it, and when it clones
another commit (`--branch`, `--at`): as `git clone --branch` does, it
downloads the history every ref reaches, the tip's Full and the side pack,
then checks out its own commit's tree. A commit on no ref adds the history
its Full does not share with the tip's. While the newest tip's Full builds
after a push, such a clone gets the refs as they were at the last tip with
a ready Full, as if cloned just before the push; your next `git fetch`
brings the push. A commit newer than the published tip downloads its own
history only. Otherwise, as before any tip's Full is ready, it fetches the
branches and tags from the upstream with your own git, as top-up does. If
that fetch fails, the clone warns and keeps only its own branch and the
tags whose targets it holds.

## Waiting

A clone polls the server for the result it needs. Polls start 100 ms apart
and double up to 2 s. After 40 polls,
about 70 s at the default cap, it stops with an error that names the commit
and says the result is still pending; run the same clone with `--at <commit>`
to keep waiting for that commit.

A result that failed for good fails the clone at once. The error names the
commit and the command that builds it again: `ripclone sync <repo> --at
<commit>`.
