# Pinned base top-up

An active repository can satisfy a HEAD or full clone from an older cached base
while the exact artifact for the latest commit is still building. The client
installs the base privately, fetches one already-resolved commit, validates it,
and publishes the completed worktree atomically.

The commit is resolved **before** this operation. A branch name is never used as
the fetch target, so a push during the clone cannot silently move the result.
If the pinned object becomes unavailable after a force-push, the operation
fails explicitly; its caller may re-resolve and start a new clone, but must not
substitute the branch's new tip inside the existing clone.

## Modes

- `Head` accepts an older HEAD base, performs an exact `--depth=1` fetch, and
  verifies that history from the resulting `HEAD` contains exactly one commit.
- `Full` requires a non-shallow full base, fetches the missing closure for the
  exact target, and verifies connectivity before checkout.

In both modes the final branch, remote-tracking ref, worktree, and `HEAD` point
to the requested object ID. The destination remains absent on installer,
network, validation, checkout, or publish failure.

## Private upstream authentication

The top-up request contains a configured Git remote name, never a URL, token,
or authorization header. Public repositories may configure that remote directly
to the provider. Private GitHub App repositories must point it at a ripclone Git
proxy (or use a trusted out-of-band Git credential helper). Installation tokens
remain server-side and must never be serialized in clone plans or embedded in
client remote URLs.

The reusable transaction is exposed as
`ripclone::topup::install_pinned_from_base`. Clone-plan/server integration is
intentionally separate: the server chooses the cached base and authenticated
transport; this primitive only enforces exact-target installation semantics.
