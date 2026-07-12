# Server-issued pinned top-up bundles

The client does not top up from GitHub/GitLab and does not trust a cached
repository's remote. The authenticated ripclone server resolves target `T`
against its mirror and issues a content-addressed manifest containing:

- exact base OID `B`, target OID `T`, mode, branch, and canonical provider URL;
- non-thin pack/idx artifacts for objects reachable from `T` but not `B`;
- exact-target checkout metadata/index and worktree content artifacts;
- artifact hashes and lengths covered by the authenticated manifest.

For an unrelated force-push, the set difference naturally becomes the target's
full closure. No client-visible provider credential, temporary ref, capability
URL, or pin lifetime is involved.

The caller supplies only the desired manifest CAS hash, not mutable clone
semantics. `PinnedBundleInstaller` is the trusted client/CAS boundary. It must
authenticate the raw manifest, verify every artifact hash and length, and return
`VerifiedPinnedBundle`: format, base, target, mode, branch, canonical origin,
and exact ordered artifact descriptors. A stable length-delimited SHA-256 digest
binds all returned semantics and descriptors and is recomputed before use, so a
receipt for an artifact set containing multiple commits cannot be retargeted.
The workspace provider adapter also supplies the exact approved
canonical origin independently of the bundle; the bundle cannot authorize its
own host or path. The top-up transaction then discards all installed control state except
physical objects and the index, writes fresh allowlisted refs/config, clears
sparse state by rebuilding the index from `T`, verifies base/target/connectivity
and depth semantics, removes ignored/untracked residue, and atomically publishes.

The server generator and clone-plan response are required integration work; the
client must fail closed until a verified bundle is available. There is no direct
SHA/provider fallback for private repositories.

## Existing runtime-Git blocker

This primitive currently uses the same `git checkout-index`/validation runtime
dependency as ripclone's existing editable clone path. That conflicts with the
README claim that prebuilt binaries require no Git runtime. Resolving that
existing product/docs inconsistency—by moving checkout/validation fully into
gix/worktree-writer or explicitly packaging/requiring Git—is a separate release
gate. The pinned-bundle design introduces no new provider-side Git dependency,
but it must not be advertised as PATH-free while the shared editable path is not.
