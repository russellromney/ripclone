# Artifact Lifecycle

Ripclone artifacts are content-addressed. A hash is both the storage key and the
integrity contract for the bytes served to clients.

## Build

The server mirrors the upstream repository and builds:

- a metadata chunk with the skeleton pack, index, archive frame table, and file
  table
- clonepack manifests for depth-1, full/editable, and files mode
- pack artifacts for editable clones
- zstd archive frames and coarser download bundles for files mode

Archive frames remain individually addressable for incremental reuse. Download
bundles group consecutive frames so files-mode clones make fewer object-storage
requests on fast links.

## Publish

Artifacts are first written to the local CAS. Each artifact is hashed once,
as it is written or, for a file git wrote, by one read, and then renamed into
the CAS from scratch space on the same filesystem. The hash names the object;
the server does not hash it again before upload.

Each artifact's upload starts as soon as it is in the CAS, while the build
goes on. At most 32 uploads run at once across all builds. A build publishes
only after every one of its uploads has succeeded; if one fails, the build
fails and publishes nothing. With local filesystem storage the CAS is the
store, so there is nothing to upload. S3-compatible storage uses sized
streaming uploads. Keys are content hashes, so an object of 1 MiB or more is
first checked with a HEAD request and not sent again when the bucket already
holds it at the same length.

With local storage the CAS syncs each object to disk before it is renamed into
place, and its directory after. With S3 storage the local CAS is a cache in
front of the bucket and skips those syncs. The git commands that write build
packs skip their own syncs in both modes, since the CAS syncs what it
installs when it must.

For remote storage, the local CAS is a build cache. After successful upload and
durable-copy verification, large local artifacts may be removed immediately or
trimmed later by age or size. The local Git mirror is also disposable. A later
build may fetch Git again or read a published parent artifact from S3.

Published exact Head, Full, and Files results do not expire. Local-storage
deployments keep their published artifact bytes on local disk. S3-compatible
deployments keep the published bytes in S3; local cache cleanup never changes
exact-result metadata and never deletes remote objects.

## Serve

Clients prefer signed object-storage URLs when present. Without signed URLs, the
server proxies artifacts by hash. Every fetched artifact is verified against its
content hash before use.

Files mode downloads archive bundles, slices each zstd frame by manifest offset,
inflates frames independently, and materializes files from frame slices or
fragments. Editable mode installs prebuilt packs and materializes the worktree
from the HEAD-closure pack while history-only packs remain on disk for Git.

## Failure Rules

- A hash mismatch is corruption and must fail closed.
- A missing local CAS object may fall back to durable storage.
- Local cache cleanup removes an object only after confirming its remote copy.
- A corrupt local CAS object must not be silently replaced after its bytes have
  entered an output artifact.
- Metadata, manifests, and bounded-size archive bundles use buffered APIs.
  Large history packs use the file-backed streaming/resume path. Current-tree
  HEAD packs remain buffered for direct extraction; because one oversized Git
  object can make a HEAD pack arbitrarily large, that is a known client-memory
  limitation rather than part of the large-history-pack guarantee.
