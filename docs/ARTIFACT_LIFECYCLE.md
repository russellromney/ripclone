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

Artifacts are first written to the local CAS. Before upload, the server verifies
the local CAS object by hash and then streams the verified file to durable
storage. Local filesystem storage uses the same hash-checked file install path;
S3-compatible storage uses sized streaming uploads.

For remote storage, the local CAS is a build cache. After successful upload and
retention protection, large local artifacts may be evicted. Future syncs can
rebuild archive bundles from durable per-frame chunks in storage.

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
- A corrupt local CAS object must not be silently replaced after its bytes have
  entered an output artifact.
- Metadata and manifests are small enough to use buffered APIs; large packs,
  archive bundles, and frame objects should use file or streaming APIs.
