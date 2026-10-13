# Support

## Platforms

Prebuilt release archives support:

- Linux x86-64 and arm64;
- macOS x86-64 and arm64.

Windows is not supported until it has a release asset and an end-to-end test.
PyPI wheels currently cover Linux x86-64 and both macOS architectures. Linux
arm64 users should use the release archive or build with Cargo.

System Git is required for servers, workers, and editable clones. Files-only
client clones do not require Git. Linux release binaries are otherwise static.

## Rust version

The minimum supported Rust version for source builds is 1.96. Release binaries
are built with the repository's pinned toolchain. The release checklist checks
the minimum version before a release.

## Client and server compatibility

Use client and server releases with the same wire version. An explicit mismatch
fails clearly; Ripclone does not keep a second legacy protocol implementation.
Before 1.0, unreleased database and artifact metadata formats have no
compatibility promise.

## Contributions

Fixes for the supported platforms, databases, storage backends, providers, and
documented behavior are welcome. New databases, providers, artifact formats, or
product features should begin with a design issue so the project stays small.
