# ripclone

`ripclone` is a Rust implementation of a content-addressed Git clone accelerator.
It prebuilds clone artifacts on the server, stores them in CAS/object storage, and
lets clients download the pieces needed for files-only, depth-1, or editable
clones.

The main documentation lives in the repository:

- Overview and usage: <https://github.com/russellromney/ripclone#readme>
- Architecture: <https://github.com/russellromney/ripclone/blob/main/docs/DESIGN.md>
- Artifact lifecycle: <https://github.com/russellromney/ripclone/blob/main/docs/ARTIFACT_LIFECYCLE.md>
- Benchmarks: <https://github.com/russellromney/ripclone/blob/main/docs/BENCHMARKS.md>

The crate exposes both binaries and internal library modules. Public API
stability is intentionally conservative while the project is pre-1.0.
